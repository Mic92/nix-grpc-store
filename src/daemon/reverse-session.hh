#pragma once
// Opt-in worker-initiated transport. Only gateway-reserved stream IDs may
// deliver responses; the worker's build dispatcher validates delegated calls.

#include <chrono>
#include <condition_variable>
#include <iterator>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/sync_stream.h>

#include "nix_remote.grpc.pb.h"
#include "auth.hh"
#include "reverse-capability.hh"
#include "reverse-registry.hh"
#include "worker-identity.hh"

namespace nixgrpc {

class ReverseSessionService final : public nix::remote::ReverseSession::Service
{
    // Replacement and leadership loss can race with handler teardown. A
    // callback must never dereference a ServerContext after Register returns.
    struct Cancellation
    {
        std::mutex mutex;
        grpc::ServerContext * context;

        explicit Cancellation(grpc::ServerContext * context) : context(context) { }

        void cancel()
        {
            const std::scoped_lock lock(mutex);
            if (context) {
                context->TryCancel();
            }
        }

        void retire()
        {
            const std::scoped_lock lock(mutex);
            context = nullptr;
        }
    };

    WorkerIdentity & identity;
    ReverseRegistry & registry;
    Auth & workerAuth;

public:
    ReverseSessionService(WorkerIdentity & identity, ReverseRegistry & registry, Auth & workerAuth)
        : identity(identity)
        , registry(registry)
        , workerAuth(workerAuth)
    {
    }

    auto Register(
        grpc::ServerContext * context,
        grpc::ServerReaderWriter<nix::remote::ReverseFarmMsg, nix::remote::ReverseWorkerMsg> * stream)
        -> grpc::Status override
    {
        // Explicitly require TLS even if the rest of this server allows
        // plaintext for local direct workers. Never trust XFCC for workers.
        auto const auth = context->auth_context();
        if (!auth) {
            return {grpc::StatusCode::UNAUTHENTICATED, "reverse registration requires TLS"};
        }
        auto const transports = auth->FindPropertyValues(GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME);
        if (transports.size() != 1 ||
            (std::string_view(transports.front().data(), transports.front().size()) != "ssl" &&
             std::string_view(transports.front().data(), transports.front().size()) != "tls")) {
            return {grpc::StatusCode::UNAUTHENTICATED, "reverse registration requires TLS"};
        }
        // The projected pod token proves which pod owns the route; it does
        // not grant permission to be a worker. Never use identify() here:
        // forwarded identity and ordinary client bearers cannot authorize a
        // data session in place of the peer's own TLS certificate.
        auto const caller = workerAuth.identifyDirect(*context);
        if (caller.kind != Caller::Kind::named) {
            return {grpc::StatusCode::UNAUTHENTICATED, "reverse registration requires a TLS client certificate"};
        }
        if (auto const status = Auth::authorize(caller, "ReverseSession", Role::trusted); !status.ok()) {
            return status;
        }
        // Avoid treating an arbitrary first bearer in duplicated metadata as
        // authoritative. The tunnel verifier is separate from client Auth.
        auto const & metadata = context->client_metadata();
        auto [first, last] = metadata.equal_range("authorization");
        if (first == last || std::next(first) != last) {
            return {grpc::StatusCode::UNAUTHENTICATED, "one tunnel credential required"};
        }
        constexpr std::string_view prefix = "Bearer ";
        std::string_view value(first->second.data(), first->second.size());
        if (!value.starts_with(prefix) || value.size() <= prefix.size()) {
            return {grpc::StatusCode::UNAUTHENTICATED, "tunnel bearer required"};
        }
        // Reject a missing bearer before waiting for a first message. The
        // identity verifier needs the claimed ID from that first message.
        nix::remote::ReverseWorkerMsg msg;
        if (!stream->Read(&msg)) {
            return {grpc::StatusCode::INVALID_ARGUMENT, "missing worker ID"};
        }
        if (msg.stream_id() != 0 || msg.frame_case() != nix::remote::ReverseWorkerMsg::FRAME_NOT_SET) {
            return {grpc::StatusCode::INVALID_ARGUMENT, "first message must be registration only"};
        }
        auto bound = identity.bind(std::string(value.substr(prefix.size())), msg.worker_id());
        if (!bound) {
            return {grpc::StatusCode::UNAUTHENTICATED, "invalid tunnel credential or worker ID"};
        }
        // Control-session binding and worker-side dispatch remain separate.
        // A registered session alone is NOT a schedulable worker.
        auto sessionKey = ReverseCapability::newKey();
        if (!sessionKey) {
            return {grpc::StatusCode::INTERNAL, "reverse session key unavailable"};
        }
        auto cancellation = std::make_shared<Cancellation>(context);
        auto transport = std::make_shared<ReverseTransport>();
        auto handle = registry.registerWorker(
            bound->workerId, msg.worker_id(), [cancellation] { cancellation->cancel(); },
            std::move(*sessionKey), transport);
        if (!handle) {
            return {grpc::StatusCode::UNAVAILABLE, "scheduler not accepting reverse sessions"};
        }
        struct Cleanup
        {
            ReverseRegistry & registry;
            ReverseRegistry::Handle handle;
            std::shared_ptr<Cancellation> cancellation;
            ~Cleanup()
            {
                cancellation->retire();
                registry.disconnected(handle, false);
            }
        } cleanup{registry, *handle, cancellation};


        // A stream must never outlive its verified projected token. stop_token
        // wakes the timer immediately on normal close/replacement.
        std::mutex mutex;
        std::condition_variable_any cv;
        std::jthread expiry([&, cancellation](std::stop_token stop) {
            std::unique_lock lock(mutex);
            if (!cv.wait_until(lock, stop, bound->expiresAt, [] { return false; }) && !stop.stop_requested()) {
                cancellation->cancel();
            }
        });
        nix::remote::ReverseFarmMsg ack;
        ack.set_generation((*handle)->generation);
        ack.set_session_key((*handle)->sessionKey);
        if (!registry.current(*handle) || !stream->Write(ack)) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse session replaced"};
        }
        // gRPC permits one concurrent reader and one concurrent writer. Only
        // this thread calls Read, and only the dedicated writer calls Write
        // after the registration ack. Closing the session wakes the writer.
        std::jthread writer([&, cancellation](std::stop_token stop) {
            nix::remote::ReverseFarmMsg frame;
            while (transport->outgoing.waitTake(frame, stop)) {
                if (!registry.current(*handle) || !stream->Write(frame)) {
                    cancellation->cancel();
                    break;
                }
            }
        });
        while (stream->Read(&msg)) {
            if (!registry.current(*handle) || !transport->responses.accept(std::move(msg))) {
                transport->close();
                return {grpc::StatusCode::INVALID_ARGUMENT, "invalid reverse response or session displaced"};
            }
            msg.Clear();
        }
        transport->close();
        return grpc::Status::OK;
    }
};

} // namespace nixgrpc
