#pragma once
// One authenticated client RPC over its exact, generation-fenced data
// registration. Never dials a worker address or trusts client route metadata.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "reverse-gateway.hh"

namespace nixgrpc {

class ReverseRelay
{
public:
    // Each serialized protobuf has a four-byte little-endian length, then
    // bytes split across <=64 KiB transport frames. Build result chunks may
    // exceed a single frame without unbounded buffering at either end.
    static constexpr uint32_t maxMessageBytes = 16 * 1024 * 1024;
    static constexpr uint32_t maxRequestBytes = 1024 * 1024;
    // Build jobs can take hours; a bounded server-side cap still prevents a
    // missing client deadline from pinning a relay forever.
    static constexpr std::chrono::hours maxCallDuration{24};
    using Receive = std::function<bool(std::string_view)>;
    using Select = std::function<ReverseGateway::Result(uint64_t)>;

    explicit ReverseRelay(ReverseGateway & gateway, ReverseRegistry & registry)
        : gateway(gateway), registry(registry) { }

    template<typename Request>
    [[nodiscard]] auto call(grpc::ServerContext & context, const Select & select,
                             const Request & request, const Receive & receive,
                             const std::function<bool()> & stillAssigned = {},
                             const std::function<bool()> & resultReceived = {},
                             const std::function<void()> & beforeAck = {}) const -> grpc::Status
    {
        if (!select || !receive) return {grpc::StatusCode::INTERNAL, "reverse relay not configured"};
        // An ID is allocated per registration after resolving the worker.
        // A callback for preparing against that exact handle is supplied by
        // the handler; the gateway verifies it again when enqueuing.
        // The first selection only pins a registration; its capability is
        // discarded. The second selection binds the actual monotonic ID.
        auto selection = select(1);
        if (!selection.status.ok()) return selection.status;
        if (!selection.prepared) return {grpc::StatusCode::INTERNAL, "missing selected reverse session"};
        auto session = selection.prepared->session;
        if (!session || !session->transport) return {grpc::StatusCode::UNAVAILABLE, "reverse session unavailable"};
        if (stillAssigned && !stillAssigned()) return {grpc::StatusCode::FAILED_PRECONDITION, "build assignment changed"};
        auto const id = session->transport->nextStreamId.fetch_add(1);
        if (id == 0) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse stream IDs exhausted"};
        selection = select(id);
        if (!selection.status.ok()) return selection.status;
        if (!selection.prepared || selection.prepared->session != session)
            return {grpc::StatusCode::UNAVAILABLE, "reverse worker changed during selection"};
        if (request.ByteSizeLong() > maxRequestBytes)
            return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse request too large"};
        auto const bytes = request.SerializeAsString();
        if (bytes.size() > maxRequestBytes)
            return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse request too large"};
        if (context.IsCancelled()) return {grpc::StatusCode::CANCELLED, "client cancelled reverse call"};
        auto & transport = *session->transport;
        auto status = gateway.enqueue(std::move(*selection.prepared), id);
        if (!status.ok()) return status;
        // Scope all reservations through failures, cancellation, and return.
        struct Release {
            ReverseTransport & transport;
            uint64_t id;
            bool completed = false;
            ~Release() {
                // A worker may still reply after the caller disconnects.
                // Explicit cancelled-ID tombstones are bounded per session.
                if (!completed) (void) transport.outgoing.cancel(id);
                (void) transport.outgoing.finished(id);
                (void) transport.responses.cancel(id, !completed);
            }
        } release{transport, id};

        // A synchronous gRPC handler may be blocked downstream waiting for a
        // response. Always bound the wait even if the client set no deadline.

        std::string wire;
        wire.reserve(bytes.size() + 4);
        auto const size = static_cast<uint32_t>(bytes.size());
        for (unsigned i = 0; i != 4; ++i) wire.push_back(static_cast<char>((size >> (8 * i)) & 0xff));
        wire += bytes;
        for (size_t offset = 0; offset < wire.size(); offset += ReverseFrames::maxFrameBytes) {
            if (context.IsCancelled()) return {grpc::StatusCode::CANCELLED, "client cancelled reverse call"};
            if (!registry.current(session) || (stillAssigned && !stillAssigned()))
                return {grpc::StatusCode::UNAVAILABLE, "reverse worker or assignment changed"};
            if (!transport.outgoing.data(id, std::string_view(wire).substr(offset, ReverseFrames::maxFrameBytes)))
                return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse request queue full"};
        }
        if (!transport.outgoing.halfClose(id)) return {grpc::StatusCode::UNAVAILABLE, "reverse request closed"};

        auto const deadline = std::min(context.deadline(), std::chrono::system_clock::now() + maxCallDuration);
        std::stop_source wake;
        std::mutex waitMutex;
        std::condition_variable_any waitCv;
        std::jthread monitor([&](std::stop_token stop) {
            std::unique_lock lock(waitMutex);
            while (!stop.stop_requested()) {
                if (context.IsCancelled() || std::chrono::system_clock::now() >= deadline || !registry.current(session)
                     || (stillAssigned && !stillAssigned() && (!resultReceived || !resultReceived()))) {
                    wake.request_stop();
                    return;
                }
                waitCv.wait_for(lock, stop, std::chrono::milliseconds(25), [] { return false; });
            }
        });
        std::string pending;
        std::optional<uint32_t> expected;
        nix::remote::ReverseWorkerMsg frame;
        while (transport.responses.waitTake(id, frame, wake.get_token())) {
            if (context.IsCancelled()) return {grpc::StatusCode::CANCELLED, "client cancelled reverse call"};
            if (!registry.current(session) || (stillAssigned && !stillAssigned() && (!resultReceived || !resultReceived())))
                return {grpc::StatusCode::UNAVAILABLE,
                    registry.current(session) ? "reverse call reassigned" : "reverse worker displaced"};
            if (frame.has_finish()) {
                if (stillAssigned && !stillAssigned() && (!resultReceived || !resultReceived()))
                    return {grpc::StatusCode::FAILED_PRECONDITION, "build assignment changed"};
                if (!pending.empty() || expected) return {grpc::StatusCode::DATA_LOSS, "incomplete reverse response"};
                if (resultReceived && resultReceived()) {
                    if (beforeAck) beforeAck();
                    if (!transport.outgoing.acknowledge(id))
                        return {grpc::StatusCode::UNAVAILABLE, "cannot acknowledge reverse result"};
                }
                release.completed = true;
                return {static_cast<grpc::StatusCode>(frame.finish().code()), frame.finish().message()};
            }
            pending += frame.data().data();
            while (true) {
                if (!expected) {
                    if (pending.size() < 4) break;
                    uint32_t length = 0;
                    for (unsigned i = 0; i != 4; ++i)
                        length |= static_cast<uint32_t>(static_cast<unsigned char>(pending[i])) << (8 * i);
                    if (length > maxMessageBytes) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse response too large"};
                    expected = length;
                    pending.erase(0, 4);
                }
                if (pending.size() < *expected) break;
                if (!receive(std::string_view(pending).substr(0, *expected)))
                    return {grpc::StatusCode::DATA_LOSS, "invalid or rejected reverse response"};
                pending.erase(0, *expected);
                expected.reset();
            }
            if (pending.size() > maxMessageBytes + 4) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse response too large"};
        }
        if (context.IsCancelled()) return {grpc::StatusCode::CANCELLED, "client cancelled reverse call"};
        if (std::chrono::system_clock::now() >= deadline) return {grpc::StatusCode::DEADLINE_EXCEEDED, "reverse call timed out"};
        if (stillAssigned && !stillAssigned() && (!resultReceived || !resultReceived()))
            return {grpc::StatusCode::FAILED_PRECONDITION, "build assignment changed"};
        return {grpc::StatusCode::UNAVAILABLE, "reverse worker session ended"};
    }

private:
    ReverseGateway & gateway;
    ReverseRegistry & registry;
};

} // namespace nixgrpc
