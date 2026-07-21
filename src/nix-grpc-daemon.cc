// gRPC server that proxies tunnelled Nix worker-protocol connections straight
// to a nix-daemon unix socket. The real nix-daemon handles trust, forking and
// interrupt-on-hangup; this process only moves bytes (and optionally zstd).
//
// QueryValidPaths, QueryPathInfos, AddMultipleToStore and NarsFromPaths are
// handled natively (via a Store opened on the same socket) so `nix copy`
// avoids the tunnel's per-batch zstd flushes and per-path round trips.

#include <csignal>
#include <mutex>
#include <sys/socket.h>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <nix/store/globals.hh>
#include <nix/store/path-info.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>
#include <nix/util/logging.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "pump.hh"

using namespace nix;
using GrpcStream = grpc::ServerReaderWriter<remote::Chunk, remote::Chunk>;

class NixRemoteService final : public remote::NixRemote::Service
{
    /* ValidPathInfo serialisation used by the bulk RPCs, matching the worker
       protocol's AddMultipleToStore framing. */
    static constexpr WorkerProto::Version::Number infoVersion{.major = 1, .minor = 16};

    std::string socketPath;

    std::mutex storeMutex;
    std::shared_ptr<Store> store_;

    // The Store connects lazily and pools connections, but opening it can
    // still throw (e.g. daemon socket missing), so defer to first use.
    ref<Store> getStore()
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        if (!store_)
            store_ = openStore("unix://" + socketPath).get_ptr();
        return ref<Store>(store_);
    }

    // gRPC aborts the process if a handler lets an exception escape.
    template<typename F>
    static grpc::Status guarded(F && f)
    {
        try {
            return f();
        } catch (Error & e) {
            return {grpc::StatusCode::INTERNAL, e.what()};
        } catch (std::exception & e) {
            return {grpc::StatusCode::INTERNAL, e.what()};
        }
    }

public:
    explicit NixRemoteService(std::string socketPath)
        : socketPath(std::move(socketPath))
    {
    }

    grpc::Status Connect(grpc::ServerContext *, GrpcStream * stream) override
    {
        AutoCloseFD sock;
        try {
            sock = nix::connect(std::filesystem::path{socketPath});
        } catch (Error & e) {
            return {grpc::StatusCode::UNAVAILABLE, e.what()};
        }

        std::thread recvT([&] {
            try {
                nixgrpc::pumpStreamToFd(*stream, sock.get());
            } catch (...) {
                ignoreExceptionInDestructor();
            }
            ::shutdown(sock.get(), SHUT_WR);
        });

        try {
            nixgrpc::pumpFdToStream(sock.get(), *stream);
        } catch (...) {
            ignoreExceptionInDestructor();
        }

        recvT.join();
        return grpc::Status::OK;
    }

    grpc::Status QueryValidPaths(
        grpc::ServerContext *,
        const remote::QueryValidPathsRequest * request,
        remote::QueryValidPathsReply * reply) override
    {
        return guarded([&] {
            auto store = getStore();
            StorePathSet paths;
            for (auto & p : request->paths())
                paths.insert(StorePath(p));
            for (auto & p : store->queryValidPaths(paths, request->substitute() ? Substitute : NoSubstitute))
                reply->add_paths(std::string(p.to_string()));
            return grpc::Status::OK;
        });
    }

    grpc::Status AddMultipleToStore(
        grpc::ServerContext *,
        grpc::ServerReader<remote::AddMultipleChunk> * reader,
        remote::AddMultipleReply *) override
    {
        return guarded([&]() -> grpc::Status {
            auto store = getStore();

            remote::AddMultipleChunk first;
            if (!reader->Read(&first))
                return {grpc::StatusCode::INVALID_ARGUMENT, "empty AddMultipleToStore stream"};
            auto repair = first.repair() ? Repair : NoRepair;
            // The nix-daemon downgrades this to CheckSigs if we are not a
            // trusted user, same as for the tunnelled protocol.
            auto checkSigs = first.check_sigs() ? CheckSigs : NoCheckSigs;

            nixgrpc::ZstdReaderSource<grpc::ServerReader<remote::AddMultipleChunk>, remote::AddMultipleChunk> source(
                *reader, std::move(*first.mutable_data()));

            // Same framing as WorkerProto::Op::AddMultipleToStore.
            auto expected = readNum<uint64_t>(source);
            for (uint64_t i = 0; i < expected; ++i) {
                auto info = WorkerProto::Serialise<ValidPathInfo>::read(
                    *store, WorkerProto::ReadConn{.from = source, .version = {.number = infoVersion}});
                info.ultimate = false;
                EnsureRead wrapper{source, info.narSize};
                store->addToStore(info, wrapper, repair, checkSigs);
                wrapper.finish();
            }
            return grpc::Status::OK;
        });
    }

    grpc::Status QueryPathInfos(
        grpc::ServerContext *,
        const remote::QueryPathInfosRequest * request,
        remote::QueryPathInfosReply * reply) override
    {
        return guarded([&] {
            auto store = getStore();
            for (auto & p : request->paths()) {
                std::shared_ptr<const ValidPathInfo> info;
                try {
                    info = store->queryPathInfo(StorePath(p));
                } catch (InvalidPath &) {
                    continue;
                }
                auto * out = reply->add_infos();
                out->set_path(std::string(info->path.to_string()));
                StringSink sink;
                WorkerProto::Serialise<UnkeyedValidPathInfo>::write(
                    *store,
                    WorkerProto::WriteConn{.to = sink, .version = {.number = infoVersion}},
                    static_cast<const UnkeyedValidPathInfo &>(*info));
                *out->mutable_info() = std::move(sink.s);
            }
            return grpc::Status::OK;
        });
    }

    grpc::Status NarsFromPaths(
        grpc::ServerContext *, grpc::ServerReaderWriter<remote::NarChunk, remote::NarRequest> * stream) override
    {
        return guarded([&] {
            auto store = getStore();
            nixgrpc::ZstdWriterSink<grpc::ServerReaderWriter<remote::NarChunk, remote::NarRequest>, remote::NarChunk>
                sink(*stream);
            remote::NarRequest request;
            while (stream->Read(&request)) {
                store->narFromPath(StorePath(request.path()), sink);
                // The client blocks on this NAR before sending the next
                // request, so it must not linger in the encoder.
                sink.flush();
            }
            sink.finish();
            return grpc::Status::OK;
        });
    }
};

int main(int argc, char ** argv)
try {
    // Pump threads write to a socket whose peer may already be gone; we want
    // EPIPE, not process death.
    std::signal(SIGPIPE, SIG_IGN);

    // Required before openStore() in the native RPC handlers.
    initLibStore();

    std::string listen = "0.0.0.0:50051";
    std::string socketPath = "/nix/var/nix/daemon-socket/socket";
    std::string tlsCert, tlsKey, clientCA;

    for (int i = 1; i < argc; ++i) {
        std::string_view a = argv[i];
        auto next = [&]() -> std::string {
            if (++i >= argc)
                throw Error("flag '%s' requires an argument", a);
            return argv[i];
        };
        if (a == "--listen")
            listen = next();
        else if (a == "--proxy-socket")
            socketPath = next();
        else if (a == "--tls-cert")
            tlsCert = next();
        else if (a == "--tls-key")
            tlsKey = next();
        else if (a == "--client-ca")
            clientCA = next();
        else
            throw Error("unknown flag '%s'", a);
    }

    NixRemoteService service(socketPath);

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    if (!tlsCert.empty()) {
        // With --client-ca the server requires and verifies a client
        // certificate (mTLS); without it, plain server-auth TLS.
        grpc::SslServerCredentialsOptions ssl(
            clientCA.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
                             : GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
        ssl.pem_key_cert_pairs.push_back({readFile(tlsKey), readFile(tlsCert)});
        if (!clientCA.empty())
            ssl.pem_root_certs = readFile(clientCA);
        builder.AddListeningPort(listen, grpc::SslServerCredentials(ssl));
    } else {
        if (!clientCA.empty())
            throw Error("--client-ca requires --tls-cert/--tls-key");
        builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
    }

    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server)
        throw Error("failed to start gRPC server on '%s'", listen);

    printInfo("nix-grpc-daemon listening on %s → %s", listen, socketPath);
    server->Wait();
    return 0;
} catch (Error & e) {
    printError("nix-grpc-daemon: %s", e.what());
    return 1;
}
