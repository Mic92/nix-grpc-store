// gRPC server that proxies tunnelled Nix worker-protocol connections straight
// to a nix-daemon unix socket. The real nix-daemon handles trust, forking and
// interrupt-on-hangup; this process only moves bytes (and optionally zstd).
//
// QueryValidPaths, QueryPathInfos, AddMultipleToStore and NarsFromPaths are
// handled natively (via a Store opened on the same socket) so `nix copy`
// avoids the tunnel's per-batch zstd flushes and per-path round trips.

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/globals.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>
#include <nix/util/fmt.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

using GrpcStream = grpc::ServerReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;
using AddMultipleReader = grpc::ServerReader<nix::remote::AddMultipleChunk>;
using NarsStream = grpc::ServerReaderWriter<nix::remote::NarChunk, nix::remote::NarRequest>;

namespace {

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    /* nix::ValidPathInfo serialisation used by the bulk RPCs, matching the worker
       protocol's AddMultipleToStore framing. */
    static constexpr nix::WorkerProto::Version::Number infoVersion{.major = 1, .minor = 16};

    std::string socketPath;

    std::mutex storeMutex;
    std::shared_ptr<nix::Store> store;

    // The Store connects lazily and pools connections, but opening it can
    // still throw (e.g. daemon socket missing), so defer to first use.
    auto getStore() -> nix::ref<nix::Store>
    {
        std::scoped_lock const lock(storeMutex);
        if (!store) {
            store = nix::openStore("unix://" + socketPath).get_ptr();
        }
        return nix::ref<nix::Store>(store);
    }

    // gRPC aborts the process if a handler lets an exception escape.
    template<typename F>
    static auto guarded(F && func) -> grpc::Status
    {
        try {
            return std::forward<F>(func)();
        } catch (std::exception & err) {
            return {grpc::StatusCode::INTERNAL, err.what()};
        }
    }

public:
    explicit NixRemoteService(std::string socketPath)
        : socketPath(std::move(socketPath))
    {
    }

    auto Connect(grpc::ServerContext * /*context*/, GrpcStream * stream) -> grpc::Status override
    {
        nix::AutoCloseFD sock;
        try {
            sock = nix::connect(std::filesystem::path{socketPath});
        } catch (nix::Error & err) {
            return {grpc::StatusCode::UNAVAILABLE, err.what()};
        }

        std::thread receiver([&]() -> void {
            try {
                nixgrpc::pumpStreamToFd(*stream, sock.get());
            } catch (...) {
                nix::ignoreExceptionInDestructor();
            }
            ::shutdown(sock.get(), SHUT_WR);
        });

        try {
            nixgrpc::pumpFdToStream(sock.get(), *stream);
        } catch (...) {
            nix::ignoreExceptionInDestructor();
        }

        receiver.join();
        return grpc::Status::OK;
    }

    auto QueryValidPaths(
        grpc::ServerContext * /*context*/,
        const nix::remote::QueryValidPathsRequest * request,
        nix::remote::QueryValidPathsReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto localStore = getStore();
            nix::StorePathSet paths;
            for (const auto & path : request->paths()) {
                paths.insert(nix::StorePath(path));
            }
            for (const auto & path :
                 localStore->queryValidPaths(paths, request->substitute() ? nix::Substitute : nix::NoSubstitute)) {
                reply->add_paths(std::string(path.to_string()));
            }
            return grpc::Status::OK;
        });
    }

    auto AddMultipleToStore(
        grpc::ServerContext * /*context*/,
        AddMultipleReader * reader,
        nix::remote::AddMultipleReply * /*reply*/) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto localStore = getStore();

            nix::remote::AddMultipleChunk first;
            if (!reader->Read(&first)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "empty AddMultipleToStore stream"};
            }
            auto repair = first.repair() ? nix::Repair : nix::NoRepair;
            // The nix-daemon downgrades this to CheckSigs if we are not a
            // trusted user, same as for the tunnelled protocol.
            auto checkSigs = first.check_sigs() ? nix::CheckSigs : nix::NoCheckSigs;

            nixgrpc::ZstdReaderSource<AddMultipleReader, nix::remote::AddMultipleChunk> source(
                *reader, std::move(*first.mutable_data()));

            // Same framing as nix::WorkerProto::Op::AddMultipleToStore.
            auto expected = nix::readNum<uint64_t>(source);
            for (uint64_t idx = 0; idx < expected; ++idx) {
                auto info = nix::WorkerProto::Serialise<nix::ValidPathInfo>::read(
                    *localStore, nix::WorkerProto::ReadConn{.from = source, .version = {.number = infoVersion}});
                info.ultimate = false;
                nix::EnsureRead wrapper{source, info.narSize};
                localStore->addToStore(info, wrapper, repair, checkSigs);
                wrapper.finish();
            }
            return grpc::Status::OK;
        });
    }

    auto QueryPathInfos(
        grpc::ServerContext * /*context*/,
        const nix::remote::QueryPathInfosRequest * request,
        nix::remote::QueryPathInfosReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto localStore = getStore();
            for (const auto & path : request->paths()) {
                std::shared_ptr<const nix::ValidPathInfo> info;
                try {
                    info = localStore->queryPathInfo(nix::StorePath(path));
                } catch (nix::InvalidPath &) {
                    continue;
                }
                auto * out = reply->add_infos();
                out->set_path(std::string(info->path.to_string()));
                nix::StringSink sink;
                nix::WorkerProto::Serialise<nix::UnkeyedValidPathInfo>::write(
                    *localStore,
                    nix::WorkerProto::WriteConn{.to = sink, .version = {.number = infoVersion}},
                    static_cast<const nix::UnkeyedValidPathInfo &>(*info));
                *out->mutable_info() = std::move(sink.s);
            }
            return grpc::Status::OK;
        });
    }

    auto NarsFromPaths(grpc::ServerContext * /*context*/, NarsStream * stream) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto localStore = getStore();
            nixgrpc::ZstdWriterSink<NarsStream, nix::remote::NarChunk> sink(*stream);
            nix::remote::NarRequest request;
            while (stream->Read(&request)) {
                localStore->narFromPath(nix::StorePath(request.path()), sink);
                // The client blocks on this NAR before sending the next
                // request, so it must not linger in the encoder.
                sink.flush();
            }
            sink.finish();
            return grpc::Status::OK;
        });
    }
};

struct Options
{
    std::string listen = "0.0.0.0:50051";
    std::string socketPath = "/nix/var/nix/daemon-socket/socket";
    std::string tlsCert;
    std::string tlsKey;
    std::string clientCA;
};

auto parseOptions(const std::vector<std::string_view> & args) -> Options
{
    Options options;
    for (size_t idx = 1; idx < args.size(); ++idx) {
        std::string_view const arg = args.at(idx);
        auto next = [&]() -> std::string_view {
            if (++idx >= args.size()) {
                throw nix::Error("flag '%s' requires an argument", arg);
            }
            return args.at(idx);
        };
        if (arg == "--listen") {
            options.listen = next();
        } else if (arg == "--proxy-socket") {
            options.socketPath = next();
        } else if (arg == "--tls-cert") {
            options.tlsCert = next();
        } else if (arg == "--tls-key") {
            options.tlsKey = next();
        } else if (arg == "--client-ca") {
            options.clientCA = next();
        } else {
            throw nix::Error("unknown flag '%s'", arg);
        }
    }
    return options;
}

auto makeServerCredentials(const Options & options) -> std::shared_ptr<grpc::ServerCredentials>
{
    if (options.tlsCert.empty()) {
        if (!options.clientCA.empty()) {
            throw nix::Error("--client-ca requires --tls-cert/--tls-key");
        }
        return grpc::InsecureServerCredentials();
    }
    // With --client-ca the server requires and verifies a client certificate
    // (mTLS); without it, plain server-auth TLS.
    grpc::SslServerCredentialsOptions ssl(
        options.clientCA.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
                                 : GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
    ssl.pem_key_cert_pairs.push_back(
        {.private_key = nix::readFile(options.tlsKey), .cert_chain = nix::readFile(options.tlsCert)});
    if (!options.clientCA.empty()) {
        ssl.pem_root_certs = nix::readFile(options.clientCA);
    }
    return grpc::SslServerCredentials(ssl);
}

} // namespace

auto main(int argc, char ** argv) -> int
try {
    // Pump threads write to a socket whose peer may already be gone; we want
    // EPIPE, not process death.
    // NOLINTNEXTLINE(misc-include-cleaner): SIGPIPE comes from <csignal>.
    static_cast<void>(std::signal(SIGPIPE, SIG_IGN));

    // Required before nix::openStore() in the native RPC handlers.
    nix::initLibStore();

    const std::span args(argv, static_cast<size_t>(argc));
    auto options = parseOptions({args.begin(), args.end()});

    NixRemoteService service(options.socketPath);

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    builder.AddListeningPort(options.listen, makeServerCredentials(options));
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server) {
        throw nix::Error("failed to start gRPC server on '%s'", options.listen);
    }

    nix::logger->log(nix::lvlInfo, nix::fmt("nix-grpc-daemon listening on %s → %s", options.listen, options.socketPath));
    server->Wait();
    return 0;
} catch (const std::exception & err) {
    // Formatting or logging could itself throw and escape main; use plain
    // C stdio which cannot.
    static_cast<void>(std::fputs("nix-grpc-daemon: ", stderr));
    static_cast<void>(std::fputs(err.what(), stderr));
    static_cast<void>(std::fputc('\n', stderr));
    return 1;
}
