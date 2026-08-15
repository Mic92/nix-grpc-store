// gRPC server that proxies tunnelled Nix worker-protocol connections straight
// to a nix-daemon unix socket. The real nix-daemon handles trust, forking and
// interrupt-on-hangup; this process only moves bytes (and optionally zstd).
//
// QueryValidPaths, QueryPathInfos, AddMultipleToStore and NarsFromPaths are
// handled natively (via a Store opened on the same socket) so `nix copy`
// avoids the tunnel's per-batch zstd flushes and per-path round trips.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <initializer_list>
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
#include <nix/util/ref.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "logfmt.hh"
#include "metrics.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

using GrpcStream = grpc::ServerReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;
using AddMultipleReader = grpc::ServerReader<nix::remote::AddMultipleChunk>;
using NarsStream = grpc::ServerReaderWriter<nix::remote::NarChunk, nix::remote::NarRequest>;

namespace {

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    std::string socketPath;
    nixgrpc::Metrics & metrics;
    nixgrpc::LogLevel logLevel;

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

    void logDebug(std::initializer_list<std::pair<std::string_view, std::string>> fields)
    {
        if (logLevel == nixgrpc::LogLevel::debug) {
            nixgrpc::logLine(nixgrpc::LogLevel::debug, fields);
        }
    }

    static auto secondsSince(std::chrono::steady_clock::time_point start) -> std::string
    {
        auto const elapsed = std::chrono::steady_clock::now() - start;
        return std::to_string(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count());
    }

public:
    NixRemoteService(std::string socketPath, nixgrpc::Metrics & metrics, nixgrpc::LogLevel logLevel)
        : socketPath(std::move(socketPath))
        , metrics(metrics)
        , logLevel(logLevel)
    {
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
        auto const commonName = nixgrpc::clientCommonName(*context);
        auto const peer = context->peer();
        auto const start = std::chrono::steady_clock::now();
        logDebug({{"event", "session_start"}, {"method", "Connect"}, {"cn", commonName}, {"peer", peer}});
        metrics.countRpc("Connect", commonName);

        nix::AutoCloseFD sock;
        try {
            sock = nix::connect(std::filesystem::path{socketPath});
        } catch (nix::Error & err) {
            return {grpc::StatusCode::UNAVAILABLE, err.what()};
        }

        std::atomic<uint64_t> bytesIn{0};
        std::thread receiver([&]() -> void {
            try {
                bytesIn = nixgrpc::pumpStreamToFd(*stream, sock.get());
            } catch (...) {
                nix::ignoreExceptionInDestructor();
            }
            ::shutdown(sock.get(), SHUT_WR);
        });

        uint64_t bytesOut = 0;
        try {
            bytesOut = nixgrpc::pumpFdToStream(sock.get(), *stream);
        } catch (...) {
            nix::ignoreExceptionInDestructor();
        }

        receiver.join();
        nixgrpc::logLine(
            nixgrpc::LogLevel::info,
            {{"event", "session_end"},
             {"method", "Connect"},
             {"cn", commonName},
             {"peer", peer},
             {"duration_s", secondsSince(start)},
             {"bytes_in", std::to_string(bytesIn.load())},
             {"bytes_out", std::to_string(bytesOut)}});
        metrics.countTunnelBytes(commonName, bytesIn, bytesOut);
        return grpc::Status::OK;
    }

    auto QueryValidPaths(
        grpc::ServerContext * context,
        const nix::remote::QueryValidPathsRequest * request,
        nix::remote::QueryValidPathsReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const commonName = nixgrpc::clientCommonName(*context);
            logDebug(
                {{"event", "rpc"},
                 {"method", "QueryValidPaths"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"paths", std::to_string(request->paths_size())}});
            metrics.countRpc("QueryValidPaths", commonName);
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
        grpc::ServerContext * context,
        AddMultipleReader * reader,
        nix::remote::AddMultipleReply * /*reply*/) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const commonName = nixgrpc::clientCommonName(*context);
            auto const peer = context->peer();
            auto const start = std::chrono::steady_clock::now();
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
            uint64_t narBytes = 0;
            for (uint64_t idx = 0; idx < expected; ++idx) {
                auto info = nix::WorkerProto::Serialise<nix::ValidPathInfo>::read(
                    *localStore,
                    nix::WorkerProto::ReadConn{.from = source, .version = nixcompat::infoProtocolVersion()});
                info.ultimate = false;
                nixcompat::EnsureRead wrapper{source, info.narSize};
                localStore->addToStore(info, wrapper, repair, checkSigs);
                wrapper.finish();
                narBytes += info.narSize;
            }
            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "AddMultipleToStore"},
                 {"cn", commonName},
                 {"peer", peer},
                 {"duration_s", secondsSince(start)},
                 {"paths", std::to_string(expected)},
                 {"nar_bytes_in", std::to_string(narBytes)}});
            metrics.countRpc("AddMultipleToStore", commonName);
            metrics.countNarBytes("in", commonName, narBytes);
            return grpc::Status::OK;
        });
    }

    auto QueryPathInfos(
        grpc::ServerContext * context,
        const nix::remote::QueryPathInfosRequest * request,
        nix::remote::QueryPathInfosReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const commonName = nixgrpc::clientCommonName(*context);
            logDebug(
                {{"event", "rpc"},
                 {"method", "QueryPathInfos"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"paths", std::to_string(request->paths_size())}});
            metrics.countRpc("QueryPathInfos", commonName);
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
                    nix::WorkerProto::WriteConn{.to = sink, .version = nixcompat::infoProtocolVersion()},
                    static_cast<const nix::UnkeyedValidPathInfo &>(*info));
                *out->mutable_info() = std::move(sink.s);
            }
            return grpc::Status::OK;
        });
    }

    auto NarsFromPaths(grpc::ServerContext * context, NarsStream * stream) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const commonName = nixgrpc::clientCommonName(*context);
            auto const start = std::chrono::steady_clock::now();
            auto localStore = getStore();
            nixgrpc::ZstdWriterSink<NarsStream, nix::remote::NarChunk> sink(*stream);

            uint64_t narBytes = 0;
            nix::LambdaSink counting([&](std::string_view data) -> void {
                sink(data);
                narBytes += data.size();
            });

            uint64_t paths = 0;
            nix::remote::NarRequest request;
            while (stream->Read(&request)) {
                localStore->narFromPath(nix::StorePath(request.path()), counting);
                // The client blocks on this NAR (requests may be pipelined,
                // but NARs are consumed in request order), so it must not
                // linger in the encoder.
                sink.flush();
                ++paths;
            }
            sink.finish();
            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "NarsFromPaths"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"duration_s", secondsSince(start)},
                 {"paths", std::to_string(paths)},
                 {"nar_bytes_out", std::to_string(narBytes)}});
            metrics.countRpc("NarsFromPaths", commonName);
            metrics.countNarBytes("out", commonName, narBytes);
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
    std::string metricsListen;
    nixgrpc::LogLevel logLevel = nixgrpc::LogLevel::info;
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
        } else if (arg == "--metrics-listen") {
            options.metricsListen = next();
        } else if (arg == "--log-level") {
            auto value = next();
            if (value == "debug") {
                options.logLevel = nixgrpc::LogLevel::debug;
            } else if (value != "info") {
                throw nix::Error("--log-level must be 'info' or 'debug'");
            }
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

    nixgrpc::Metrics metrics(options.metricsListen);
    NixRemoteService service(options.socketPath, metrics, options.logLevel);

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

    nixgrpc::logLine(
        nixgrpc::LogLevel::info,
        {{"event", "startup"}, {"listen", options.listen}, {"proxy_socket", options.socketPath}});
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
