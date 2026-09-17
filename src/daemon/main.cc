// gRPC server that proxies tunnelled Nix worker-protocol connections straight
// to a nix-daemon unix socket. The real nix-daemon handles trust, forking and
// interrupt-on-hangup; this process only moves bytes (and optionally zstd).
//
// QueryValidPaths, QueryPathInfos, AddMultipleToStore and NarsFromPaths are
// handled natively (via a Store opened on the same socket) so `nix copy`
// avoids the tunnel's per-batch zstd flushes and per-path round trips.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <signal.h> // NOLINT(modernize-deprecated-headers): sigaction is POSIX, not in <csignal>
#include <cstddef>
#include <initializer_list>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <sys/socket.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/globals.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/worker-protocol.hh>
// Generic definitions for the std::vector serialiser wrappers used by
// BuildPaths, which libnixstore does not instantiate explicitly.
#include <nix/store/worker-protocol-impl.hh> // IWYU pragma: keep
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>
#include <nix/util/ref.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "build-log.hh"
#include "acl.hh"
#include "auth.hh"
#include "backend.hh"
#include "options.hh"
#include "idle.hh"
#include "import-paths.hh"
#include "logfmt.hh"
#include "path-info-wire.hh"
#include "metrics.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"
#include "socket-activation.hh"

using GrpcStream = grpc::ServerReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;
using AddMultipleReader = grpc::ServerReader<nix::remote::AddMultipleChunk>;
using NarFrameWriter = grpc::ServerWriter<nix::remote::NarFrame>;
using BuildWriter = grpc::ServerWriter<nix::remote::BuildDerivationChunk>;
using BuildPathsWriter = grpc::ServerWriter<nix::remote::BuildPathsChunk>;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler.
volatile std::sig_atomic_t nixgrpc::stopSignal = 0;

namespace {
using nixgrpc::stopSignal;

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    std::string storeUri;
    nixgrpc::Metrics & metrics;
    nixgrpc::IdleTracker & idle;
    nixgrpc::LogLevel logLevel;
    nixgrpc::Auth auth;
    nixgrpc::Backends backends;

    std::mutex storeMutex;
    std::shared_ptr<nix::Store> store;

    // The Store connects lazily and pools connections, but opening it can
    // still throw (e.g. daemon socket missing), so defer to first use.
    auto getStore() -> nix::ref<nix::Store>
    {
        std::scoped_lock const lock(storeMutex);
        if (!store) {
            store = nix::openStore(storeUri).get_ptr();
        }
        return nix::ref<nix::Store>(store);
    }

    // nix-daemon keeps temp roots per connection, so writes get their own.
    auto openScopedStore() -> nix::ref<nix::Store>
    {
        return nix::openStore(storeUri);
    }

    // gRPC aborts the process if a handler lets an exception escape.
    template<typename F>
    auto guarded(F && func) -> grpc::Status
    {
        nixgrpc::IdleTracker::Guard const active(idle);
        try {
            return std::forward<F>(func)();
        } catch (std::exception & err) {
            nixgrpc::logLine(
                nixgrpc::LogLevel::info, {{"event", "handler_error"}, {"error", std::string(err.what())}});
            return {grpc::StatusCode::INTERNAL, err.what()};
        }
    }

    void logDebug(std::initializer_list<std::pair<std::string_view, std::string>> fields)
    {
        if (logLevel == nixgrpc::LogLevel::debug) {
            nixgrpc::logLine(nixgrpc::LogLevel::debug, fields);
        }
    }

    using Fields = std::vector<nixgrpc::LogField>;

    // The authorised, counted part of an RPC. done() writes the one info line per call.
    struct Rpc
    {
        grpc::ServerContext * context;
        std::string_view method;
        nixgrpc::Caller caller;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

        void done(const Fields & extra) const
        {
            auto const secs = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start);
            Fields fields{
                {"event", "rpc"},
                {"method", std::string(method)},
                {"cn", caller.name},
                {"peer", context->peer()},
                {"duration_s", std::to_string(secs.count())}};
            fields.insert(fields.end(), extra.begin(), extra.end());
            nixgrpc::logLine(nixgrpc::LogLevel::info, fields);
        }
    };

    // identify + authorize + count. Cheap RPCs log at debug here, expensive ones call done().
    auto begin(grpc::ServerContext & context, std::string_view method, nixgrpc::Role minRole, uint32_t buildMode = 0)
        -> std::variant<grpc::Status, Rpc>
    {
        auto caller = auth.identify(context);
        if (auto status = nixgrpc::Auth::authorize(caller, method, minRole, buildMode); !status.ok()) {
            return status;
        }
        metrics.countRpc(std::string(method), caller.name);
        logDebug({{"event", "rpc_start"}, {"method", std::string(method)}, {"cn", caller.name}, {"peer", context.peer()}});
        return Rpc{.context = &context, .method = method, .caller = std::move(caller)};
    }

public:
    NixRemoteService(
        std::string socketPath,
        std::string storeUri,
        nixgrpc::Metrics & metrics,
        nixgrpc::IdleTracker & idle,
        nixgrpc::LogLevel logLevel,
        nixgrpc::Acl acl)
        : storeUri(std::move(storeUri))
        , metrics(metrics)
        , idle(idle)
        , logLevel(logLevel)
        , auth{.acl = std::move(acl)}
        , backends{.socketPath = std::move(socketPath)}
    {
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
        nixgrpc::IdleTracker::Guard const active(idle);
        // The opaque worker protocol cannot be inspected here.
        auto started = begin(*context, "Connect", nixgrpc::Role::trusted);
        if (auto * denied = std::get_if<grpc::Status>(&started)) {
            return *denied;
        }
        auto & rpc = std::get<Rpc>(started);

        nix::AutoCloseFD sock;
        try {
            sock = nix::connect(std::filesystem::path{backends.socketPath});
        } catch (nix::Error & err) {
            return {grpc::StatusCode::UNAVAILABLE, err.what()};
        }

        std::atomic<uint64_t> bytesIn{0};
        std::jthread receiver([&]() -> void {
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
        rpc.done({{"bytes_in", std::to_string(bytesIn.load())}, {"bytes_out", std::to_string(bytesOut)}});
        metrics.countTunnelBytes(rpc.caller.name, bytesIn, bytesOut);
        return grpc::Status::OK;
    }

    auto QueryValidPaths(
        grpc::ServerContext * context,
        const nix::remote::QueryValidPathsRequest * request,
        nix::remote::QueryValidPathsReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "QueryValidPaths", nixgrpc::Role::readOnly);
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            static_cast<void>(rpc);
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
            auto started = begin(*context, "AddMultipleToStore", nixgrpc::Role::write);
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            auto localStore = openScopedStore();

            nix::remote::AddMultipleChunk first;
            if (!reader->Read(&first)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "empty AddMultipleToStore stream"};
            }
            auto repair = first.repair() ? nix::Repair : nix::NoRepair;
            // The nix-daemon downgrades this to CheckSigs if we are not a
            // trusted user, same as for the tunnelled protocol.
            auto checkSigs = first.check_sigs() ? nix::CheckSigs : nix::NoCheckSigs;
            if (rpc.caller.role == nixgrpc::Role::write) {
                // write may only import signed paths, no matter how trusted
                // the proxy's own uid is.
                repair = nix::NoRepair;
                checkSigs = nix::CheckSigs;
            }

            nixgrpc::ZstdReaderSource<AddMultipleReader, nix::remote::AddMultipleChunk> source(
                *reader, std::move(*first.mutable_data()));
            auto stats = nixgrpc::importPaths(
                *localStore, source, [&](const nix::ValidPathInfo & info, nix::Source & nar) -> void {
                    localStore->addToStore(info, nar, repair, checkSigs);
                });
            rpc.done({{"paths", std::to_string(stats.paths)}, {"nar_bytes_in", std::to_string(stats.narBytes)}});
            metrics.countNarBytes("in", rpc.caller.name, stats.narBytes);
            return grpc::Status::OK;
        });
    }

    auto QueryPathInfos(
        grpc::ServerContext * context,
        const nix::remote::QueryPathInfosRequest * request,
        nix::remote::QueryPathInfosReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "QueryPathInfos", nixgrpc::Role::readOnly);
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            static_cast<void>(rpc);
            auto localStore = getStore();
            for (const auto & path : request->paths()) {
                std::shared_ptr<const nix::ValidPathInfo> info;
                try {
                    info = localStore->queryPathInfo(nix::StorePath(path)).get_ptr();
                } catch (nix::InvalidPath &) {
                    continue;
                }
                nixgrpc::encodePathInfo(*localStore, *info, reply->add_infos());
            }
            return grpc::Status::OK;
        });
    }

    auto QueryMissing(
        grpc::ServerContext * context,
        const nix::remote::QueryMissingRequest * request,
        nix::remote::QueryMissingReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "QueryMissing", nixgrpc::Role::readOnly);
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            static_cast<void>(rpc);
            auto localStore = getStore();
            auto missing = localStore->queryMissing(parseTargets(*localStore, request->targets()));
            for (const auto & path : missing.willBuild) {
                reply->add_will_build(std::string(path.to_string()));
            }
            for (const auto & path : missing.willSubstitute) {
                reply->add_will_substitute(std::string(path.to_string()));
            }
            for (const auto & path : missing.unknown) {
                reply->add_unknown(std::string(path.to_string()));
            }
            reply->set_download_size(missing.downloadSize);
            reply->set_nar_size(missing.narSize);
            return grpc::Status::OK;
        });
    }

    auto StoreInfo(
        grpc::ServerContext * context,
        const nix::remote::StoreInfoRequest * /*request*/,
        nix::remote::StoreInfoReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "StoreInfo", nixgrpc::Role::readOnly);
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            static_cast<void>(rpc);
            auto backend = backends.connect(*getStore());
            if (backend->info.remoteTrustsUs) {
                reply->set_trusted(*backend->info.remoteTrustsUs == nix::Trusted);
            }
            return grpc::Status::OK;
        });
    }

    static auto parseTargets(nix::Store & store, const auto & strings) -> std::vector<nix::DerivedPath>
    {
        std::vector<nix::DerivedPath> targets;
        targets.reserve(static_cast<size_t>(strings.size()));
        for (const auto & target : strings) {
            targets.push_back(nix::DerivedPath::parse(store, target));
        }
        return targets;
    }

    auto BuildDerivation(
        grpc::ServerContext * context,
        const nix::remote::BuildDerivationRequest * request,
        BuildWriter * writer) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "BuildDerivation", nixgrpc::Role::write, request->build_mode());
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            auto localStore = getStore();
            auto sendLogLine = [&](nixgrpc::BuildEvent event) -> void {
                writer->Write(nixgrpc::toChunk<nix::remote::BuildDerivationChunk>(std::move(event)));
            };

            nix::StorePath const drvPath(request->drv_path());
            nix::StringSource drvSource(request->drv());
            nix::BasicDerivation drv;
            nixcompat::readDrv(drvSource, *localStore, drv, nix::Derivation::nameFromPath(drvPath));
            auto const mode = static_cast<nix::BuildMode>(request->build_mode());

            auto res = backends.proxyBuild(*context, *localStore, drvPath, drv, mode, sendLogLine);

            nix::remote::BuildDerivationChunk chunk;
            nixgrpc::encodeResult(*localStore, res, chunk.mutable_done());
            writer->Write(chunk);

            rpc.done({{"drv", std::string(drvPath.to_string())}, {"outputs", std::to_string(chunk.done().outputs_size())}});
            return grpc::Status::OK;
        });
    }

    // Builds run entirely server-side under the proxy user, so the write
    // role suffices where the raw worker-protocol tunnel would not.
    auto BuildPaths(
        grpc::ServerContext * context,
        const nix::remote::BuildPathsRequest * request,
        BuildPathsWriter * writer) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "BuildPaths", nixgrpc::Role::write, request->build_mode());
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            auto localStore = getStore();
            auto targets = parseTargets(*localStore, request->targets());
            auto sendLogLine = [&](nixgrpc::BuildEvent event) -> void {
                writer->Write(nixgrpc::toChunk<nix::remote::BuildPathsChunk>(std::move(event)));
            };
            auto backend = backends.forBuild(*context, *localStore);
            auto results = nixgrpc::Backends::buildPathsVia(
                *backend, *localStore, targets, static_cast<nix::BuildMode>(request->build_mode()), sendLogLine);

            nix::remote::BuildPathsChunk chunk;
            auto * done = chunk.mutable_done();
            nix::StringSink sink;
            nix::WorkerProto::Serialise<std::vector<nix::KeyedBuildResult>>::write(
                *localStore,
                nix::WorkerProto::WriteConn{.to = sink, .version = nixcompat::buildProtocolVersion()},
                results);
            *done->mutable_results() = std::move(sink.s);
            nix::StorePathSet outputPaths;
            for (auto & res : results) {
                nixcompat::forBuiltOutputs(
                    res, [&](const nix::StorePath & outPath) -> void { outputPaths.insert(outPath); });
            }
            for (const auto & outPath : outputPaths) {
                nixgrpc::encodePathInfo(*localStore, *localStore->queryPathInfo(outPath), done->add_outputs());
            }
            writer->Write(chunk);
            rpc.done({{"targets", std::to_string(targets.size())}, {"outputs", std::to_string(outputPaths.size())}});
            return grpc::Status::OK;
        });
    }

    auto FetchNars(
        grpc::ServerContext * context,
        const nix::remote::FetchNarsRequest * request,
        NarFrameWriter * writer) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto started = begin(*context, "FetchNars", nixgrpc::Role::readOnly);
            if (auto * denied = std::get_if<grpc::Status>(&started)) {
                return *denied;
            }
            auto & rpc = std::get<Rpc>(started);
            auto localStore = getStore();

            class TaggingWriter
            {
                NarFrameWriter * writer;
                uint32_t pathIndex = 0;

            public:
                explicit TaggingWriter(NarFrameWriter * writer)
                    : writer(writer)
                {
                }

                void setPathIndex(uint32_t index)
                {
                    pathIndex = index;
                }

                auto Write(nix::remote::NarFrame & frame) -> bool
                {
                    frame.set_path_index(pathIndex);
                    return writer->Write(frame);
                }
            };

            TaggingWriter tagged(writer);
            nixgrpc::ZstdWriterSink<TaggingWriter, nix::remote::NarFrame> sink(tagged);

            uint64_t narBytes = 0;
            nix::LambdaSink counting([&](std::string_view data) -> void {
                sink(data);
                narBytes += data.size();
            });

            for (int idx = 0; idx < request->paths_size(); ++idx) {
                tagged.setPathIndex(static_cast<uint32_t>(idx));
                localStore->narFromPath(nix::StorePath(request->paths(idx)), counting);
                sink.flush();
                nix::remote::NarFrame eofFrame;
                eofFrame.set_path_index(static_cast<uint32_t>(idx));
                eofFrame.set_eof(true);
                if (!writer->Write(eofFrame)) {
                    throw nix::Error("gRPC stream closed by peer");
                }
            }
            rpc.done({{"paths", std::to_string(request->paths_size())}, {"nar_bytes_out", std::to_string(narBytes)}});
            metrics.countNarBytes("out", rpc.caller.name, narBytes);
            return grpc::Status::OK;
        });
    }
};

} // namespace

auto main(int argc, char ** argv) -> int
try {
    // Pump threads write to a socket whose peer may already be gone; we want
    // EPIPE, not process death.
    // NOLINTBEGIN(misc-include-cleaner): darwin's <signal.h> forwards these.
    struct sigaction act{};
    act.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &act, nullptr);
    // Polled by the main loop so in-flight RPCs get the shutdown grace.
    act.sa_handler = [](int) -> void { stopSignal = 1; };
    for (int const sig : {SIGTERM, SIGINT}) {
        sigaction(sig, &act, nullptr);
    }
    // NOLINTEND(misc-include-cleaner)

    // Required before nix::openStore() in the native RPC handlers.
    nix::initLibStore();

    const std::span args(argv, static_cast<size_t>(argc));
    auto options = nixgrpc::parseOptions({args.begin(), args.end()});
    auto const listenFds = nixgrpc::systemdListenFds();
    if (listenFds.empty() && options.idleTimeout) {
        // Nobody would restart us on the next connection.
        throw nix::Error("--idle-timeout requires systemd socket activation");
    }

    nixgrpc::Metrics metrics(options.metricsListen);
    nixgrpc::IdleTracker idle;
    if (options.storeUri.empty()) {
        options.storeUri = "unix://" + options.socketPath;
    }
    NixRemoteService service(options.socketPath, options.storeUri, metrics, idle, options.logLevel, options.acl);

    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    auto creds = nixgrpc::makeServerCredentials(options);
    std::shared_ptr<grpc::experimental::ExternalConnectionAcceptor> acceptor;
    if (listenFds.empty()) {
        builder.AddListeningPort(options.listen, creds);
    } else {
        options.listen = "systemd";
        acceptor = builder.experimental().AddExternalConnectionAcceptor(
            grpc::ServerBuilder::experimental_type::ExternalConnectionType::FROM_FD, creds);
    }
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server) {
        throw nix::Error("failed to start gRPC server on '%s'", options.listen);
    }
    if (acceptor) {
        nixgrpc::acceptInto(listenFds, acceptor);
    }

    nixgrpc::logLine(
        nixgrpc::LogLevel::info,
        {{"event", "startup"}, {"listen", options.listen}, {"proxy_socket", options.socketPath}});
    nixgrpc::sdNotify("READY=1");

    auto const watchdog = nixgrpc::sdWatchdogInterval();
    std::chrono::nanoseconds const tick =
        watchdog.count() != 0 ? std::min<std::chrono::nanoseconds>(watchdog, std::chrono::seconds(1))
                              : std::chrono::seconds(1);
    while (stopSignal == 0 && (!options.idleTimeout || idle.idleFor() < *options.idleTimeout)) {
        if (watchdog.count() != 0) {
            nixgrpc::sdNotify("WATCHDOG=1");
        }
        std::this_thread::sleep_for(tick);
    }
    nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", stopSignal != 0 ? "signal_exit" : "idle_exit"}});
    nixgrpc::sdNotify("STOPPING=1");
    constexpr std::chrono::seconds shutdownGrace{5};
    server->Shutdown(std::chrono::system_clock::now() + shutdownGrace);
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
