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
#include <iterator>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <semaphore>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>
#include <sys/statvfs.h>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/outputs-spec.hh>
#include <nix/store/globals.hh>
#include <nix/store/worker-protocol-connection.hh>
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
#include <nix/util/environment-variables.hh>
#include <nix/util/strings.hh>
#include <nix/util/util.hh>

#include "acl.hh"
#include "build-log.hh"
#include "farm.hh"
#include "idle.hh"
#include "import-paths.hh"
#include "logfmt.hh"
#include "parse-int.hh"
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

namespace {

struct FarmConfig
{
    std::string niks3Url;
    std::string niks3TokenFile;
    nixgrpc::PushProcess::Argv pushArgv;
    unsigned maxJobs = 1;
    uint64_t minFree = 0;
    std::string storeDir = nix::getEnv("NIX_STORE_DIR").value_or("/nix/store");
};

struct Farm
{
    nixgrpc::Niks3 niks3;
    nixgrpc::PushProcess push;
    std::counting_semaphore<> slots;
    uint64_t minFree;
    std::string storeDir;
    // Low disk: new builds bounce with UNAVAILABLE and gRPC health says
    // NOT_SERVING so the balancer routes elsewhere.
    std::atomic<bool> healthy{true};

    explicit Farm(const FarmConfig & cfg)
        : niks3(cfg.niks3Url, nix::chomp(nix::readFile(cfg.niks3TokenFile)))
        , push(cfg.pushArgv)
        , slots(static_cast<std::ptrdiff_t>(cfg.maxJobs))
        , minFree(cfg.minFree)
        , storeDir(cfg.storeDir)
    {
    }

    void updateHealth(grpc::Server & server)
    {
        struct statvfs vfs{};
        if (minFree == 0 || statvfs(storeDir.c_str(), &vfs) != 0) {
            return;
        }
        bool const now = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize >= minFree;
        if (healthy.exchange(now) == now) {
            return;
        }
        server.GetHealthCheckService()->SetServingStatus(now);
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", now ? "healthy" : "unhealthy"}, {"reason", "min_free"}});
    }
};

class SlotGuard
{
    std::counting_semaphore<> & sem;

public:
    explicit SlotGuard(std::counting_semaphore<> & sem)
        : sem(sem)
    {
        sem.acquire();
    }

    SlotGuard(const SlotGuard &) = delete;
    SlotGuard(SlotGuard &&) = delete;
    auto operator=(const SlotGuard &) -> SlotGuard & = delete;
    auto operator=(SlotGuard &&) -> SlotGuard & = delete;

    ~SlotGuard()
    {
        sem.release();
    }
};

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    std::string socketPath;
    std::string storeUri;
    nixgrpc::Metrics & metrics;
    nixgrpc::IdleTracker & idle;
    nixgrpc::LogLevel logLevel;
    nixgrpc::Acl acl;
    std::optional<Farm> & farm;

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

    static auto secondsSince(std::chrono::steady_clock::time_point start) -> std::string
    {
        auto const elapsed = std::chrono::steady_clock::now() - start;
        return std::to_string(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count());
    }

    // Builds need write, except repair, which rewrites existing store paths.
    auto authorizeBuild(const std::optional<std::string> & cert, std::string_view method, uint32_t buildMode)
        -> grpc::Status
    {
        if (auto status = authorize(cert, method, nixgrpc::Role::write); !status.ok()) {
            return status;
        }
        if (buildMode == static_cast<uint32_t>(nix::bmRepair)) {
            return authorize(cert, std::string(method) + "(repair)", nixgrpc::Role::trusted);
        }
        return grpc::Status::OK;
    }

    auto authorize(const std::optional<std::string> & cert, std::string_view method, nixgrpc::Role minRole)
        -> grpc::Status
    {
        auto const commonName = cert.value_or("-");
        auto const role = acl.roleFor(cert);
        if (role && *role >= minRole) {
            return grpc::Status::OK;
        }
        nixgrpc::logLine(
            nixgrpc::LogLevel::info,
            {{"event", "denied"},
             {"method", std::string(method)},
             {"cn", commonName},
             {"role", role ? std::string(nixgrpc::roleName(*role)) : "none"}});
        if (role) {
            return {
                grpc::StatusCode::PERMISSION_DENIED,
                "role '" + std::string(nixgrpc::roleName(*role)) + "' may not call " + std::string(method)};
        }
        if (!cert) {
            return {grpc::StatusCode::UNAUTHENTICATED, "server requires a TLS client certificate"};
        }
        return {grpc::StatusCode::PERMISSION_DENIED, "no access rule matches certificate CN '" + commonName + "'"};
    }

public:
    NixRemoteService(
        std::string socketPath,
        std::string storeUri,
        nixgrpc::Metrics & metrics,
        nixgrpc::IdleTracker & idle,
        nixgrpc::LogLevel logLevel,
        nixgrpc::Acl acl,
        std::optional<Farm> & farm)
        : socketPath(std::move(socketPath))
        , storeUri(std::move(storeUri))
        , metrics(metrics)
        , idle(idle)
        , logLevel(logLevel)
        , acl(std::move(acl))
        , farm(farm)
    {
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
        if (farm) {
            return {grpc::StatusCode::UNIMPLEMENTED, "farm endpoint: pass --eval-store auto and build via BuildDerivation"};
        }
        nixgrpc::IdleTracker::Guard const active(idle);
        auto const cert = nixgrpc::clientCommonName(*context);
        auto const commonName = cert.value_or("-");
        // The opaque worker protocol cannot be inspected here.
        if (auto status = authorize(cert, "Connect", nixgrpc::Role::trusted); !status.ok()) {
            return status;
        }
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
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "QueryValidPaths", nixgrpc::Role::readOnly); !status.ok()) {
                return status;
            }
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
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "AddMultipleToStore", nixgrpc::Role::write); !status.ok()) {
                return status;
            }
            auto const peer = context->peer();
            auto const start = std::chrono::steady_clock::now();
            auto localStore = openScopedStore();

            nix::remote::AddMultipleChunk first;
            if (!reader->Read(&first)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "empty AddMultipleToStore stream"};
            }
            auto repair = first.repair() ? nix::Repair : nix::NoRepair;
            // The nix-daemon downgrades this to CheckSigs if we are not a
            // trusted user, same as for the tunnelled protocol.
            auto checkSigs = first.check_sigs() ? nix::CheckSigs : nix::NoCheckSigs;
            if (acl.roleFor(cert) == nixgrpc::Role::write) {
                // write may only import signed paths, no matter how trusted
                // the proxy's own uid is.
                repair = nix::NoRepair;
                checkSigs = nix::CheckSigs;
            }

            nixgrpc::ZstdReaderSource<AddMultipleReader, nix::remote::AddMultipleChunk> source(
                *reader, std::move(*first.mutable_data()));

            std::vector<std::string> imported;
            auto stats = nixgrpc::importPaths(
                *localStore, source, [&](const nix::ValidPathInfo & info, nix::Source & nar) -> void {
                    localStore->addToStore(info, nar, repair, checkSigs);
                    imported.push_back(localStore->printStorePath(info.path));
                });
            if (farm && !imported.empty()) {
                try {
                    farm->push.queue(imported);
                } catch (nix::Error & err) {
                    nixgrpc::logLine(
                        nixgrpc::LogLevel::info, {{"event", "push_queue_failed"}, {"error", std::string(err.what())}});
                }
            }
            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "AddMultipleToStore"},
                 {"cn", commonName},
                 {"peer", peer},
                 {"duration_s", secondsSince(start)},
                 {"paths", std::to_string(stats.paths)},
                 {"nar_bytes_in", std::to_string(stats.narBytes)}});
            metrics.countRpc("AddMultipleToStore", commonName);
            metrics.countNarBytes("in", commonName, stats.narBytes);
            return grpc::Status::OK;
        });
    }

    auto QueryPathInfos(
        grpc::ServerContext * context,
        const nix::remote::QueryPathInfosRequest * request,
        nix::remote::QueryPathInfosReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "QueryPathInfos", nixgrpc::Role::readOnly); !status.ok()) {
                return status;
            }
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
                nixgrpc::encodePathInfo(*localStore, *info, reply->add_infos());
            }
            return grpc::Status::OK;
        });
    }

    // Relays the raw worker-protocol stderr stream of one build to the
    // client, which replays it through its own protocol code. Output path
    // infos ride on the final message.
    struct Backend
    {
        struct Conn : nix::WorkerProto::BasicClientConnection
        {
            void closeWrite() override {}
        };
        nix::AutoCloseFD sock;
        Conn conn;
        nix::WorkerProto::ClientHandshakeInfo info;
    };

    auto connectBackend(nix::Store & store) -> std::unique_ptr<Backend>
    {
        auto backend = std::make_unique<Backend>();
        backend->sock = nix::connect(std::filesystem::path{socketPath});
        backend->conn.to = nix::FdSink(backend->sock.get());
        backend->conn.from = nix::FdSource(backend->sock.get());
        backend->conn.protoVersion = nixcompat::handshakeCompat(backend->conn, nixcompat::buildProtocolVersion());
        backend->info = backend->conn.postHandshake(store);
        return backend;
    }

    auto QueryMissing(
        grpc::ServerContext * context,
        const nix::remote::QueryMissingRequest * request,
        nix::remote::QueryMissingReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "QueryMissing", nixgrpc::Role::readOnly); !status.ok()) {
                return status;
            }
            logDebug(
                {{"event", "rpc"},
                 {"method", "QueryMissing"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"targets", std::to_string(request->targets_size())}});
            metrics.countRpc("QueryMissing", commonName);
            auto localStore = getStore();
            std::vector<nix::DerivedPath> targets;
            targets.reserve(request->targets_size());
            for (const auto & target : request->targets()) {
                targets.push_back(nix::DerivedPath::parse(*localStore, target));
            }
            auto missing = localStore->queryMissing(targets);
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
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "StoreInfo", nixgrpc::Role::readOnly); !status.ok()) {
                return status;
            }
            logDebug({{"event", "rpc"}, {"method", "StoreInfo"}, {"cn", commonName}, {"peer", context->peer()}});
            metrics.countRpc("StoreInfo", commonName);
            auto backend = connectBackend(*getStore());
            if (backend->info.remoteTrustsUs) {
                reply->set_trusted(*backend->info.remoteTrustsUs == nix::Trusted);
            }
            return grpc::Status::OK;
        });
    }

    static void appendOutputInfo(nix::Store & store, nix::remote::PathInfo * out, const nix::StorePath & outPath)
    {
        nixgrpc::encodePathInfo(store, *store.queryPathInfo(outPath), out);
    }

    using LogSink = std::function<void(std::string)>;

    // With `drv`, sends the inline BasicDerivation (needs a trusted user
    // for input-addressed outputs). Without, builds the stored .drv:
    // nix-daemon recomputes output paths from the closure, so a forged
    // inline drv cannot claim foreign paths. Farm mode uses the latter.
    // A farm worker builds itself: no build hook (its failures would look
    // transient and it defeats claim accounting), one job per connection.
    static void buildLocally(Backend::Conn & conn)
    {
        constexpr uint64_t off = 0;
        constexpr uint64_t yes = 1;
        conn.to << nix::WorkerProto::Op::SetOptions << off /* keepFailed */ << off /* keepGoing */
                << off /* tryFallback */ << off /* verbosity */ << yes /* maxBuildJobs */
                << off /* maxSilentTime */ << yes << off /* verbosity */ << off << off << off /* buildCores */
                << yes /* useSubstitutes */;
        std::map<std::string, std::string> const overrides{{"builders", ""}};
        conn.to << overrides.size();
        for (const auto & [name, value] : overrides) {
            conn.to << name << value;
        }
        if (auto exc = conn.processStderrReturn()) {
            std::rethrow_exception(exc);
        }
    }

    
    auto runBuild(
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation * drv,
        nix::BuildMode mode,
        const LogSink & sendLogLine) -> nix::BuildResult
    {
        auto backend = connectBackend(localStore);
        auto & conn = backend->conn;
        if (nixcompat::protocolWire(conn.protoVersion) != nixcompat::kBuildProtocolWire) {
            throw nix::Error("backend daemon is too old");
        }
        if (drv == nullptr) {
            buildLocally(conn);
        }
        nix::WorkerProto::ReadConn const from{.from = conn.from, .version = nixcompat::buildProtocolVersion()};
        nixgrpc::relayBuildLog(conn.from, sendLogLine);
        if (drv != nullptr) {
            bool daemonException = false;
            conn.putBuildDerivationRequest(localStore, &daemonException, drvPath, *drv, mode);
            conn.to.flush();
            nixgrpc::relayBuildLog(conn.from, sendLogLine);
            return nix::WorkerProto::Serialise<nix::BuildResult>::read(localStore, from);
        }
        std::vector<nix::DerivedPath> const targets{nix::DerivedPath::Built{
            .drvPath = nix::makeConstantStorePathRef(drvPath), .outputs = nix::OutputsSpec::All{}}};
        conn.to << nix::WorkerProto::Op::BuildPathsWithResults;
        nix::WorkerProto::write(
            localStore, nix::WorkerProto::WriteConn{.to = conn.to, .version = conn.protoVersion}, targets);
        conn.to << static_cast<uint32_t>(mode);
        conn.to.flush();
        nixgrpc::relayBuildLog(conn.from, sendLogLine);
        auto results = nix::WorkerProto::Serialise<std::vector<nix::KeyedBuildResult>>::read(localStore, from);
        if (results.size() != 1) {
            throw nix::Error("nix-daemon returned %d results for one derivation", results.size());
        }
        return std::move(results.front());
    }

    static void encodeResult(nix::Store & localStore, nix::BuildResult & res, nix::remote::BuildDerivationDone * done)
    {
        nix::StringSink sink;
        nix::WorkerProto::Serialise<nix::BuildResult>::write(
            localStore, nix::WorkerProto::WriteConn{.to = sink, .version = nixcompat::buildProtocolVersion()}, res);
        *done->mutable_result() = std::move(sink.s);
        nixcompat::forBuiltOutputs(res, [&](const nix::StorePath & outPath) -> void {
            if (localStore.isValidPath(outPath)) {
                appendOutputInfo(localStore, done->add_outputs(), outPath);
            }
        });
    }

    static auto narinfoKeys(const auto & paths) -> std::vector<std::string>
    {
        std::vector<std::string> keys;
        keys.reserve(static_cast<size_t>(std::ranges::distance(paths)));
        for (const nix::StorePath & path : paths) {
            keys.push_back(std::string(path.hashPart()) + ".narinfo");
        }
        return keys;
    }

    static auto staticOutputs(nix::Store & store, const nix::BasicDerivation & drv)
        -> std::map<std::string, nix::StorePath>
    {
        std::map<std::string, nix::StorePath> res;
        for (const auto & [name, output] : drv.outputs) {
            if (auto path = output.path(store, drv.name, name)) {
                res.emplace(name, *path);
            }
        }
        return res;
    }

    // gRPC errors mean "retry this RPC". Build
    // outcomes travel as BuildResult.
    auto farmBuild(
        Farm & frm,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        const LogSink & log,
        nix::BuildResult & res) -> grpc::Status
    {
        auto outPaths = staticOutputs(localStore, drv);
        if (outPaths.size() != drv.outputs.size()) {
            return {grpc::StatusCode::UNIMPLEMENTED, "farm builds need statically known output paths"};
        }
        if (!frm.healthy) {
            return {grpc::StatusCode::UNAVAILABLE, "worker low on disk space"};
        }
        std::unique_ptr<SlotGuard> slot = std::make_unique<SlotGuard>(frm.slots);
        auto claim = frm.niks3.claim(narinfoKeys(outPaths | std::views::values), narinfoKeys(nixcompat::drvInputs(drv)));
        if (claim->first() == nixgrpc::Claim::Status::wait) {
            slot.reset();
            log("waiting for another worker building " + std::string(drvPath.to_string()));
        }
        switch (claim->await()) {
        case nixgrpc::Claim::Status::built:
            res = nixcompat::alreadyValid(drvPath, std::move(outPaths));
            return grpc::Status::OK;
        case nixgrpc::Claim::Status::failed:
            res = nixcompat::failed(
                nixcompat::FailureStatus::PermanentFailure, "failed on another worker: " + claim->kind());
            return grpc::Status::OK;
        default:
            break;
        }
        if (!slot) {
            slot = std::make_unique<SlotGuard>(frm.slots);
        }

        if (!localStore.isValidPath(drvPath)) {
            return {grpc::StatusCode::NOT_FOUND, "missing: " + localStore.printStorePath(drvPath)};
        }
        try {
            for (const auto & input : nixcompat::drvInputs(drv)) {
                localStore.addTempRoot(input);
                nixcompat::ensurePath(localStore, input);
            }
        } catch (nix::Error & err) {
            return {grpc::StatusCode::FAILED_PRECONDITION, std::string("input not substitutable: ") + err.what()};
        }

        res = runBuild(localStore, drvPath, nullptr, nix::bmNormal, log);

        if (claim->lost()) {
            return {grpc::StatusCode::UNAVAILABLE, "lost niks3 claim during build"};
        }
        if (!nixcompat::succeeded(res)) {
            // The client must see the build log and result even if niks3 is
            // down. Waiters then learn it by expiry instead of at once.
            try {
                claim->fail(nixcompat::deterministicFailureKind(res));
            } catch (nix::Error & err) {
                log(std::string("reporting failure to niks3: ") + err.what());
            }
            // nix reports ENOSPC and the like as TransientFailure. Not the
            // build's fault, so let the client retry on another worker.
            if (nixcompat::failureStatus(res) == nixcompat::FailureStatus::TransientFailure) {
                return {grpc::StatusCode::UNAVAILABLE, nixcompat::buildFailureMsg(res).value_or("transient failure")};
            }
            return grpc::Status::OK;
        }
        std::vector<std::string> built;
        built.reserve(outPaths.size());
        for (const auto & [name, path] : outPaths) {
            built.push_back(localStore.printStorePath(path));
        }
        try {
            frm.push.pushWait(built, claim->token());
        } catch (nixgrpc::StaleClaim &) {
            log("another worker published " + std::string(drvPath.to_string()) + " first");
        } catch (nix::Error & err) {
            return {grpc::StatusCode::UNAVAILABLE, std::string("publish failed: ") + err.what()};
        }
        return grpc::Status::OK;
    }

    auto BuildDerivation(
        grpc::ServerContext * context,
        const nix::remote::BuildDerivationRequest * request,
        BuildWriter * writer) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorizeBuild(cert, "BuildDerivation", request->build_mode());
                !status.ok()) {
                return status;
            }
            auto const start = std::chrono::steady_clock::now();
            metrics.countRpc("BuildDerivation", commonName);

            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            auto localStore = getStore();
            auto sendLogLine = [&](std::string line) -> void {
                nix::remote::BuildDerivationChunk chunk;
                *chunk.mutable_log_line() = std::move(line);
                writer->Write(chunk);
            };

            nix::StorePath const drvPath(request->drv_path());
            nix::StringSource drvSource(request->drv());
            nix::BasicDerivation drv;
            nixcompat::readDrv(drvSource, *localStore, drv, nix::Derivation::nameFromPath(drvPath));
            auto const mode = static_cast<nix::BuildMode>(request->build_mode());

            nix::BuildResult res;
            if (farm) {
                if (mode != nix::bmNormal) {
                    return {grpc::StatusCode::INVALID_ARGUMENT, "farm endpoint only does normal builds"};
                }
                if (auto status = farmBuild(*farm, *localStore, drvPath, drv, sendLogLine, res); !status.ok()) {
                    return status;
                }
            } else {
                res = runBuild(*localStore, drvPath, &drv, mode, sendLogLine);
            }

            nix::remote::BuildDerivationChunk chunk;
            encodeResult(*localStore, res, chunk.mutable_done());
            writer->Write(chunk);

            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "BuildDerivation"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"duration_s", secondsSince(start)},
                 {"drv", std::string(drvPath.to_string())},
                 {"outputs", std::to_string(chunk.done().outputs_size())}});
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
            if (farm) {
                return {grpc::StatusCode::UNIMPLEMENTED, "farm endpoint: build per derivation"};
            }
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorizeBuild(cert, "BuildPaths", request->build_mode());
                !status.ok()) {
                return status;
            }
            auto const start = std::chrono::steady_clock::now();
            metrics.countRpc("BuildPaths", commonName);

            auto const protocol = nixcompat::buildProtocolVersion();
            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            auto localStore = getStore();

            std::vector<nix::DerivedPath> targets;
            targets.reserve(request->targets_size());
            for (const auto & target : request->targets()) {
                targets.push_back(nix::DerivedPath::parse(*localStore, target));
            }

            auto backend = connectBackend(*localStore);
            auto & conn = backend->conn;
            if (nixcompat::protocolWire(conn.protoVersion) != nixcompat::kBuildProtocolWire) {
                return {grpc::StatusCode::FAILED_PRECONDITION, "backend daemon is too old"};
            }
            auto sendLogLine = [&](std::string line) -> void {
                nix::remote::BuildPathsChunk chunk;
                *chunk.mutable_log_line() = std::move(line);
                writer->Write(chunk);
            };
            // The daemon opens every connection with a stderr work block.
            nixgrpc::relayBuildLog(conn.from, sendLogLine);

            conn.to << nix::WorkerProto::Op::BuildPathsWithResults;
            nix::WorkerProto::write(
                *localStore,
                nix::WorkerProto::WriteConn{.to = conn.to, .version = conn.protoVersion},
                targets);
            conn.to << request->build_mode();
            conn.to.flush();

            nixgrpc::relayBuildLog(conn.from, sendLogLine);

            auto results = nix::WorkerProto::Serialise<std::vector<nix::KeyedBuildResult>>::read(
                *localStore, nix::WorkerProto::ReadConn{.from = conn.from, .version = protocol});

            nix::remote::BuildPathsChunk chunk;
            auto * done = chunk.mutable_done();
            {
                nix::StringSink sink;
                nix::WorkerProto::Serialise<std::vector<nix::KeyedBuildResult>>::write(
                    *localStore,
                    nix::WorkerProto::WriteConn{.to = sink, .version = protocol},
                    results);
                *done->mutable_results() = std::move(sink.s);
            }
            nix::StorePathSet outputPaths;
            for (auto & res : results) {
                nixcompat::forBuiltOutputs(
                    res, [&](const nix::StorePath & outPath) -> void { outputPaths.insert(outPath); });
            }
            for (const auto & outPath : outputPaths) {
                appendOutputInfo(*localStore, done->add_outputs(), outPath);
            }
            writer->Write(chunk);

            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "BuildPaths"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"duration_s", secondsSince(start)},
                 {"targets", std::to_string(targets.size())},
                 {"outputs", std::to_string(outputPaths.size())}});
            return grpc::Status::OK;
        });
    }

    auto FetchNars(
        grpc::ServerContext * context,
        const nix::remote::FetchNarsRequest * request,
        NarFrameWriter * writer) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "FetchNars", nixgrpc::Role::readOnly); !status.ok()) {
                return status;
            }
            auto const start = std::chrono::steady_clock::now();
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
            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "FetchNars"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"duration_s", secondsSince(start)},
                 {"paths", std::to_string(request->paths_size())},
                 {"nar_bytes_out", std::to_string(narBytes)}});
            metrics.countRpc("FetchNars", commonName);
            metrics.countNarBytes("out", commonName, narBytes);
            return grpc::Status::OK;
        });
    }
};

struct Options
{
    std::string listen = "0.0.0.0:50051";
    std::optional<std::chrono::seconds> idleTimeout;
    std::string socketPath = "/nix/var/nix/daemon-socket/socket";
    // Store URI for the native bulk RPCs. Defaults to the proxy socket.
    std::string storeUri;
    std::string tlsCert;
    std::string tlsKey;
    std::string clientCA;
    std::string metricsListen;
    nixgrpc::LogLevel logLevel = nixgrpc::LogLevel::info;
    nixgrpc::Acl acl;
    std::optional<FarmConfig> farm;
};

auto parseLogLevel(std::string_view value) -> nixgrpc::LogLevel
{
    if (value == "debug") {
        return nixgrpc::LogLevel::debug;
    }
    if (value != "info") {
        throw nix::Error("--log-level must be 'info' or 'debug'");
    }
    return nixgrpc::LogLevel::info;
}

auto farm(Options & options) -> FarmConfig &
{
    if (!options.farm) {
        options.farm.emplace();
    }
    return *options.farm;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): flat flag list.
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
        } else if (arg == "--proxy-store") {
            options.storeUri = next();
        } else if (arg == "--tls-cert") {
            options.tlsCert = next();
        } else if (arg == "--tls-key") {
            options.tlsKey = next();
        } else if (arg == "--client-ca") {
            options.clientCA = next();
        } else if (arg == "--allow") {
            options.acl.addRule(next());
        } else if (arg == "--allow-anonymous") {
            options.acl.allowAnonymous(nixgrpc::parseRole(next()));
        } else if (arg == "--metrics-listen") {
            options.metricsListen = next();
        } else if (arg == "--idle-timeout") {
            auto secs = nixgrpc::parseInt<unsigned>(next());
            if (!secs) {
                throw nix::Error("--idle-timeout expects a non-negative integer");
            }
            options.idleTimeout = std::chrono::seconds(*secs);
        } else if (arg == "--log-level") {
            options.logLevel = parseLogLevel(next());
        } else if (arg == "--niks3") {
            farm(options).niks3Url = next();
        } else if (arg == "--niks3-token-file") {
            farm(options).niks3TokenFile = next();
        } else if (arg == "--niks3-push") {
            // The program plus extra flags, e.g. "niks3 push --max-concurrent-uploads 8".
            auto words = nix::shellSplitString(next());
            farm(options).pushArgv = {words.begin(), words.end()};
        } else if (arg == "--max-jobs") {
            auto jobs = nixgrpc::parseInt<unsigned>(next());
            if (!jobs || *jobs == 0) {
                throw nix::Error("--max-jobs expects a positive integer");
            }
            farm(options).maxJobs = *jobs;
        } else if (arg == "--min-free") {
            farm(options).minFree = nix::string2IntWithUnitPrefix<uint64_t>(next());
        } else {
            throw nix::Error("unknown flag '%s'", arg);
        }
    }
    if (options.farm) {
        if (options.farm->niks3Url.empty() || options.farm->niks3TokenFile.empty()) {
            throw nix::Error("farm mode needs --niks3 and --niks3-token-file");
        }
        auto & argv = options.farm->pushArgv;
        if (argv.empty()) {
            argv = {"niks3", "push"};
        }
        argv.insert(
            argv.end(),
            {"--stdin", "--server-url", options.farm->niks3Url, "--auth-token-path", options.farm->niks3TokenFile});
    }
    if ((options.acl.active() || options.acl.anonymousRole()) && options.clientCA.empty()) {
        // Without mTLS every client's CN is "-".
        throw nix::Error("--allow/--allow-anonymous requires --client-ca");
    }
    if (!options.clientCA.empty() && !options.acl.anonymousRole()) {
        options.acl.requireCertificate();
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
    // Cert-less clients pass the handshake and are denied by the ACL with a
    // readable UNAUTHENTICATED instead of an opaque "Socket closed".
    auto const clientCertRequest = options.clientCA.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
                                                             : GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;
    grpc::SslServerCredentialsOptions ssl(clientCertRequest);
    ssl.pem_key_cert_pairs.push_back(
        {.private_key = nix::readFile(options.tlsKey), .cert_chain = nix::readFile(options.tlsCert)});
    if (!options.clientCA.empty()) {
        ssl.pem_root_certs = nix::readFile(options.clientCA);
    }
    return grpc::SslServerCredentials(ssl);
}

} // namespace

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler.
volatile std::sig_atomic_t stopSignal = 0;
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
    auto options = parseOptions({args.begin(), args.end()});
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
    std::optional<Farm> farm;
    if (options.farm) {
        farm.emplace(*options.farm);
        nixgrpc::logLine(
            nixgrpc::LogLevel::info,
            {{"event", "farm_mode"},
             {"niks3", options.farm->niks3Url},
             {"max_jobs", std::to_string(options.farm->maxJobs)}});
    }
    NixRemoteService service(
        options.socketPath, options.storeUri, metrics, idle, options.logLevel, options.acl, farm);

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    auto creds = makeServerCredentials(options);
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
        if (farm) {
            farm->updateHealth(*server);
        }
        std::this_thread::sleep_for(tick);
    }
    nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", stopSignal != 0 ? "signal_exit" : "idle_exit"}});
    nixgrpc::sdNotify("STOPPING=1");
    if (auto * health = server->GetHealthCheckService()) {
        health->SetServingStatus(false);
    }
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
