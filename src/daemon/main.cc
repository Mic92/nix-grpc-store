// gRPC server that proxies tunnelled Nix worker-protocol connections straight
// to a nix-daemon unix socket. The real nix-daemon handles trust, forking and
// interrupt-on-hangup; this process only moves bytes (and optionally zstd).
//
// QueryValidPaths, QueryPathInfos, AddMultipleToStore and NarsFromPaths are
// handled natively (via a Store opened on the same socket) so `nix copy`
// avoids the tunnel's per-batch zstd flushes and per-path round trips.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <signal.h> // NOLINT(modernize-deprecated-headers): sigaction is POSIX, not in <csignal>
#include <cstddef>
#include <initializer_list>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <future>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <iterator>
#include <set>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
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
#include <nix/util/types.hh>
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
#include <nix/util/strings.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "build-log.hh"
#include "acl.hh"
#include "auth.hh"
#include "backend.hh"
#include "cache.hh"
#include "coordinator.hh"
#include "options.hh"
#include "push.hh"
#include "idle.hh"
#include "import-paths.hh"
#include "log-spill.hh"
#include "keepalive.hh"
#include "logfmt.hh"
#include "oidc.hh"
#include "reverse-session.hh"
#include "reverse-gateway.hh"
#include "reverse-relay.hh"
#include "reverse-request.hh"
#include "reverse-upload-relay.hh"
#include "path-info-wire.hh"
#include "metrics.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"
#include "socket-activation.hh"
#include "session-roots.hh"
#include "upload-claims.hh"
#include "parse-int.hh"
#include "sandbox.hh"
#include "upload-spool.hh"

using GrpcStream = grpc::ServerReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;
using AddMultipleReader = grpc::ServerReader<nix::remote::AddMultipleChunk>;
using NarFrameWriter = grpc::ServerWriter<nix::remote::NarFrame>;
using BuildWriter = grpc::ServerWriter<nix::remote::BuildDerivationChunk>;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler.
volatile std::sig_atomic_t nixgrpc::stopSignal = 0;

namespace {
using nixgrpc::stopSignal;

static_assert(static_cast<int>(nixgrpc::SpillKind::line) == static_cast<int>(nixgrpc::BuildEvent::Kind::line));
static_assert(static_cast<int>(nixgrpc::SpillKind::phase) == static_cast<int>(nixgrpc::BuildEvent::Kind::phase));

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    std::string storeUri;
    std::string workerName;
    nixgrpc::Metrics & metrics;
    nixgrpc::IdleTracker & idle;
    nixgrpc::LogLevel logLevel;
    nixgrpc::Auth & auth;
    nixgrpc::Backends backends;
    nixgrpc::Coordinator & coord;
    std::unique_ptr<nixgrpc::ReverseGateway> reverseGateway;
    std::unique_ptr<nixgrpc::ReverseRelay> reverseRelay;
    std::unique_ptr<nixgrpc::ReverseUploadRelay> reverseUploadRelay;

    std::mutex storeMutex;
    std::shared_ptr<nix::Store> store;
    nixgrpc::UploadClaims uploads;
    nixgrpc::SessionRoots sessions{[this] -> nix::ref<nix::Store> { return openScopedStore(); }};
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
    // Fixture-only pause after the importer consumes bytes, before it checks
    // whether the assignment survived that read. Not built into the daemon.
    std::function<void(const std::string &, size_t)> reverseUploadReadHook;
    // After importPaths spools the entire NAR, before the existing apply gate.
    std::function<void(const std::string &)> reverseUploadSpooledHook;
    // Before publishing a built output; throwing simulates an unreachable cache.
    std::function<void(const std::string &)> publishHook;
#endif

    // Without the path info cache a path deleted behind our back (gc) is
    // invalid here too, instead of failing later when its NAR is read.
    [[nodiscard]] auto openLocalStore(nix::StringMap params = {}) const -> nix::ref<nix::Store>
    {
        params.emplace("path-info-cache-size", "0");
        return nix::openStore(storeUri, params);
    }

    // Aged-out connections release their temp roots. Not sooner: a fresh
    // nix-daemon probes every substituter again.
    static constexpr auto sharedConnections = "16";
    static constexpr auto sharedConnectionAge = "600";

    // The Store connects lazily and pools connections, but opening it can
    // still throw (e.g. daemon socket missing), so defer to first use.
    auto getStore() -> nix::ref<nix::Store>
    {
        std::scoped_lock const lock(storeMutex);
        if (!store) {
            store = openLocalStore({{"max-connections", sharedConnections}, {"max-connection-age", sharedConnectionAge}})
                        .get_ptr();
        }
        return nix::ref<nix::Store>(store);
    }

    // A restarted nix-daemon leaves the pool full of dead connections that
    // only fail on the next write. Drop them all so the retry gets fresh ones.
    // A failed reconnect while the daemon is down is latched by the store and
    // reported as "previously failed", so that drops the store too.
    static auto backendGone(const std::exception & err) -> bool
    {
        const auto * sys = dynamic_cast<const nix::SysError *>(&err);
        return dynamic_cast<const nix::EndOfFile *>(&err) != nullptr
               || (sys != nullptr
                   && (sys->errNo == EPIPE || sys->errNo == ECONNRESET || sys->errNo == ECONNREFUSED
                       || sys->errNo == ENOENT))
               || std::string_view(err.what()).contains("previously failed");
    }

    auto localDaemonGone(const std::exception & err) -> bool
    {
        bool const gone = backendGone(err);
        if (gone) {
            std::scoped_lock const lock(storeMutex);
            store.reset();
        }
        return gone;
    }

    // For writes (temp roots end with the RPC) and anything long.
    auto openScopedStore() -> nix::ref<nix::Store>
    {
        return openLocalStore();
    }

    // Retryable on another worker or later: guarded() maps it to UNAVAILABLE.
    struct Unavailable : nix::Error
    {
        using nix::Error::Error;
    };

    // Publishing makes our paths everyone's. Without it the client may not rely on them.
    void publish(nix::Store & localStore, const std::vector<std::string> & paths, const nixgrpc::Cancelled & cancelled)
    {
        try {
            coord.cache.publish(localStore, paths, cancelled);
        } catch (nixgrpc::CancelledWait &) {
            throw;
        } catch (nix::Error & err) {
            throw Unavailable("publish failed: %s", err.what());
        }
    }

public:
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
    void setReverseUploadReadHook(std::function<void(const std::string &, size_t)> hook)
    {
        reverseUploadReadHook = std::move(hook);
    }
    void setReverseUploadSpooledHook(std::function<void(const std::string &)> hook)
    {
        reverseUploadSpooledHook = std::move(hook);
    }
    void setPublishHook(std::function<void(const std::string &)> hook)
    {
        publishHook = std::move(hook);
    }
#endif
    // begin() throws this for callers the ACL refuses.
    struct Denied : std::exception
    {
        grpc::Status status;
        explicit Denied(grpc::Status status_)
            : status(std::move(status_))
        {
        }
    };

private:

    // gRPC aborts the process if a handler lets an exception escape.
    template<typename F>
    auto guarded(F && func) -> grpc::Status
    {
        nixgrpc::IdleTracker::Guard const active(idle);
        try {
            return std::forward<F>(func)();
        } catch (Denied & denied) {
            return denied.status;
        } catch (nixgrpc::CancelledWait & err) {
            return {grpc::StatusCode::UNAVAILABLE, err.what()};
        } catch (Unavailable & err) {
            return {grpc::StatusCode::UNAVAILABLE, err.what()};
        } catch (std::exception & err) {
            if (stopSignal != 0) {
                return {grpc::StatusCode::UNAVAILABLE, std::string("worker shutting down: ") + err.what()};
            }
            if (localDaemonGone(err)) {
                return {grpc::StatusCode::UNAVAILABLE, std::string("local nix-daemon connection lost: ") + err.what()};
            }
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
        -> Rpc
    {
        auto caller = auth.identify(context);
        if (auto const status = nixgrpc::Auth::authorize(caller, method, minRole, buildMode); !status.ok()) {
            throw Denied{status};
        }
        metrics.countRpc(std::string(method), caller.name);
        logDebug({{"event", "rpc_start"}, {"method", std::string(method)}, {"cn", caller.name}, {"peer", context.peer()}});
        return Rpc{.context = &context, .method = method, .caller = std::move(caller)};
    }

    // Writers only: a session pins disk until it expires.
    void holdForSession(const Rpc & rpc, const std::vector<nix::StorePath> & paths)
    {
        if (rpc.caller.role < nixgrpc::Role::write) {
            return;
        }
        auto const & meta = rpc.context->client_metadata();
        if (auto const found = meta.find("x-nix-session"); found != meta.end()) {
            sessions.hold(std::string_view(found->second.data(), found->second.size()), paths);
        }
    }

public:
    void tick()
    {
        sessions.expire();
    }

    NixRemoteService(
        std::string socketPath,
        std::string storeUri,
        std::string workerName,
        nixgrpc::Metrics & metrics,
        nixgrpc::IdleTracker & idle,
        nixgrpc::LogLevel logLevel,
        nixgrpc::Auth & auth,
        nixgrpc::Coordinator & coord)
        : storeUri(std::move(storeUri))
        , workerName(std::move(workerName))
        , metrics(metrics)
        , idle(idle)
        , logLevel(logLevel)
        , auth(auth)
        , backends{.socketPath = std::move(socketPath)}
        , coord(coord)
    {
        if (coord.dispatcher) {
            reverseGateway = std::make_unique<nixgrpc::ReverseGateway>(
                auth, coord.reverseRegistry,
                [&coord](std::string_view pod, std::string_view drv, uint64_t assignment) {
                    return coord.dispatcher->podOwnsAssignment(pod, drv, assignment);
                });
            reverseRelay = std::make_unique<nixgrpc::ReverseRelay>(*reverseGateway, coord.reverseRegistry);
            reverseUploadRelay = std::make_unique<nixgrpc::ReverseUploadRelay>(*reverseGateway, coord.reverseRegistry);
        }
    }

    // Called only by the authenticated, pod-bound reverse registration after
    // its capability and complete request were checked. Never re-identify a
    // loopback connection as the original caller.
    auto reverseBuild(const nixgrpc::ReverseRequest::Ready & call,
                       const nixgrpc::Coordinator::ReverseEmit & emit,
                       const std::function<bool()> & cancelled,
                       std::function<void()> & afterFinish) -> grpc::Status
    {
        return guarded([&] -> grpc::Status {
            nix::remote::BuildDerivationRequest request;
            if (!request.ParseFromString(call.payload) || !nixgrpc::ReverseRequest::matchesBuild(call, request)
                || request.protocol() != nixcompat::kBuildProtocolWire ||
                request.build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "invalid delegated build request"};
            }
            if (auto status = nixgrpc::Auth::authorize(call.delegated.caller, "BuildDerivation",
                    nixgrpc::Role::write, request.build_mode()); !status.ok()) return status;
            // As on the direct path: a stopping worker is UNAVAILABLE, which the
            // client hands back to the scheduler. CANCELLED would read as the
            // client's own cancel and fail the build.
            if (stopSignal != 0) return {grpc::StatusCode::UNAVAILABLE, "worker draining"};
            if (cancelled()) return {grpc::StatusCode::CANCELLED, "reverse build cancelled"};
            if (!coord.builder) return {grpc::StatusCode::FAILED_PRECONDITION, "not a builder"};
            metrics.countRpc("BuildDerivation", call.delegated.caller.name);
            auto const localStore = getStore();
            nix::StorePath drvPath(request.drv_path());
            nix::StringSource drvSource(request.drv());
            nix::BasicDerivation drv;
            nixcompat::readDrv(drvSource, *localStore, drv, nix::Derivation::nameFromPath(drvPath));
            auto const sendLog = [&](nixgrpc::BuildEvent event) {
                if (!emit(nixgrpc::toChunk<nix::remote::BuildDerivationChunk>(std::move(event))))
                    throw nixgrpc::CancelledWait("reverse build response closed");
            };
            nixgrpc::Metrics::Inputs inputs;
            nix::remote::BuildDerivationChunk chunk;
            // The gateway acknowledges only a delivered result. Without one
            // (publish failure, cancel, timeout) no acknowledgement comes, so
            // the deferred Done must go out now or the scheduler keeps this
            // slot busy until the session drops.
            auto const retireUnacknowledged = [&] {
                if (afterFinish) std::exchange(afterFinish, nullptr)();
            };
            grpc::Status status;
            try {
                status = build(cancelled, *localStore, drvPath, drv,
                    static_cast<nix::BuildMode>(request.build_mode()),
                    {.buildTimeout = request.build_timeout(), .maxSilentTime = request.max_silent_time()},
                    request.assign_id(), sendLog, inputs, *chunk.mutable_done(), &afterFinish);
            } catch (...) {
                retireUnacknowledged();
                throw;
            }
            if (status.ok() && !emit(chunk))
                status = {grpc::StatusCode::CANCELLED, "reverse response closed"};
            if (!status.ok()) retireUnacknowledged();
            return status;
        });
    }

    auto reverseUpload(const nixgrpc::ReverseRequest::Upload & call,
                       const std::function<bool()> & cancelled) -> grpc::Status
    {
        return guarded([&] -> grpc::Status {
            if (!coord.builder || !nixgrpc::ReverseCapability::matchesBuildInputs(call.delegated,
                    call.delegated.drvPath, call.delegated.assignId))
                return {grpc::StatusCode::PERMISSION_DENIED, "invalid delegated upload"};
            if (auto status = nixgrpc::Auth::authorize(call.delegated.caller, "UploadBuildInputs",
                    nixgrpc::Role::write); !status.ok()) return status;
            auto const live = [&] {
                return !cancelled() && !call.pipe->cancelled() && stopSignal == 0
                    && coord.builder->expects(call.delegated.drvPath, call.delegated.assignId);
            };
            if (!live()) return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment no longer live"};
            nix::remote::AddMultipleChunk first;
            if (!call.pipe->Read(&first) || !nixgrpc::ReverseCapability::permitsImport(call.delegated, first)
                || !live()) return {grpc::StatusCode::FAILED_PRECONDITION, "invalid or interrupted upload"};
            auto const repair = first.repair() ? nix::Repair : nix::NoRepair;
            auto const checkSigs = first.check_sigs() ? nix::CheckSigs : nix::NoCheckSigs;
            auto localStore = openScopedStore();
            nixgrpc::ZstdReaderSource<nixgrpc::ReverseUploadPipe, nix::remote::AddMultipleChunk> source(
                *call.pipe, std::move(*first.mutable_data()));
            nixgrpc::Metrics::Held const held(metrics, "UploadBuildInputs");
            nixgrpc::Metrics::Phase phase(metrics, "UploadBuildInputs", "recv");
            std::vector<std::string> imported;
            // Keep the source live while importUploads reads. A cancelled or
            // replaced assignment must not finish consuming an already
            // buffered NAR as if it still belonged to this worker.
            class CheckedSource : public nix::Source {
                nix::Source & inner;
                std::function<bool()> valid;
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
                std::function<void(size_t)> afterRead;
                size_t bytesRead = 0;
#endif
            public:
#ifndef NIX_GRPC_DAEMON_EMBED_TEST
                CheckedSource(nix::Source & inner, std::function<bool()> valid)
                    : inner(inner), valid(std::move(valid)) { }
#else
                CheckedSource(nix::Source & inner, std::function<bool()> valid, std::function<void(size_t)> afterRead)
                    : inner(inner), valid(std::move(valid)), afterRead(std::move(afterRead)) { }
#endif
                auto read(char * data, size_t length) -> size_t override {
                    if (!valid()) throw nix::Error("reverse upload assignment revoked");
                    auto const got = inner.read(data, length);
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
                    bytesRead += got;
                    if (got && afterRead) afterRead(bytesRead);
#endif
                    if (!valid()) throw nix::Error("reverse upload assignment revoked");
                    return got;
                }
            }
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
            checked(source, live, [this, drv = call.delegated.drvPath](size_t got) {
                if (reverseUploadReadHook) reverseUploadReadHook(drv, got);
            });
#else
            checked(source, live);
#endif
            auto const stats = importUploads(*localStore, checked,
                call.delegated.caller.role == nixgrpc::Role::write ? nix::NoRepair : repair,
                call.delegated.caller.role == nixgrpc::Role::write ? nix::CheckSigs : checkSigs,
                [&] { return !live(); }, imported,
                [&] {
                    // NARs are on disk, not in the store. Also recheck before
                    // each path (including after a reference lock wait).
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
                    if (reverseUploadSpooledHook) reverseUploadSpooledHook(call.delegated.drvPath);
#endif
                    source.finishFrame();
                    if (!call.pipe->endWithoutTrailingData() || !live())
                        throw nix::Error("reverse upload incomplete or assignment changed");
                });
            phase.next("publish");
            coord.cache.publish(*localStore, imported, [&] { return !live(); });
            if (!live()) return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment changed"};
            phase.done();
            metrics.countNarBytes("in", call.delegated.caller.name, stats.narBytes);
            return grpc::Status::OK;
        });
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            // The opaque worker protocol cannot be inspected here. Builds sent
            // through it bypass the scheduler, hence trusted only.
            auto const rpc = begin(*context, "Connect", nixgrpc::Role::trusted);

            nix::AutoCloseFD sock;
            try {
                sock = nix::connect(std::filesystem::path{backends.socketPath});
            } catch (nix::Error & err) {
                return {grpc::StatusCode::UNAVAILABLE, err.what()};
            }

            std::atomic<uint64_t> bytesIn{0};
            std::atomic<bool> received{false};
            std::jthread receiver([&] -> void {
                try {
                    bytesIn = nixgrpc::pumpStreamToFd(*stream, sock.get());
                } catch (...) {
                    nix::ignoreExceptionInDestructor();
                }
                received = true;
                ::shutdown(sock.get(), SHUT_WR);
            });

            uint64_t bytesOut = 0;
            try {
                bytesOut = nixgrpc::pumpFdToStream(sock.get(), *stream);
            } catch (...) {
                nix::ignoreExceptionInDestructor();
            }

            // nix-daemon hung up first: the client still waits for a reply, so end the call.
            if (!received) {
                context->TryCancel();
            }
            receiver.join();
            rpc.done({{"bytes_in", std::to_string(bytesIn.load())}, {"bytes_out", std::to_string(bytesOut)}});
            metrics.countTunnelBytes(rpc.caller.name, bytesIn, bytesOut);
            return grpc::Status::OK;
        });
    }

    auto QueryValidPaths(
        grpc::ServerContext * context,
        const nix::remote::QueryValidPathsRequest * request,
        nix::remote::QueryValidPathsReply * reply) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "QueryValidPaths", nixgrpc::Role::readOnly);
            nixgrpc::Metrics::Held const held(metrics, "QueryValidPaths");
            auto const localStore = getStore();
            nix::StorePathSet paths;
            for (const auto & path : request->paths()) {
                paths.insert(nix::StorePath(path));
            }
            // Substituting fills this node's disk, so read-only callers only query.
            auto const substitute = request->substitute() && rpc.caller.role >= nixgrpc::Role::write;
            std::vector<std::string> valid;
            nixgrpc::Metrics::Phase phase(metrics, "QueryValidPaths", "query");
            auto const here = localStore->queryValidPaths(paths, substitute ? nix::Substitute : nix::NoSubstitute);
                holdForSession(rpc, {here.begin(), here.end()});
            for (const auto & path : here) {
                reply->add_paths(std::string(path.to_string()));
                valid.push_back(localStore->printStorePath(path));
                paths.erase(path);
            }
            if (coord.cache.hasRemote()) {
                // Nodes answer as one store: what the cache has, every node can serve,
                for (const auto & path : localStore->querySubstitutablePaths(paths)) {
                    reply->add_paths(std::string(path.to_string()));
                }
                // and what only we have must become so before the client relies on it.
                phase.next("publish");
                publish(*localStore, valid, [&] -> bool { return context->IsCancelled(); });
            }
            return grpc::Status::OK;
        });
    }

    // An import that waits for a path lock must not stop reading its stream:
    // the unread bytes can fill an HTTP/2 window that other uploads share.
    template<class Source>
    auto importUploads(
        nix::Store & localStore,
        Source & source,
        nix::RepairFlag repair,
        nix::CheckSigsFlag checkSigs,
        const nixgrpc::Cancelled & cancelled,
        std::vector<std::string> & imported,
        const std::function<void()> & beforeApply = {}) -> nixgrpc::ImportStats
    {
        struct Owned
        {
            std::shared_ptr<const nix::ValidPathInfo> info;
            std::promise<void> promise;
            nixgrpc::SpoolRange nar{};
        };
        nixgrpc::NarSpool spool;
        std::vector<Owned> owned;
        std::map<std::string, std::shared_future<void>> theirs;
        std::set<std::string> seen;
        auto stats = nixgrpc::importPaths(
            localStore, source, [&](const nix::ValidPathInfo & info, nix::Source & nar) -> void {
                auto const name = localStore.printStorePath(info.path);
                // A repeat would wait on its own claim.
                if (!seen.insert(name).second) {
                    nixgrpc::NarSpool::discard(nar, info.narSize);
                    return;
                }
                auto entry = uploads.enter(name);
                if (entry.owner) {
                    owned.push_back({.info = std::make_shared<const nix::ValidPathInfo>(info),
                                     .promise = std::move(entry.promise),
                                     .nar = spool.add(nar, info.narSize)});
                } else {
                    nixgrpc::NarSpool::discard(nar, info.narSize);
                    theirs.emplace(name, std::move(entry.result));
                }
            });
        // The owner's failure is not ours: a replay can succeed.
        auto const await = [&](const std::string & name, const std::shared_future<void> & result) -> void {
            while (result.wait_for(nixgrpc::cancelPoll) != std::future_status::ready) {
                if (cancelled()) {
                    throw nixgrpc::CancelledWait("gave up waiting for a concurrent upload");
                }
            }
            try {
                result.get();
            } catch (const std::future_error &) {
                throw Unavailable("a concurrent upload of '%s' failed", name);
            }
        };
        // For reverse uploads the callback verifies both the complete wire
        // body and the current assignment; ordinary uploads have no gate.
        if (beforeApply) beforeApply();
        // Only references are awaited, so two uploads cannot wait on each other.
        for (auto & item : owned) {
            for (const auto & ref : item.info->references) {
                if (auto const other = theirs.find(localStore.printStorePath(ref)); other != theirs.end()) {
                    await(other->first, other->second);
                }
            }
            auto reader = spool.reader(item.nar);
            if (beforeApply) beforeApply();
            coord.cache.completeRefs(localStore, *item.info);
            if (beforeApply) beforeApply();
            localStore.addToStore(*item.info, reader, repair, checkSigs);
            imported.push_back(localStore.printStorePath(item.info->path));
            item.promise.set_value();
        }
        // Ours to publish as well: the owner may have answered before publishing.
        for (const auto & [name, result] : theirs) {
            await(name, result);
            imported.push_back(name);
        }
        return stats;
    }

    auto AddMultipleToStore(
        grpc::ServerContext * context,
        AddMultipleReader * reader,
        nix::remote::AddMultipleReply * /*reply*/) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "AddMultipleToStore", nixgrpc::Role::write);
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

            nixgrpc::Metrics::Held const held(metrics, "AddMultipleToStore");
            nixgrpc::Metrics::Phase phase(metrics, "AddMultipleToStore", "recv");
            std::vector<std::string> imported;
            auto const cancelled = [&] -> bool { return context->IsCancelled() || stopSignal != 0; };
            auto const stats = importUploads(*localStore, source, repair, checkSigs, cancelled, imported);
            holdForSession(
                rpc,
                imported | std::views::transform([&](const auto & path) -> nix::StorePath {
                    return localStore->parseStorePath(path);
                }) | std::ranges::to<std::vector>());
            phase.next("publish");
            publish(*localStore, imported, cancelled);
            phase.done();
            rpc.done({{"paths", std::to_string(stats.paths)}, {"nar_bytes_in", std::to_string(stats.narBytes)}});
            metrics.countNarBytes("in", rpc.caller.name, stats.narBytes);
            return grpc::Status::OK;
        });
    }

    auto UploadBuildInputs(grpc::ServerContext * context,
                           grpc::ServerReader<nix::remote::UploadBuildInputsChunk> * reader,
                           nix::remote::AddMultipleReply * /*reply*/) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            if (!reverseUploadRelay || !reverseGateway || !coord.dispatcher || coord.builder)
                return {grpc::StatusCode::FAILED_PRECONDITION, "upload requires a scheduler-only gateway"};
            nix::remote::UploadBuildInputsChunk first;
            if (!reader->Read(&first)) return {grpc::StatusCode::INVALID_ARGUMENT, "empty build-input upload"};
            auto const drv = first.drv_path();
            auto const assignment = first.assign_id();
            if (first.ByteSizeLong() > nixgrpc::ReverseUpload::maxChunkBytes || !first.has_chunk()
                || drv.empty() || assignment == 0)
                return {grpc::StatusCode::INVALID_ARGUMENT, "invalid upload header"};
            auto const caller = auth.identify(*context);
            if (caller.kind != nixgrpc::Caller::Kind::named)
                return {grpc::StatusCode::UNAUTHENTICATED, "reverse upload requires an authenticated client"};
            if (auto status = nixgrpc::Auth::authorize(caller, "UploadBuildInputs", nixgrpc::Role::write);
                !status.ok()) return status;
            auto const pod = coord.dispatcher->assignmentPod(drv, assignment);
            if (!pod) return {grpc::StatusCode::FAILED_PRECONDITION, "no live pod-bound upload assignment"};
            auto const assigned = [this, pod = *pod, drv, assignment] {
                return coord.dispatcher->podOwnsAssignment(pod, drv, assignment);
            };
            return reverseUploadRelay->call(*context,
                [&](uint64_t id) {
                    return reverseGateway->prepareBuildInputs(*context,
                        [this](std::string_view path, uint64_t assignId) {
                            return coord.dispatcher->assignmentPod(path, assignId);
                        }, id, drv, assignment);
                }, assigned, [&](nix::remote::UploadBuildInputsChunk & frame) { return reader->Read(&frame); },
                std::move(first));
        });
    }

    auto QueryPathInfos(
        grpc::ServerContext * context,
        const nix::remote::QueryPathInfosRequest * request,
        nix::remote::QueryPathInfosReply * reply) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "QueryPathInfos", nixgrpc::Role::readOnly);
            auto const localStore = getStore();
            for (const auto & name : request->paths()) {
                nix::StorePath const path(name);
                std::shared_ptr<const nix::ValidPathInfo> info;
                try {
                    substituteIfRemote(*localStore, path);
                    info = localStore->queryPathInfo(path).get_ptr();
                } catch (nix::Error & err) {
                    if (backendGone(err)) {
                        throw;
                    }
                    // invalid here and not substitutable: omitted from the reply
                    logDebug({{"event", "path_info_miss"}, {"path", name}, {"error", err.what()}});
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
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "QueryMissing", nixgrpc::Role::readOnly);
            auto const localStore = getStore();
            auto const missing = localStore->queryMissing(parseTargets(*localStore, request->targets()));
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
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "StoreInfo", nixgrpc::Role::readOnly);
            // build-remote sends BuildDerivation and unsigned inputs only to stores that trust it.
            reply->set_trusted(rpc.caller.role == nixgrpc::Role::trusted);
            return grpc::Status::OK;
        });
    }

    // Outputs built on another node live only in the cache. Clients read them back through us.
    void substituteIfRemote(nix::Store & store, const nix::StorePath & path) const
    {
        if (coord.cache.hasRemote()) {
            nixgrpc::Cache::substitute(store, path);
        }
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

    // Inputs may have been uploaded to, or built on, another worker.
    auto gatherInputs(
        nix::Store & localStore,
        const nix::BasicDerivation & drv,
        nix::Store & roots,
        nixgrpc::Metrics::Inputs & counted,
        std::string & missing) -> grpc::Status
    {
        try {
            const auto & inputs = nixcompat::drvInputs(drv);
            counted.wanted = inputs.size();
            auto const local = localStore.queryValidPaths(inputs);
            counted.fetched = inputs.size() - local.size();
            auto const before = std::chrono::steady_clock::now();
            for (const auto & input : inputs) {
                roots.addTempRoot(input);
                if (local.contains(input)) {
                    continue;
                }
                missing = localStore.printStorePath(input);
                nixcompat::ensurePath(localStore, input);
                missing.clear();
                counted.bytes += localStore.queryPathInfo(input)->narSize;
            }
            counted.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - before).count();
            metrics.inputs(counted);
        } catch (nix::Error & err) {
            // Another worker may still hold it locally. UNAVAILABLE makes the client retry elsewhere.
            metrics.event("input_not_substitutable");
            return {grpc::StatusCode::FAILED_PRECONDITION, workerName + ": input not substitutable: " + err.what()};
        }
        return grpc::Status::OK;
    }

    static auto missingFeature(nix::Store & store, const nix::BasicDerivation & drv) -> std::optional<std::string>
    {
        for (const auto & feature : nixcompat::requiredSystemFeatures(store, drv)) {
            if (!nix::settings.systemFeatures.get().contains(feature)) {
                return feature;
            }
        }
        return std::nullopt;
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

    // Second caller for a drv that already builds here: wait for its result.
    static auto attach(const std::function<bool()> & cancelled, nixgrpc::Expected::Shared & shared, const nixgrpc::BuildEventSink & log)
        -> std::optional<std::string>
    {
        log({.text = "attached to the build already running on this worker"});
        shared.attached++;
        auto const completed = nixgrpc::followSpill(
            shared.spill, shared.mutex, shared.cv, shared.finished, nixgrpc::cancelPoll,
            [&] -> bool { return cancelled() || stopSignal >= nixgrpc::kCancelBuilds; },
            [&](nixgrpc::SpillKind kind, std::string_view text) -> void {
                log({.kind = static_cast<nixgrpc::BuildEvent::Kind>(kind), .text = std::string(text)});
            });
        shared.attached--;
        if (!completed) {
            return std::nullopt;
        }
        // Set before `finished`, and never written again.
        return shared.resultWire;
    }

    auto attached(
        const std::function<bool()> & cancelled,
        nixgrpc::Expected::Shared & shared,
        const nixgrpc::BuildEventSink & log,
        const std::array<uint64_t, 3> & settings,
        nix::remote::BuildDerivationDone & done) -> grpc::Status
    {
        metrics.event("attached");
        auto wire = attach(cancelled, shared, log);
        if (shared.revoked) {
            return {grpc::StatusCode::ABORTED, "superseded, another worker was already building this"};
        }
        if (!wire) {
            throw nixgrpc::CancelledWait("gave up waiting for the running build");
        }
        if (wire->empty()) {
            return {grpc::StatusCode::UNAVAILABLE, "the build this call attached to did not finish, retry"};
        }
        // Its result (a timeout, or no check) is not ours. Retrying after it finished builds anew.
        if (shared.settings != settings) {
            return {grpc::StatusCode::UNAVAILABLE, "the build this call attached to ran with other mode or limits, retry"};
        }
        if (!done.ParseFromString(*wire)) {
            return {grpc::StatusCode::INTERNAL, "corrupt shared result"};
        }
        return grpc::Status::OK;
    }

    // Sends the owner's log from its own thread, so a slow client can't
    // block the relay.
    class OwnerRelay
    {
    public:
        OwnerRelay(const std::function<bool()> & cancelled, nixgrpc::Expected::Shared & shared,
                   const nixgrpc::BuildEventSink & log)
            : shared_(shared)
            , thread_([&cancelled, &shared, &log] -> void {
                (void) nixgrpc::followSpill(
                    shared.spill, shared.mutex, shared.cv, shared.finished, nixgrpc::cancelPoll,
                    [&] -> bool { return cancelled() || stopSignal >= nixgrpc::kCancelBuilds; },
                    [&](nixgrpc::SpillKind kind, std::string_view text) -> void {
                        log({.kind = static_cast<nixgrpc::BuildEvent::Kind>(kind), .text = std::string(text)});
                    });
            })
        {
        }

        OwnerRelay(const OwnerRelay &) = delete;
        OwnerRelay(OwnerRelay &&) = delete;
        auto operator=(const OwnerRelay &) -> OwnerRelay & = delete;
        auto operator=(OwnerRelay &&) -> OwnerRelay & = delete;

        // Builder::finished already did this unless the Expect was gone.
        ~OwnerRelay()
        {
            {
                const std::scoped_lock lock(shared_.mutex);
                shared_.finished = true;
                shared_.cv.notify_all();
            }
            thread_.join();
        }

    private:
        nixgrpc::Expected::Shared & shared_;
        std::jthread thread_;
    };

    // gRPC errors mean "ask the scheduler again", build outcomes travel as BuildResult.
    auto build(
        const std::function<bool()> & cancelled,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        nix::BuildMode mode,
        nixgrpc::Backends::Limits limits,
        uint64_t assignId,
        const nixgrpc::BuildEventSink & log,
        nixgrpc::Metrics::Inputs & inputs,
        nix::remote::BuildDerivationDone & done,
        std::function<void()> * deferredDone = nullptr,
        std::string * missingInput = nullptr) -> grpc::Status
    {
        if (!coord.builder) {
            return {grpc::StatusCode::FAILED_PRECONDITION, "not a builder"};
        }
        auto & builder = *coord.builder;
        auto const outPaths = staticOutputs(localStore, drv);
        if (outPaths.size() != drv.outputs.size()) {
            return {grpc::StatusCode::UNIMPLEMENTED, "builds need statically known output paths"};
        }
        if (auto feature = missingFeature(localStore, drv)) {
            return {grpc::StatusCode::FAILED_PRECONDITION, "lacks system feature '" + *feature + "'"};
        }
        auto const drvName = std::string(drvPath.to_string());
        // The client is here: the lease holds through its upload. NOT_FOUND
        // makes it upload the closure and retry, so admit comes after.
        builder.claim(drvName);
        if (!localStore.isValidPath(drvPath)) {
            try {
                nixcompat::ensurePath(localStore, drvPath);
            } catch (nix::Error &) {
                return {grpc::StatusCode::NOT_FOUND, "missing: " + localStore.printStorePath(drvPath)};
            }
        }
        if (cancelled()) return {grpc::StatusCode::CANCELLED, "build cancelled before admission"};
        auto adm = builder.admit(drvName, assignId);
        if (!adm) {
            metrics.event("unexpected_build");
            return {grpc::StatusCode::FAILED_PRECONDITION, "not scheduled here: " + drvName};
        }
        std::array<uint64_t, 3> const settings{static_cast<uint64_t>(mode), limits.buildTimeout, limits.maxSilentTime};
        if (adm->attach) {
            return attached(cancelled, *adm->shared, log, settings, done);
        }

        // From here on we own the Expect and must report Done exactly once.
        adm->shared->settings = settings;
        auto outcome = nix::remote::Done::FAILED;
        auto reason = nix::remote::Done::REASON_UNSPECIFIED;
        std::vector<std::pair<std::string, uint64_t>> outputs;
        std::string wire;
        auto const report = [&] -> void {
            builder.finished(drvName, adm->assignId, outcome, outputs, wire, reason, deferredDone);
        };
        auto const tee = [&](const nixgrpc::BuildEvent & event) -> void {
            adm->shared->logged(static_cast<nixgrpc::SpillKind>(event.kind), event.text);
        };
        OwnerRelay const relay(cancelled, *adm->shared, log);
        try {
            auto status =
                buildExpected(
                    cancelled, *adm->shared, localStore, drvPath, drv, mode, limits, tee, inputs, outPaths, done, outcome,
                    outputs, missingInput);
            // No result to share unless the build produced one: attachers of a
            // failed owner must retry, not return an empty Done as success.
            if (status.ok()) {
                wire = done.SerializeAsString();
            } else if (auto const why = giveBackReason(status)) {
                outcome = nix::remote::Done::GIVEN_BACK;
                reason = *why;
            } else {
                outcome = nix::remote::Done::FAILED;
            }
            report();
            return status;
        } catch (nixgrpc::CancelledWait &) {
            outcome = nix::remote::Done::CANCELLED;
            report();
            if (adm->shared->revoked) {
                metrics.event("revoked");
                return {grpc::StatusCode::ABORTED, "superseded, another worker was already building this"};
            }
            throw;
        } catch (Unavailable &) {
            outcome = nix::remote::Done::GIVEN_BACK;
            reason = nix::remote::Done::PUBLISH_UNAVAILABLE;
            report();
            throw;
        } catch (...) {
            report();
            throw;
        }
    }

    // A failure of this worker, not of the build: another one may do better.
    static auto giveBackReason(const grpc::Status & status) -> std::optional<nix::remote::Done::Reason>
    {
        switch (status.error_code()) {
        case grpc::StatusCode::FAILED_PRECONDITION:
            return nix::remote::Done::INPUT_UNAVAILABLE;
        case grpc::StatusCode::UNAVAILABLE:
            return nix::remote::Done::TRANSIENT_FAILURE;
        default:
            return std::nullopt;
        }
    }

    auto buildExpected(
        const std::function<bool()> & isCancelled,
        nixgrpc::Expected::Shared & shared,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        nix::BuildMode mode,
        nixgrpc::Backends::Limits limits,
        const nixgrpc::BuildEventSink & log,
        nixgrpc::Metrics::Inputs & inputs,
        const std::map<std::string, nix::StorePath> & outPaths,
        nix::remote::BuildDerivationDone & done,
        nix::remote::Done::Outcome & outcome,
        std::vector<std::pair<std::string, uint64_t>> & outputs,
        std::string * missingInput) -> grpc::Status
    {
        auto const cancelled = [&] -> bool {
            return (isCancelled() && shared.attached == 0) || shared.revoked || stopSignal >= nixgrpc::kCancelBuilds;
        };
        nixgrpc::Metrics::Phase phase(metrics, "BuildDerivation", "substitute");
        // Roots inputs and outputs until publish is done.
        auto const roots = openScopedStore();
        std::string missing;
        if (auto status = gatherInputs(localStore, drv, *roots, inputs, missing); !status.ok()) {
            if (!missing.empty() && missingInput) {
                *missingInput = missing;
            }
            return status;
        }
        for (const auto & [name, path] : outPaths) {
            roots->addTempRoot(path);
        }
        nix::BuildResult res;
        log({.text = workerName + ": building " + std::string(drvPath.to_string())});
        try {
            phase.next("build");
            nixgrpc::Metrics::Held const building(metrics, "build_slot");
            res = backends.storedBuild(cancelled, localStore, drvPath, mode, limits, log);
            phase.done();
        } catch (nix::Error &) {
            if (cancelled()) {
                throw nixgrpc::CancelledWait("build interrupted");
            }
            throw;
        }
        if (!nixcompat::succeeded(res) && cancelled()) {
            // nix-daemon killed the builder when we hung up and still got its
            // "failed: signal 9" result out before the socket closed.
            throw nixgrpc::CancelledWait("build interrupted");
        }
        metrics.event(nixcompat::succeeded(res) ? "built" : "build_failed");
        if (auto const status = nixcompat::failureStatus(res)) {
            metrics.buildFailure(nixcompat::failureStatusName(*status));
        }
        if (nixcompat::succeeded(res)) {
            std::vector<std::string> built;
            built.reserve(outPaths.size());
            for (const auto & [name, path] : outPaths) {
                built.push_back(localStore.printStorePath(path));
                outputs.emplace_back(built.back(), localStore.queryPathInfo(path)->narSize);
            }
            phase.next("publish");
#ifdef NIX_GRPC_DAEMON_EMBED_TEST
            if (publishHook) {
                try {
                    publishHook(std::string(drvPath.to_string()));
                } catch (nix::Error & err) {
                    throw Unavailable("publish failed: %s", err.what());
                }
            }
#endif
            publish(localStore, built, cancelled);
            phase.done();
            outcome = nix::remote::Done::BUILT;
        } else if (nixcompat::failureStatus(res) == nixcompat::FailureStatus::TransientFailure && nixcompat::isSandboxed(drv)) {
            // ENOSPC and the like: not the build's fault, let the scheduler place it again.
            // Nix reports every failed fixed-output build as transient too, which is the build's result.
            return {grpc::StatusCode::UNAVAILABLE, nixcompat::buildFailureMsg(res).value_or("transient failure")};
        }
        nixgrpc::encodeResult(localStore, res, &done);
        return grpc::Status::OK;
    }

    auto BuildDerivation(
        grpc::ServerContext * context,
        const nix::remote::BuildDerivationRequest * request,
        BuildWriter * writer) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "BuildDerivation", nixgrpc::Role::write, request->build_mode());
            // The whole call, attachers too: the drain waits for it.
            nixgrpc::Metrics::Held const inflight(metrics, "BuildDerivation");
            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            // Only a signed pod-bound scheduler assignment may select the
            // reverse data route. No x-nix-worker metadata participates.
            // Direct workers continue through the existing local handler.
            auto const selectedPod = reverseRelay && coord.dispatcher
                ? coord.dispatcher->assignmentPod(request->drv_path(), request->assign_id()) : std::nullopt;
            if (selectedPod) {
                std::atomic<bool> receivedDone{false};
                auto const status = reverseRelay->call(*context,
                    [&](uint64_t id) {
                        return reverseGateway->prepareBuild(*context,
                            [this](std::string_view drv, uint64_t assignment) {
                                return coord.dispatcher->assignmentPod(drv, assignment);
                            }, id, request->drv_path(), request->assign_id(), request->build_mode());
                    }, *request, [&](std::string_view wire) {
                        nix::remote::BuildDerivationChunk chunk;
                        if (!chunk.ParseFromArray(wire.data(), static_cast<int>(wire.size()))
                            || chunk.event_case() == nix::remote::BuildDerivationChunk::EVENT_NOT_SET || receivedDone.load())
                            return false;
                        if (!writer->Write(chunk)) return false;
                        if (chunk.has_done()) receivedDone = true;
                        return true;
                    }, [this, drv = request->drv_path(), assignment = request->assign_id(),
                        pod = *selectedPod] {
                        return coord.dispatcher->podOwnsAssignment(pod, drv, assignment);
                    }, [&] { return receivedDone.load(); });
                return status.ok() && !receivedDone.load()
                    ? grpc::Status{grpc::StatusCode::DATA_LOSS, "reverse build completed without a result"} : status;
            }
            // Scheduler-only frontends never build locally: a stale or
            // certificate-only assignment must not fall back to the
            // scheduler's own Nix store. Direct clients still dial the worker
            // through the existing balancer and use the ordinary handler.
            if (!coord.builder) {
                return {grpc::StatusCode::FAILED_PRECONDITION, "no pod-bound build assignment on this frontend"};
            }
            // No drain check: the scheduler stops sending new work, and what it already sent runs.
            auto const localStore = getStore();
            auto const sendLogLine = [&](nixgrpc::BuildEvent event) -> void {
                writer->Write(nixgrpc::toChunk<nix::remote::BuildDerivationChunk>(std::move(event)));
            };

            nix::StorePath const drvPath(request->drv_path());
            nix::StringSource drvSource(request->drv());
            nix::BasicDerivation drv;
            nixcompat::readDrv(drvSource, *localStore, drv, nix::Derivation::nameFromPath(drvPath));
            auto const mode = static_cast<nix::BuildMode>(request->build_mode());
            nixgrpc::Backends::Limits const limits{
                .buildTimeout = request->build_timeout(), .maxSilentTime = request->max_silent_time()};

            std::vector<nix::StorePath> held{drvPath};
            std::ranges::copy(nixcompat::drvInputs(drv), std::back_inserter(held));
            for (auto & [name, path] : staticOutputs(*localStore, drv)) {
                held.push_back(std::move(path));
            }
            holdForSession(rpc, held);

            nix::remote::BuildDerivationChunk chunk;
            nixgrpc::Metrics::Inputs inputs;
            std::string missingInput;
            if (auto const status =
                    build([&] { return context->IsCancelled(); }, *localStore, drvPath, drv, mode, limits,
                          request->assign_id(), sendLogLine, inputs, *chunk.mutable_done(), nullptr, &missingInput);
                !status.ok()) {
                if (!missingInput.empty()) {
                    context->AddTrailingMetadata("x-nix-missing-input", missingInput);
                }
                return {status.error_code(), workerName + ": " + status.error_message()};
            }
            writer->Write(chunk);
            rpc.done(
                {{"drv", std::string(drvPath.to_string())},
                 {"assign_id", std::to_string(request->assign_id())},
                 {"outputs", std::to_string(chunk.done().outputs_size())},
                 {"inputs", std::to_string(inputs.wanted)},
                 {"inputs_fetched", std::to_string(inputs.fetched)},
                 {"input_bytes", std::to_string(inputs.bytes)},
                 {"input_s", std::to_string(static_cast<int>(inputs.seconds))}});
            return grpc::Status::OK;
        });
    }

    auto FetchNars(
        grpc::ServerContext * context,
        const nix::remote::FetchNarsRequest * request,
        NarFrameWriter * writer) -> grpc::Status override
    {
        return guarded([&] -> grpc::Status {
            auto const rpc = begin(*context, "FetchNars", nixgrpc::Role::readOnly);
            nixgrpc::Metrics::Held const held(metrics, "FetchNars"); // the drain waits for it
            auto const localStore = openScopedStore();

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
                nix::StorePath const path(request->paths(idx));
                substituteIfRemote(*localStore, path);
                localStore->narFromPath(path, counting);
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

namespace {
// Claimed leases count too: their clients may still be uploading.
auto drained(nixgrpc::Coordinator & coord, nixgrpc::Metrics & metrics) -> bool
{
    return (!coord.builder || coord.builder->holding() == 0)
           && metrics.inflightNow("BuildDerivation") + metrics.inflightNow("FetchNars") == 0;
}

// Returns on idle timeout or SIGTERM. A builder's first SIGTERM only stops
// taking builds and this returns once those in flight have reported. The
// supervisor's stop timeout or a second signal bounds that.
void serve(
    grpc::Server const & server,
    NixRemoteService & service,
    const nixgrpc::Options & options,
    nixgrpc::IdleTracker const & idle,
    nixgrpc::Coordinator & coord,
    nixgrpc::Metrics & metrics)
{
    auto const watchdog = nixgrpc::sdWatchdogInterval();
    std::chrono::nanoseconds const tick =
        watchdog.count() != 0 ? std::min<std::chrono::nanoseconds>(watchdog, std::chrono::seconds(1))
                              : std::chrono::seconds(1);
    bool draining = false;
    for (;;) {
        if (stopSignal == 0) {
            if (options.idleTimeout && idle.idleFor() >= *options.idleTimeout) {
                return;
            }
            coord.tick();
            service.tick();
        } else {
            if (!draining) {
                draining = true;
                nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "draining"}});
                if (coord.builder) {
                    coord.builder->setDraining(true);
                }
                server.GetHealthCheckService()->SetServingStatus("", false);
            }
            coord.tick(); // gives back unclaimed leases
            if (stopSignal >= nixgrpc::kCancelBuilds || drained(coord, metrics)) {
                return;
            }
        }
        if (watchdog.count() != 0) {
            nixgrpc::sdNotify("WATCHDOG=1");
        }
        std::this_thread::sleep_for(tick);
    }
}
} // namespace

namespace {

constexpr std::string_view unixPrefix = "unix:";

// What the daemon may write, execute, connect to and listen on after startup.
auto sandboxPolicy(const nixgrpc::Options & options, bool listenFdsEmpty) -> nixgrpc::sandbox::Policy
{
    using nixgrpc::sandbox::CanonicalPath;
    namespace fs = std::filesystem;
    nixgrpc::sandbox::Policy policy;
    policy.execPaths.push_back(CanonicalPath::from(options.storeDir));
    policy.writePaths.push_back(CanonicalPath::from(fs::temp_directory_path()));
    for (auto const & path : options.sandboxWrite) {
        policy.writePaths.push_back(CanonicalPath::from(path));
    }
    policy.connectSockets.push_back(CanonicalPath::from(options.socketPath));

    // Only what the daemon binds itself: systemd hands over the main listener.
    // An address we cannot read a TCP port from leaves binding unrestricted.
    std::vector<uint16_t> ports;
    for (const std::string_view addr : {std::string_view(options.metricsListen), std::string_view(listenFdsEmpty ? options.listen : "")}) {
        if (addr.empty()) {
            continue;
        }
        if (addr.starts_with(unixPrefix)) {
            policy.listenSockets.push_back(CanonicalPath::from(addr.substr(unixPrefix.size())));
            continue;
        }
        auto const port = nixgrpc::parseInt<uint16_t>(addr.substr(addr.rfind(':') + 1));
        if (!port) {
            policy.listenPorts = nixgrpc::sandbox::AnyPort{};
            return policy;
        }
        ports.push_back(*port);
    }
    policy.listenPorts = std::move(ports);
    return policy;
}

} // namespace

#ifndef NIX_GRPC_DAEMON_EMBED_TEST
auto main(int argc, char ** argv) -> int
try {
    // Pump threads write to a socket whose peer may already be gone; we want
    // EPIPE, not process death.
    // NOLINTBEGIN(misc-include-cleaner): darwin's <signal.h> forwards these.
    struct sigaction act{};
    act.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &act, nullptr);
    // Polled by the main loop so in-flight RPCs get the shutdown grace.
    act.sa_handler = [](int) -> void { stopSignal = stopSignal + 1; };
    for (int const sig : {SIGTERM, SIGINT}) {
        sigaction(sig, &act, nullptr);
    }
    // NOLINTEND(misc-include-cleaner)

    // Required before nix::openStore() in the native RPC handlers.
    nix::initLibStore();

    const std::span args(argv, static_cast<size_t>(argc));
    auto options = nixgrpc::parseOptions({args.begin(), args.end()});
    auto const listenFds = nixgrpc::systemdListenFds();
    if (options.sandbox) {
        nixgrpc::sandbox::apply(sandboxPolicy(options, listenFds.empty()));
    }
    if (listenFds.empty() && options.idleTimeout) {
        // Nobody would restart us on the next connection.
        throw nix::Error("--idle-timeout requires systemd socket activation");
    }

    nixgrpc::Metrics metrics(options.metricsListen);
    metrics.buildInfo(
        NIX_GRPC_VERSION,
        options.workerName,
        nix::settings.thisSystem.get(),
        nix::concatStringsSep(",", nix::settings.systemFeatures.get()));
    nixgrpc::IdleTracker idle;
    if (options.storeUri.empty()) {
        options.storeUri = "unix://" + options.socketPath;
    }
    std::optional<nixgrpc::oidc::Verifier> oidc;
    if (!options.oidcConfig.empty()) {
        auto cfg = nixgrpc::oidc::loadConfig(options.oidcConfig);
        for (const auto & provider : cfg.providers) {
            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "oidc_provider"}, {"name", provider.name}, {"issuer", provider.issuer}});
        }
        oidc.emplace(std::move(cfg));
    }
    std::optional<nixgrpc::oidc::Verifier> reverseOidc;
    std::optional<nixgrpc::WorkerIdentity> reverseIdentity;
    std::unique_ptr<nixgrpc::ReverseSessionService> reverseService;
    if (!options.reverseOidcConfig.empty()) {
        reverseOidc.emplace(nixgrpc::oidc::loadConfig(options.reverseOidcConfig));
        reverseIdentity.emplace(
            *reverseOidc, options.reverseProvider, options.reverseNamespace, options.reverseServiceAccount,
            options.reverseAudience, options.reverseMaxTokenLifetime);
    }
    nixgrpc::Auth auth{.acl = options.acl, .proxies = options.proxies, .oidc = &oidc};
    nixgrpc::Coordinator coord(options, auth, metrics, idle);
    if (reverseIdentity) {
        reverseService = std::make_unique<nixgrpc::ReverseSessionService>(*reverseIdentity, coord.reverseRegistry, auth);
        if (coord.scheduler) {
            coord.scheduler->setReverseIdentity(*reverseIdentity);
        }
    }
    nixgrpc::logLine(
        nixgrpc::LogLevel::info,
        {{"event", "roles"},
         {"builder", coord.builder ? "1" : "0"},
         {"scheduler", coord.scheduler ? "1" : "0"},
         {"scheduler_addr", options.schedulerAddr},
         {"advertise", options.advertise},
         {"niks3", options.niks3.url},
         {"max_jobs", std::to_string(options.maxJobs)}});
    NixRemoteService service(
        options.socketPath, options.storeUri, options.workerName, metrics, idle, options.logLevel, auth, coord);
    if (coord.builder && !options.reverseTokenFile.empty()) {
        coord.setReverseBuild([&service](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            return service.reverseBuild(call, emit, cancelled, afterFinish);
        });
        coord.setReverseUpload([&service](auto const & call, auto const & cancelled) {
            return service.reverseUpload(call, cancelled);
        });
    }

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    // Clients stream NARs in 256 KiB chunks and Wants in 4 MiB batches. The
    // quota bounds what peers, authenticated or not, can make us buffer.
    constexpr int maxReceiveBytes = 64 << 20;
    constexpr size_t memoryQuotaBytes = size_t{1} << 30U;
    grpc::ResourceQuota quota("nix-grpc-daemon");
    quota.Resize(memoryQuotaBytes);
    builder.SetResourceQuota(quota);
    builder.SetMaxReceiveMessageSize(maxReceiveBytes);
    builder.SetMaxSendMessageSize(-1);
    // A balancer keeps idle upstream connections alive with pings.
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    // Builds keep streams silent for hours: drop clients that vanished without a FIN.
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, nixgrpc::kKeepaliveTimeMs);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, nixgrpc::kKeepaliveTimeoutMs);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    constexpr int minPingIntervalMs = 10'000;
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, minPingIntervalMs);
    auto const creds = nixgrpc::makeServerCredentials(options);
    std::shared_ptr<grpc::experimental::ExternalConnectionAcceptor> acceptor;
    if (listenFds.empty()) {
        builder.AddListeningPort(options.listen, creds);
    } else {
        options.listen = "systemd";
        acceptor = builder.experimental().AddExternalConnectionAcceptor(
            grpc::ServerBuilder::experimental_type::ExternalConnectionType::FROM_FD, creds);
    }
    builder.RegisterService(&service);
    if (coord.scheduler) {
        builder.RegisterService(coord.scheduler.get());
        builder.RegisterService(coord.eds.get());
        if (reverseService) {
            builder.RegisterService(reverseService.get());
        }
    }

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
    coord.start(*server);
    nixgrpc::sdNotify("READY=1");

    serve(*server, service, options, idle, coord, metrics);
    nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", stopSignal != 0 ? "signal_exit" : "idle_exit"}});
    stopSignal = nixgrpc::kCancelBuilds;
    nixgrpc::sdNotify("STOPPING=1");
    server->GetHealthCheckService()->SetServingStatus(false);
    coord.restarting();
    // Shutdown() then waits for handlers stuck in nix. Clients already got CANCELLED.
    static constexpr std::chrono::seconds shutdownGrace{5};
    std::thread([] -> void {
        std::this_thread::sleep_for(shutdownGrace + std::chrono::seconds(1));
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "shutdown_forced"}});
        std::_Exit(0);
    }).detach();
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
#endif
