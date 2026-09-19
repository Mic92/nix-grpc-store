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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include <grpc/impl/channel_arg_names.h>
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
#include "logfmt.hh"
#include "oidc.hh"
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

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler.
volatile std::sig_atomic_t nixgrpc::stopSignal = 0;

namespace {
using nixgrpc::stopSignal;

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

public:
    // begin() throws this for callers the ACL refuses.
    struct Denied
    {
        grpc::Status status;
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
        } catch (std::exception & err) {
            if (stopSignal != 0) {
                return {grpc::StatusCode::UNAVAILABLE, std::string("worker shutting down: ") + err.what()};
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
        if (auto status = nixgrpc::Auth::authorize(caller, method, minRole, buildMode); !status.ok()) {
            throw Denied{status};
        }
        metrics.countRpc(std::string(method), caller.name);
        logDebug({{"event", "rpc_start"}, {"method", std::string(method)}, {"cn", caller.name}, {"peer", context.peer()}});
        return Rpc{.context = &context, .method = method, .caller = std::move(caller)};
    }

public:
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
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
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
        });
    }

    auto QueryValidPaths(
        grpc::ServerContext * context,
        const nix::remote::QueryValidPathsRequest * request,
        nix::remote::QueryValidPathsReply * reply) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const rpc = begin(*context, "QueryValidPaths", nixgrpc::Role::readOnly);
            nixgrpc::Metrics::Held const held(metrics, "QueryValidPaths");
            auto localStore = getStore();
            nix::StorePathSet paths;
            for (const auto & path : request->paths()) {
                paths.insert(nix::StorePath(path));
            }
            std::vector<std::string> valid;
            nixgrpc::Metrics::Phase phase(metrics, "QueryValidPaths", "query");
            for (const auto & path :
                 localStore->queryValidPaths(paths, request->substitute() ? nix::Substitute : nix::NoSubstitute)) {
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
                coord.cache.publish(valid, [&]() -> bool { return context->IsCancelled(); });
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
            std::vector<std::string> imported;
            nixgrpc::Metrics::Phase phase(metrics, "AddMultipleToStore", "recv");
            auto stats = nixgrpc::importPaths(
                *localStore, source, [&](const nix::ValidPathInfo & info, nix::Source & nar) -> void {
                    coord.cache.completeRefs(*localStore, info);
                    localStore->addToStore(info, nar, repair, checkSigs);
                    imported.push_back(localStore->printStorePath(info.path));
                });
            phase.next("publish");
            coord.cache.publish(imported, [&]() -> bool { return context->IsCancelled(); });
            phase.done();
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
            auto const rpc = begin(*context, "QueryPathInfos", nixgrpc::Role::readOnly);
            auto localStore = getStore();
            for (const auto & name : request->paths()) {
                nix::StorePath const path(name);
                std::shared_ptr<const nix::ValidPathInfo> info;
                try {
                    substituteIfRemote(*localStore, path);
                    info = localStore->queryPathInfo(path).get_ptr();
                } catch (nix::Error & err) {
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
        return guarded([&]() -> grpc::Status {
            auto const rpc = begin(*context, "QueryMissing", nixgrpc::Role::readOnly);
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
    auto gatherInputs(nix::Store & localStore, const nix::BasicDerivation & drv, nix::Store & roots) -> grpc::Status
    {
        try {
            for (const auto & input : nixcompat::drvInputs(drv)) {
                roots.addTempRoot(input);
                nixcompat::ensurePath(localStore, input);
            }
        } catch (nix::Error & err) {
            // Another worker may still hold it locally. UNAVAILABLE makes the client retry elsewhere.
            metrics.event("input_not_substitutable");
            return {grpc::StatusCode::UNAVAILABLE, workerName + ": input not substitutable: " + err.what()};
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
    static auto attach(grpc::ServerContext & context, nixgrpc::Expected::Shared & shared, const nixgrpc::BuildEventSink & log)
        -> std::optional<std::string>
    {
        log({.text = "attached to the build already running on this worker"});
        std::unique_lock lock(shared.mutex);
        while (!shared.finished) {
            if (context.IsCancelled() || stopSignal >= nixgrpc::kCancelBuilds) {
                return std::nullopt;
            }
            shared.cv.wait_for(lock, nixgrpc::cancelPoll);
        }
        return shared.resultWire;
    }

    // gRPC errors mean "ask the scheduler again", build outcomes travel as BuildResult.
    auto build(
        grpc::ServerContext & context,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        nix::BuildMode mode,
        const nixgrpc::BuildEventSink & log,
        nix::remote::BuildDerivationDone & done) -> grpc::Status
    {
        if (!coord.builder) {
            return {grpc::StatusCode::FAILED_PRECONDITION, "not a builder"};
        }
        auto & builder = *coord.builder;
        auto outPaths = staticOutputs(localStore, drv);
        if (outPaths.size() != drv.outputs.size()) {
            return {grpc::StatusCode::UNIMPLEMENTED, "builds need statically known output paths"};
        }
        if (auto feature = missingFeature(localStore, drv)) {
            return {grpc::StatusCode::FAILED_PRECONDITION, "lacks system feature '" + *feature + "'"};
        }
        // Before admit: NOT_FOUND makes the client upload the closure and retry, which must not consume the Expect.
        if (!localStore.isValidPath(drvPath)) {
            try {
                nixcompat::ensurePath(localStore, drvPath);
            } catch (nix::Error &) {
                return {grpc::StatusCode::NOT_FOUND, "missing: " + localStore.printStorePath(drvPath)};
            }
        }
        auto const drvName = std::string(drvPath.to_string());
        auto adm = builder.admit(drvName);
        if (!adm) {
            metrics.event("unexpected_build");
            return {grpc::StatusCode::FAILED_PRECONDITION, "not scheduled here: " + drvName};
        }
        if (adm->attach) {
            metrics.event("attached");
            auto wire = attach(context, *adm->shared, log);
            if (!wire) {
                throw nixgrpc::CancelledWait("gave up waiting for the running build");
            }
            if (!done.ParseFromString(*wire)) {
                return {grpc::StatusCode::INTERNAL, "corrupt shared result"};
            }
            return grpc::Status::OK;
        }

        // From here on we own the Expect and must report Done exactly once.
        auto outcome = nix::remote::Done::FAILED;
        std::vector<std::pair<std::string, uint64_t>> outputs;
        std::string wire;
        auto report = [&]() -> void { builder.finished(drvName, outcome, outputs, wire); };
        try {
            auto status = buildExpected(context, localStore, drvPath, drv, mode, log, outPaths, done, outcome, outputs);
            wire = done.SerializeAsString();
            if (!status.ok()) {
                outcome = nix::remote::Done::FAILED;
            }
            report();
            return status;
        } catch (nixgrpc::CancelledWait &) {
            outcome = nix::remote::Done::CANCELLED;
            report();
            throw;
        } catch (...) {
            report();
            throw;
        }
    }

    auto buildExpected(
        grpc::ServerContext & context,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        nix::BuildMode mode,
        const nixgrpc::BuildEventSink & log,
        const std::map<std::string, nix::StorePath> & outPaths,
        nix::remote::BuildDerivationDone & done,
        nix::remote::Done::Outcome & outcome,
        std::vector<std::pair<std::string, uint64_t>> & outputs) -> grpc::Status
    {
        auto cancelled = [&]() -> bool { return context.IsCancelled() || stopSignal >= nixgrpc::kCancelBuilds; };
        nixgrpc::Metrics::Held const held(metrics, "BuildDerivation");
        nixgrpc::Metrics::Phase phase(metrics, "BuildDerivation", "substitute");
        // Roots inputs and outputs until publish is done.
        auto roots = openScopedStore();
        if (auto status = gatherInputs(localStore, drv, *roots); !status.ok()) {
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
            res = backends.storedBuild(context, localStore, drvPath, mode, log);
            phase.done();
        } catch (nix::Error &) {
            if (cancelled()) {
                throw nixgrpc::CancelledWait("build interrupted");
            }
            throw;
        }
        metrics.event(nixcompat::succeeded(res) ? "built" : "build_failed");
        if (nixcompat::succeeded(res)) {
            std::vector<std::string> built;
            built.reserve(outPaths.size());
            for (const auto & [name, path] : outPaths) {
                built.push_back(localStore.printStorePath(path));
                outputs.emplace_back(built.back(), localStore.queryPathInfo(path)->narSize);
            }
            try {
                phase.next("publish");
                coord.cache.publish(built, cancelled);
                phase.done();
            } catch (nixgrpc::CancelledWait &) {
                throw;
            } catch (nix::Error & err) {
                return {grpc::StatusCode::UNAVAILABLE, std::string("publish failed: ") + err.what()};
            }
            outcome = nix::remote::Done::BUILT;
        } else if (nixcompat::failureStatus(res) == nixcompat::FailureStatus::TransientFailure) {
            // ENOSPC and the like: not the build's fault, let the scheduler place it again.
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
        return guarded([&]() -> grpc::Status {
            auto const rpc = begin(*context, "BuildDerivation", nixgrpc::Role::write, request->build_mode());
            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            if (stopSignal != 0) {
                return {grpc::StatusCode::UNAVAILABLE, "worker draining"};
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

            nix::remote::BuildDerivationChunk chunk;
            if (auto status = build(*context, *localStore, drvPath, drv, mode, sendLogLine, *chunk.mutable_done()); !status.ok()) {
                return {status.error_code(), workerName + ": " + status.error_message()};
            }
            writer->Write(chunk);
            rpc.done(
                {{"drv", std::string(drvPath.to_string())},
                 {"assign_id", std::to_string(request->assign_id())},
                 {"outputs", std::to_string(chunk.done().outputs_size())}});
            return grpc::Status::OK;
        });
    }

    auto FetchNars(
        grpc::ServerContext * context,
        const nix::remote::FetchNarsRequest * request,
        NarFrameWriter * writer) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const rpc = begin(*context, "FetchNars", nixgrpc::Role::readOnly);
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
// Returns on idle timeout or SIGTERM. A builder's first SIGTERM only stops
// taking builds and this returns once those in flight have reported. The
// supervisor's stop timeout or a second signal bounds that.
void serve(
    grpc::Server & server,
    const nixgrpc::Options & options,
    nixgrpc::IdleTracker & idle,
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
        } else if (stopSignal >= nixgrpc::kCancelBuilds || !coord.builder || metrics.inflightNow("BuildDerivation") == 0) {
            return;
        } else if (!draining) {
            draining = true;
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "draining"}});
            coord.builder->setDraining(true);
            server.GetHealthCheckService()->SetServingStatus("", false);
        }
        if (watchdog.count() != 0) {
            nixgrpc::sdNotify("WATCHDOG=1");
        }
        std::this_thread::sleep_for(tick);
    }
}
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
    nixgrpc::Auth auth{.acl = options.acl, .proxies = options.proxies, .oidc = &oidc};
    nixgrpc::Coordinator coord(options, auth, metrics);
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

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    // A balancer keeps idle upstream connections alive with pings.
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    constexpr int minPingIntervalMs = 10'000;
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, minPingIntervalMs);
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
    if (coord.scheduler) {
        builder.RegisterService(coord.scheduler.get());
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

    serve(*server, options, idle, coord, metrics);
    nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", stopSignal != 0 ? "signal_exit" : "idle_exit"}});
    stopSignal = nixgrpc::kCancelBuilds;
    nixgrpc::sdNotify("STOPPING=1");
    server->GetHealthCheckService()->SetServingStatus(false);
    coord.restarting();
    // Shutdown() then waits for handlers stuck in nix. Clients already got CANCELLED.
    static constexpr std::chrono::seconds shutdownGrace{5};
    std::thread([]() -> void {
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
