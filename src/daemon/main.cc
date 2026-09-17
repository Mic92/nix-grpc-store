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
#include <chrono>
#include <csignal>
#include <signal.h> // NOLINT(modernize-deprecated-headers): sigaction is POSIX, not in <csignal>
#include <cstddef>
#include <initializer_list>
#include <iterator>
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
#include <variant>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

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
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "build-log.hh"
#include "acl.hh"
#include "auth.hh"
#include "backend.hh"
#include "farm.hh"
#include "options.hh"
#include "claim.hh"
#include "push.hh"
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
#include "xfcc.hh"

using GrpcStream = grpc::ServerReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;
using AddMultipleReader = grpc::ServerReader<nix::remote::AddMultipleChunk>;
using NarFrameWriter = grpc::ServerWriter<nix::remote::NarFrame>;
using BuildWriter = grpc::ServerWriter<nix::remote::BuildDerivationChunk>;
using BuildPathsWriter = grpc::ServerWriter<nix::remote::BuildPathsChunk>;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler.
volatile std::sig_atomic_t nixgrpc::stopSignal = 0;

namespace {
using nixgrpc::stopSignal;

auto localHostName() -> std::string
{
    constexpr size_t maxLen = 256;
    std::array<char, maxLen> buf{};
    if (::gethostname(buf.data(), buf.size() - 1) != 0) {
        return "?";
    }
    return buf.data();
}

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    std::string storeUri;
    std::string hostName = localHostName();
    nixgrpc::Metrics & metrics;
    nixgrpc::IdleTracker & idle;
    nixgrpc::LogLevel logLevel;
    nixgrpc::Auth auth;
    nixgrpc::Backends backends;
    std::optional<nixgrpc::Farm> & farm;

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
        nixgrpc::Acl acl,
        nixgrpc::xfcc::TrustedProxies proxies,
        std::optional<nixgrpc::Farm> & farm)
        : storeUri(std::move(storeUri))
        , metrics(metrics)
        , idle(idle)
        , logLevel(logLevel)
        , auth{.acl = std::move(acl), .proxies = std::move(proxies)}
        , backends{.socketPath = std::move(socketPath)}
        , farm(farm)
    {
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
        if (farm) {
            return {grpc::StatusCode::UNIMPLEMENTED, "farm endpoint: pass --eval-store auto and build via BuildDerivation"};
        }
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
            std::vector<std::string> valid;
            for (const auto & path :
                 localStore->queryValidPaths(paths, request->substitute() ? nix::Substitute : nix::NoSubstitute)) {
                reply->add_paths(std::string(path.to_string()));
                valid.push_back(localStore->printStorePath(path));
                paths.erase(path);
            }
            if (farm) {
                // The farm answers as one store: what the cache has, every worker can serve.
                for (const auto & path : localStore->querySubstitutablePaths(paths)) {
                    reply->add_paths(std::string(path.to_string()));
                }
                // And what only we have must become so before the client relies on it.
                if (!valid.empty()) {
                    farm->push.pushWait(valid, 0, [&]() -> bool { return context->IsCancelled(); });
                }
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
            std::vector<std::string> imported;
            auto stats = nixgrpc::importPaths(
                *localStore, source, [&](const nix::ValidPathInfo & info, nix::Source & nar) -> void {
                    if (farm) {
                        substituteMissingReferences(*localStore, info);
                    }
                    localStore->addToStore(info, nar, repair, checkSigs);
                    imported.push_back(localStore->printStorePath(info.path));
                });
            if (farm && !imported.empty()) {
                farm->push.pushWait(imported, 0, [&]() -> bool { return context->IsCancelled(); });
            }
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
            for (const auto & name : request->paths()) {
                nix::StorePath const path(name);
                std::shared_ptr<const nix::ValidPathInfo> info;
                try {
                    substituteIfFarm(*localStore, path);
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
            if (farm) {
                // build-remote sends BuildDerivation and unsigned inputs only to stores that trust it.
                reply->set_trusted(rpc.caller.role == nixgrpc::Role::trusted);
                return grpc::Status::OK;
            }
            auto backend = backends.connect(*getStore());
            if (backend->info.remoteTrustsUs) {
                reply->set_trusted(*backend->info.remoteTrustsUs == nix::Trusted);
            }
            return grpc::Status::OK;
        });
    }

    // Farm outputs may live only in the cache. The build hook reads them back through us.
    void substituteIfFarm(nix::Store & store, const nix::StorePath & path)
    {
        if (farm && !store.isValidPath(path)) {
            nixcompat::ensurePath(store, path);
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

    // The drv closure and inputs may have been uploaded to, or built on, another worker.
    auto gatherInputs(
        nix::Store & localStore, const nix::StorePath & drvPath, const nix::BasicDerivation & drv, nix::Store & roots)
        -> grpc::Status
    {
        if (!localStore.isValidPath(drvPath)) {
            try {
                nixcompat::ensurePath(localStore, drvPath);
            } catch (nix::Error &) {
                return {grpc::StatusCode::NOT_FOUND, "missing: " + localStore.printStorePath(drvPath)};
            }
        }
        try {
            for (const auto & input : nixcompat::drvInputs(drv)) {
                roots.addTempRoot(input);
                nixcompat::ensurePath(localStore, input);
            }
        } catch (nix::Error & err) {
            // Another worker may still hold it locally. UNAVAILABLE makes the client retry elsewhere.
            return {grpc::StatusCode::UNAVAILABLE, hostName + ": input not substitutable: " + err.what()};
        }
        return grpc::Status::OK;
    }

    // References the client skipped were reported present, hence published, by another worker.
    static void substituteMissingReferences(nix::Store & store, const nix::ValidPathInfo & info)
    {
        for (const auto & ref : info.references) {
            if (ref != info.path && !store.isValidPath(ref)) {
                nixcompat::ensurePath(store, ref);
            }
        }
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

    // gRPC errors mean "retry this RPC", build outcomes travel as BuildResult.
    auto farmBuild(
        grpc::ServerContext & context,
        nixgrpc::Farm & frm,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        const nixgrpc::BuildEventSink & log,
        nix::BuildResult & res) -> grpc::Status
    {
        auto outPaths = staticOutputs(localStore, drv);
        if (outPaths.size() != drv.outputs.size()) {
            return {grpc::StatusCode::UNIMPLEMENTED, "farm builds need statically known output paths"};
        }
        if (!frm.healthy) {
            return {grpc::StatusCode::UNAVAILABLE, "worker low on disk space"};
        }
        auto cancelled = [&]() -> bool { return context.IsCancelled() || stopSignal != 0; };
        auto slot = nixgrpc::acquireSlot(frm.slots);
        auto claim = frm.niks3.claim(narinfoKeys(outPaths | std::views::values), narinfoKeys(nixcompat::drvInputs(drv)));
        if (claim->first(cancelled) == nixgrpc::Claim::Status::wait) {
            slot.reset();
            log({.text = hostName + ": waiting for another worker building " + std::string(drvPath.to_string())});
        }
        switch (claim->await(cancelled)) {
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
        // Blocking would keep the claim alive while every other worker waits on us.
        if (!slot) {
            slot = nixgrpc::tryAcquireSlot(frm.slots);
        }
        if (!slot) {
            return {grpc::StatusCode::UNAVAILABLE, "promoted to build but no free slot"};
        }

        // Roots inputs and outputs until publish is done.
        auto roots = openScopedStore();
        if (auto status = gatherInputs(localStore, drvPath, drv, *roots); !status.ok()) {
            return status;
        }
        for (const auto & [name, path] : outPaths) {
            roots->addTempRoot(path);
        }

        log({.text = hostName + ": building " + std::string(drvPath.to_string())});
        try {
            res = backends.storedBuild(context, localStore, drvPath, log);
        } catch (nix::Error &) {
            if (cancelled()) {
                throw nixgrpc::CancelledWait("worker shutting down, build interrupted");
            }
            throw;
        }

        if (claim->lost()) {
            return {grpc::StatusCode::UNAVAILABLE, "lost niks3 claim during build"};
        }
        if (!nixcompat::succeeded(res)) {
            // The client gets the result even if niks3 is down.
            try {
                claim->fail(nixcompat::deterministicFailureKind(res));
            } catch (nix::Error & err) {
                log({.text = std::string("reporting failure to niks3: ") + err.what()});
            }
            // ENOSPC and the like: not the build's fault, retry elsewhere.
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
            frm.push.pushWait(built, claim->token(), cancelled);
        } catch (nixgrpc::StaleClaim &) {
            claim->published();
            return {grpc::StatusCode::UNAVAILABLE, "lost niks3 claim before publishing"};
        } catch (nixgrpc::CancelledWait &) {
            throw;
        } catch (nix::Error & err) {
            return {grpc::StatusCode::UNAVAILABLE, std::string("publish failed: ") + err.what()};
        }
        claim->published();
        return grpc::Status::OK;
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

            nix::BuildResult res;
            if (farm) {
                if (mode != nix::bmNormal) {
                    return {grpc::StatusCode::INVALID_ARGUMENT, "farm endpoint only does normal builds"};
                }
                if (auto status = farmBuild(*context, *farm, *localStore, drvPath, drv, sendLogLine, res); !status.ok()) {
                    return {status.error_code(), hostName + ": " + status.error_message()};
                }
            } else {
                res = backends.proxyBuild(*context, *localStore, drvPath, drv, mode, sendLogLine);
            }

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
            if (farm) {
                return {grpc::StatusCode::UNIMPLEMENTED, "farm endpoint: build per derivation"};
            }
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
                nix::StorePath const path(request->paths(idx));
                substituteIfFarm(*localStore, path);
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
    std::optional<nixgrpc::Farm> farm;
    if (!options.farm.niks3Url.empty()) {
        farm.emplace(options.farm);
        nixgrpc::logLine(
            nixgrpc::LogLevel::info,
            {{"event", "farm_mode"}, {"niks3", options.farm.niks3Url}, {"max_jobs", std::to_string(options.farm.maxJobs)}});
    }
    NixRemoteService service(
        options.socketPath, options.storeUri, metrics, idle, options.logLevel, options.acl, options.proxies, farm);

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
