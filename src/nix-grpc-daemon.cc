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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/globals.hh>
#include <nix/util/logging.hh>
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
#include <nix/util/util.hh>

#include "acl.hh"
#include "logfmt.hh"
#include "metrics.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

using GrpcStream = grpc::ServerReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;
using AddMultipleReader = grpc::ServerReader<nix::remote::AddMultipleChunk>;
using NarsStream = grpc::ServerReaderWriter<nix::remote::NarChunk, nix::remote::NarRequest>;
using BuildWriter = grpc::ServerWriter<nix::remote::BuildDerivationChunk>;
using BuildPathsWriter = grpc::ServerWriter<nix::remote::BuildPathsChunk>;

namespace {

class NixRemoteService final : public nix::remote::NixRemote::Service
{
    std::string socketPath;
    std::string storeUri;
    nixgrpc::Metrics & metrics;
    nixgrpc::LogLevel logLevel;
    nixgrpc::Acl acl;

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

    // gRPC aborts the process if a handler lets an exception escape.
    template<typename F>
    static auto guarded(F && func) -> grpc::Status
    {
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
        auto const detail = role
                                ? "role '" + std::string(nixgrpc::roleName(*role)) + "' may not call "
                                      + std::string(method)
                                : "no access rule matches certificate CN '" + commonName + "'";
        return {grpc::StatusCode::PERMISSION_DENIED, detail};
    }

public:
    NixRemoteService(
        std::string socketPath,
        std::string storeUri,
        nixgrpc::Metrics & metrics,
        nixgrpc::LogLevel logLevel,
        nixgrpc::Acl acl)
        : socketPath(std::move(socketPath))
        , storeUri(std::move(storeUri))
        , metrics(metrics)
        , logLevel(logLevel)
        , acl(std::move(acl))
    {
    }

    auto Connect(grpc::ServerContext * context, GrpcStream * stream) -> grpc::Status override
    {
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
            auto localStore = getStore();

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

    // Consumes the worker-protocol stderr stream up to STDERR_LAST,
    // forwarding plain build log lines.
    static void appendOutputInfo(nix::Store & store, nix::remote::PathInfo * out, const nix::StorePath & outPath)
    {
        auto info = store.queryPathInfo(outPath);
        out->set_path(std::string(info->path.to_string()));
        nix::StringSink sink;
        nix::WorkerProto::Serialise<nix::UnkeyedValidPathInfo>::write(
            store,
            nix::WorkerProto::WriteConn{.to = sink, .version = nixcompat::infoProtocolVersion()},
            static_cast<const nix::UnkeyedValidPathInfo &>(*info));
        *out->mutable_info() = std::move(sink.s);
    }

    static void relayBuildLog(nix::Source & source, const std::function<void(std::string)> & sendLogLine)
    {
        auto readLogFields = [&]() -> std::vector<std::string> {
            std::vector<std::string> fields;
            auto count = nix::readNum<uint64_t>(source);
            for (uint64_t idx = 0; idx < count; ++idx) {
                if (nix::readNum<uint64_t>(source) == 0) {
                    nix::readNum<uint64_t>(source);
                    fields.emplace_back();
                } else {
                    fields.push_back(nix::readString(source));
                }
            }
            return fields;
        };
        while (true) {
            auto msg = nix::readNum<uint64_t>(source);
            if (msg == STDERR_NEXT) {
                sendLogLine(nix::chomp(nix::readString(source)));
            } else if (msg == STDERR_START_ACTIVITY) {
                nix::readNum<uint64_t>(source);
                nix::readNum<uint64_t>(source);
                nix::readNum<uint64_t>(source);
                nix::readString(source);
                readLogFields();
                nix::readNum<uint64_t>(source);
            } else if (msg == STDERR_STOP_ACTIVITY) {
                nix::readNum<uint64_t>(source);
            } else if (msg == STDERR_RESULT) {
                nix::readNum<uint64_t>(source);
                auto type = nix::readNum<uint64_t>(source);
                auto fields = readLogFields();
                if (type == nix::resBuildLogLine && !fields.empty()) {
                    sendLogLine(std::move(fields.front()));
                }
            } else if (msg == STDERR_ERROR) {
                throw nix::readError(source);
            } else if (msg == STDERR_LAST) {
                break;
            } else {
                throw nix::Error("unexpected worker protocol message from backend");
            }
        }
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

            auto const protocol = nixcompat::buildProtocolVersion();
            if (request->protocol() != nixcompat::kBuildProtocolWire
                || request->build_mode() > static_cast<uint32_t>(nix::bmCheck)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "unsupported build protocol or mode"};
            }
            auto localStore = getStore();

            auto backend = connectBackend(*localStore);
            auto & conn = backend->conn;
            if (nixcompat::protocolWire(conn.protoVersion) != nixcompat::kBuildProtocolWire) {
                return {grpc::StatusCode::FAILED_PRECONDITION, "backend daemon is too old"};
            }
            auto sendLogLine = [&](std::string line) -> void {
                nix::remote::BuildDerivationChunk chunk;
                *chunk.mutable_log_line() = std::move(line);
                writer->Write(chunk);
            };
            // The daemon opens every connection with a stderr work block.
            relayBuildLog(conn.from, sendLogLine);

            nix::StorePath const drvPath(request->drv_path());
            nix::StringSource drvSource(request->drv());
            nix::BasicDerivation drv;
            nixcompat::readDrv(drvSource, *localStore, drv, nix::Derivation::nameFromPath(drvPath));
            bool daemonException = false;
            conn.putBuildDerivationRequest(
                *localStore,
                &daemonException,
                drvPath,
                drv,
                static_cast<nix::BuildMode>(request->build_mode()));
            conn.to.flush();

            relayBuildLog(conn.from, sendLogLine);

            auto res = nix::WorkerProto::Serialise<nix::BuildResult>::read(
                *localStore, nix::WorkerProto::ReadConn{.from = conn.from, .version = protocol});

            nix::remote::BuildDerivationChunk chunk;
            auto * done = chunk.mutable_done();
            {
                nix::StringSink sink;
                nix::WorkerProto::Serialise<nix::BuildResult>::write(
                    *localStore, nix::WorkerProto::WriteConn{.to = sink, .version = protocol}, res);
                *done->mutable_result() = std::move(sink.s);
            }
            uint64_t outputs = 0;
            nixcompat::forBuiltOutputs(res, [&](const nix::StorePath & outPath) -> void {
                appendOutputInfo(*localStore, done->add_outputs(), outPath);
                ++outputs;
            });
            writer->Write(chunk);

            nixgrpc::logLine(
                nixgrpc::LogLevel::info,
                {{"event", "rpc"},
                 {"method", "BuildDerivation"},
                 {"cn", commonName},
                 {"peer", context->peer()},
                 {"duration_s", secondsSince(start)},
                 {"drv", std::string(drvPath.to_string())},
                 {"outputs", std::to_string(outputs)}});
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
            relayBuildLog(conn.from, sendLogLine);

            conn.to << nix::WorkerProto::Op::BuildPathsWithResults;
            nix::WorkerProto::write(
                *localStore,
                nix::WorkerProto::WriteConn{.to = conn.to, .version = conn.protoVersion},
                targets);
            conn.to << request->build_mode();
            conn.to.flush();

            relayBuildLog(conn.from, sendLogLine);

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

    auto NarsFromPaths(grpc::ServerContext * context, NarsStream * stream) -> grpc::Status override
    {
        return guarded([&]() -> grpc::Status {
            auto const cert = nixgrpc::clientCommonName(*context);
            auto const commonName = cert.value_or("-");
            if (auto status = authorize(cert, "NarsFromPaths", nixgrpc::Role::readOnly); !status.ok()) {
                return status;
            }
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
    // Store URI for the native bulk RPCs. Defaults to the proxy socket.
    std::string storeUri;
    std::string tlsCert;
    std::string tlsKey;
    std::string clientCA;
    std::string metricsListen;
    nixgrpc::LogLevel logLevel = nixgrpc::LogLevel::info;
    nixgrpc::Acl acl;
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
    if ((options.acl.active() || options.acl.anonymousRole()) && options.clientCA.empty()) {
        // Without mTLS every client's CN is "-".
        throw nix::Error("--allow/--allow-anonymous requires --client-ca");
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
    // (mTLS); --allow-anonymous downgrades "require" to "verify if
    // presented" and the ACL sees cert-less clients as CN "-".
    auto clientCertRequest = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
    if (!options.clientCA.empty()) {
        clientCertRequest = options.acl.anonymousRole()
                                ? GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY
                                : GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
    }
    grpc::SslServerCredentialsOptions ssl(clientCertRequest);
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
    if (options.storeUri.empty()) {
        options.storeUri = "unix://" + options.socketPath;
    }
    NixRemoteService service(options.socketPath, options.storeUri, metrics, options.logLevel, options.acl);

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
