// Worker-protocol connections to the local nix-daemon for builds.

#include "backend.hh"

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <map>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>


#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/outputs-spec.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
// Generic definitions for the std::vector serialiser wrappers used by
// BuildPaths, which libnixstore does not instantiate explicitly.
#include <nix/store/worker-protocol-impl.hh> // IWYU pragma: keep
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/unix-domain-socket.hh>

#include "build-log.hh"
#include "nix-compat.hh"
#include "nix_remote.pb.h"
#include "path-info-wire.hh"

namespace nixgrpc {

namespace {
// No build hook: it would bypass the scheduler.
void buildLocally(Backend::Conn & conn)
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
} // namespace

void Backend::cancelWith(Cancelled cancelled)
{
    canceller = std::jthread([cancelled = std::move(cancelled), this](const std::stop_token & stop) -> void {
        constexpr std::chrono::milliseconds poll{200};
        while (!stop.stop_requested() && !cancelled()) {
            std::this_thread::sleep_for(poll);
        }
        if (!stop.stop_requested()) {
            ::shutdown(sock.get(), SHUT_RDWR);
        }
    });
}

auto Backends::connect(nix::Store & store) const -> std::unique_ptr<Backend>
{
    auto backend = std::make_unique<Backend>();
    backend->sock = nix::connect(std::filesystem::path{socketPath});
    backend->conn.to = nix::FdSink(backend->sock.get());
    backend->conn.from = nix::FdSource(backend->sock.get());
    backend->conn.protoVersion = nixcompat::handshakeCompat(backend->conn, nixcompat::buildProtocolVersion());
    backend->info = backend->conn.postHandshake(store);
    return backend;
}

auto Backends::forBuild(Cancelled cancelled, nix::Store & localStore) const -> std::unique_ptr<Backend>
{
    auto backend = connect(localStore);
    backend->cancelWith(std::move(cancelled));
    if (nixcompat::protocolWire(backend->conn.protoVersion) != nixcompat::kBuildProtocolWire) {
        throw nix::Error("backend daemon is too old");
    }
    return backend;
}

namespace {
auto buildPathsVia(
    Backend & backend,
    nix::Store & localStore,
    const std::vector<nix::DerivedPath> & targets,
    nix::BuildMode mode,
    const BuildEventSink & sendLogLine) -> std::vector<nix::KeyedBuildResult>
{
    auto & conn = backend.conn;
    // The daemon opens every connection with a stderr work block.
    relayBuildLog(conn.from, sendLogLine);
    conn.to << nix::WorkerProto::Op::BuildPathsWithResults;
    nix::WorkerProto::write(
        localStore, nix::WorkerProto::WriteConn{.to = conn.to, .version = conn.protoVersion}, targets);
    conn.to << static_cast<uint32_t>(mode);
    conn.to.flush();
    relayBuildLog(conn.from, sendLogLine);
    return nix::WorkerProto::Serialise<std::vector<nix::KeyedBuildResult>>::read(
        localStore, nix::WorkerProto::ReadConn{.from = conn.from, .version = nixcompat::buildProtocolVersion()});
}
} // namespace

auto Backends::storedBuild(
    Cancelled cancelled,
    nix::Store & localStore,
    const nix::StorePath & drvPath,
    nix::BuildMode mode,
    const BuildEventSink & sendLogLine) const -> nix::BuildResult
{
    auto backend = forBuild(std::move(cancelled), localStore);
    buildLocally(backend->conn);
    std::vector<nix::DerivedPath> const targets{nix::DerivedPath::Built{
        .drvPath = nix::makeConstantStorePathRef(drvPath), .outputs = nix::OutputsSpec::All{}}};
    auto results = buildPathsVia(*backend, localStore, targets, mode, sendLogLine);
    if (results.size() != 1) {
        throw nix::Error("nix-daemon returned %d results for one derivation", results.size());
    }
    return std::move(results.front());
}

void encodeResult(nix::Store & localStore, nix::BuildResult & res, nix::remote::BuildDerivationDone * done)
{
    nix::StringSink sink;
    nix::WorkerProto::Serialise<nix::BuildResult>::write(
        localStore, nix::WorkerProto::WriteConn{.to = sink, .version = nixcompat::buildProtocolVersion()}, res);
    *done->mutable_result() = std::move(sink.s);
    nixcompat::forBuiltOutputs(res, [&](const nix::StorePath & outPath) -> void {
        if (localStore.isValidPath(outPath)) {
            encodePathInfo(localStore, *localStore.queryPathInfo(outPath), done->add_outputs());
        }
    });
}

} // namespace nixgrpc
