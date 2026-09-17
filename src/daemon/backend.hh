#pragma once
// One worker-protocol connection to the local nix-daemon for a build, with the
// stderr stream relayed to the gRPC client.

#include <csignal>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/server_context.h>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol-connection.hh>
#include <nix/util/file-descriptor.hh>

#include "build-log.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler, defined in main.
extern volatile std::sig_atomic_t stopSignal;

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
    // Declared last so it joins before sock closes.
    std::jthread canceller;

    // A silent build sends nothing to notice a cancelled RPC on, so poll
    // and drop the connection, which makes nix-daemon kill the build.
    void cancelWith(grpc::ServerContext & context);
};

struct Backends
{
    std::string socketPath;

    [[nodiscard]] auto connect(nix::Store & store) const -> std::unique_ptr<Backend>;
    // Build-capable connection: cancellable, protocol checked.
    [[nodiscard]] auto forBuild(grpc::ServerContext & context, nix::Store & localStore) const
        -> std::unique_ptr<Backend>;

    // Inline drv through wopBuildDerivation (standalone mode).
    [[nodiscard]] auto proxyBuild(
        grpc::ServerContext & context,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        const nix::BasicDerivation & drv,
        nix::BuildMode mode,
        const BuildEventSink & sendLogLine) const -> nix::BuildResult;

    static auto buildPathsVia(
        Backend & backend,
        nix::Store & localStore,
        const std::vector<nix::DerivedPath> & targets,
        nix::BuildMode mode,
        const BuildEventSink & sendLogLine) -> std::vector<nix::KeyedBuildResult>;
};

template<typename Chunk>
auto toChunk(BuildEvent event) -> Chunk
{
    Chunk chunk;
    if (event.kind == BuildEvent::Kind::phase) {
        *chunk.mutable_phase() = std::move(event.text);
    } else {
        *chunk.mutable_log_line() = std::move(event.text);
    }
    return chunk;
}

// Wire BuildResult plus PathInfo of every valid output.
void encodeResult(nix::Store & localStore, nix::BuildResult & res, nix::remote::BuildDerivationDone * done);

} // namespace nixgrpc
