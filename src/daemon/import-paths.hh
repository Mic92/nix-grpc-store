#pragma once
// Body framing of the AddMultipleToStore RPC (after zstd), shared between the
// daemon handler and its fuzzer.

#include <cstdint>

#include <nix/store/path-info.hh>
#include <nix/store/store-dir-config.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/serialise.hh>

#include "nix-compat.hh"

namespace nixgrpc {

struct ImportStats
{
    uint64_t paths = 0;
    uint64_t narBytes = 0;
};

// Same framing as nix::WorkerProto::Op::AddMultipleToStore: a count, then per
// path a ValidPathInfo (protocol 1.16) followed by narSize NAR bytes. `addOne`
// receives the info and a Source positioned at the NAR; whatever it leaves
// unread up to narSize is skipped.
template<class Fn>
inline auto importPaths(const nix::StoreDirConfig & store, nix::Source & source, const Fn & addOne) -> ImportStats
{
    ImportStats stats;
    auto const expected = nix::readNum<uint64_t>(source);
    for (uint64_t idx = 0; idx < expected; ++idx) {
        auto info = nix::WorkerProto::Serialise<nix::ValidPathInfo>::read(
            store, nix::WorkerProto::ReadConn{.from = source, .version = nixcompat::infoProtocolVersion()});
        info.ultimate = false;
        nixcompat::EnsureRead wrapper{source, info.narSize};
        addOne(info, static_cast<nix::Source &>(wrapper));
        wrapper.finish();
        stats.narBytes += info.narSize;
        ++stats.paths;
    }
    return stats;
}

} // namespace nixgrpc
