#pragma once
// (De)serialisation of remote::PathInfo, which carries a store path plus the
// worker-protocol 1.16 UnkeyedValidPathInfo bytes. Server encodes, client
// decodes; both live here so the fuzzer exercises the exact client code.

#include <memory>
#include <string>
#include <utility>

#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-dir-config.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/serialise.hh>

#include "nix-compat.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

inline void encodePathInfo(const nix::StoreDirConfig & store, const nix::ValidPathInfo & info, nix::remote::PathInfo * out)
{
    out->set_path(std::string(info.path.to_string()));
    nix::StringSink sink;
    nix::WorkerProto::Serialise<nix::UnkeyedValidPathInfo>::write(
        store,
        nix::WorkerProto::WriteConn{.to = sink, .version = nixcompat::infoProtocolVersion()},
        static_cast<const nix::UnkeyedValidPathInfo &>(info));
    *out->mutable_info() = std::move(sink.s);
}

inline auto decodePathInfo(const nix::StoreDirConfig & store, const nix::remote::PathInfo & entry)
    -> std::pair<nix::StorePath, std::shared_ptr<const nix::ValidPathInfo>>
{
    nix::StorePath path(entry.path());
    nix::StringSource source(entry.info());
    auto info = nix::WorkerProto::Serialise<nix::UnkeyedValidPathInfo>::read(
        store, nix::WorkerProto::ReadConn{.from = source, .version = nixcompat::infoProtocolVersion()});
    auto full = std::make_shared<const nix::ValidPathInfo>(path, std::move(info));
    return {std::move(path), std::move(full)};
}

} // namespace nixgrpc
