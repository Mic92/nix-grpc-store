// Peer-supplied strings/blobs that go straight into libnixstore parsers. The
// first byte selects which one, so one corpus covers all of them.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <vector>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/store/worker-protocol-impl.hh> // IWYU pragma: keep
#include <nix/util/error.hh>
#include <nix/util/serialise.hh>

#include "nix_remote.pb.h"
#include "../src/nix-compat.hh"
#include "../src/path-info-wire.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    using namespace nixgrpc::fuzz;
    auto & dummy = store();
    uint8_t const which = takeByte(raw, size) % 6;
    auto input = view(raw, size);
    // ReadConn holds the version by reference.
    auto const version = nixcompat::buildProtocolVersion();
    auto readConn = [&](nix::Source & source) -> nix::WorkerProto::ReadConn {
        return nix::WorkerProto::ReadConn{.from = source, .version = version};
    };
    try {
        switch (which) {
        case 0:
            // QueryValidPaths/QueryPathInfos/FetchNars requests and replies.
            static_cast<void>(nix::StorePath(input));
            break;
        case 1:
            // QueryMissing/BuildPaths targets.
            static_cast<void>(nix::DerivedPath::parse(dummy, input));
            break;
        case 2: {
            // BuildDerivation: drv_path, then drv.
            auto sep = input.find('\0');
            nix::StorePath const drvPath(input.substr(0, sep));
            nix::StringSource source(sep == std::string_view::npos ? "" : input.substr(sep + 1));
            nix::BasicDerivation drv;
            nixcompat::readDrv(source, dummy, drv, nix::Derivation::nameFromPath(drvPath));
            break;
        }
        case 3: {
            // QueryPathInfos reply / build output infos on the client.
            nix::remote::PathInfo entry;
            if (entry.ParseFromString(std::string(input))) {
                static_cast<void>(nixgrpc::decodePathInfo(dummy, entry));
            }
            break;
        }
        case 4: {
            nix::StringSource source(input);
            static_cast<void>(nix::WorkerProto::Serialise<nix::BuildResult>::read(dummy, readConn(source)));
            break;
        }
        case 5: {
            nix::StringSource source(input);
            static_cast<void>(
                nix::WorkerProto::Serialise<std::vector<nix::KeyedBuildResult>>::read(dummy, readConn(source)));
            break;
        }
        default:
            break;
        }
    } catch (std::exception & err) { // mirrors NixRemoteService::guarded()
        rejected(err);
    }
    return 0;
}
