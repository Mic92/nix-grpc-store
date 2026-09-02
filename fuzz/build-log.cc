// Worker-protocol stderr stream from the backend daemon.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>

#include <nix/util/error.hh>
#include <nix/util/serialise.hh>

#include "../src/build-log.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    nix::StringSource source(nixgrpc::fuzz::view(raw, size));
    try {
        nixgrpc::relayBuildLog(source, [](std::string line) -> void { static_cast<void>(line); });
    } catch (nixgrpc::BackendError &) {
    } catch (std::exception & err) { // mirrors NixRemoteService::guarded()
        nixgrpc::fuzz::rejected(err);
    }
    return 0;
}
