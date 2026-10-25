// x-forwarded-client-cert parsing: any bytes in, a sane identity or none out.

#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "../src/daemon/xfcc.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    auto peer = nixgrpc::xfcc::elementPeer(nixgrpc::fuzz::view(raw, size));
    if (peer.commonName && !peer.commonName->empty() && !nixgrpc::xfcc::detail::saneCommonName(*peer.commonName)) {
        std::abort();
    }
    if (peer.spiffeId && !(peer.present() && nixgrpc::saneSpiffeId(*peer.spiffeId))) {
        std::abort();
    }
    return 0;
}
