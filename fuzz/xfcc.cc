// x-forwarded-client-cert parsing: any bytes in, a sane CN or nullopt out.

#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "../src/xfcc.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    auto name = nixgrpc::xfcc::subjectCommonName(nixgrpc::fuzz::view(raw, size));
    if (name && !nixgrpc::xfcc::detail::saneCommonName(*name)) {
        std::abort();
    }
    return 0;
}
