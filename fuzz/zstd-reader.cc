// ZstdReaderSource decodes peer-controlled bytes: it must throw, not crash.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <nix/util/error.hh>

#include "nix_remote.pb.h"
#include "../src/pump.hh"

namespace {

struct FuzzReader
{
    std::string_view data;
    size_t chunk;

    auto Read(nix::remote::Chunk * msg) -> bool
    {
        if (data.empty()) {
            return false;
        }
        size_t const n = std::min(chunk, data.size());
        msg->set_data(std::string(data.substr(0, n)));
        data.remove_prefix(n);
        return true;
    }
};

} // namespace

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    if (size < 1) {
        return 0;
    }
    // First byte picks the message size so chunk boundaries are fuzzed too.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    FuzzReader reader{
        .data = {reinterpret_cast<const char *>(raw + 1), size - 1},
        .chunk = static_cast<size_t>(raw[0]) + 1,
    };
    nixgrpc::ZstdReaderSource<FuzzReader, nix::remote::Chunk> source(reader, {});
    std::array<char, 4096> buf{};
    try {
        while (true) {
            source.read(buf.data(), buf.size());
        }
    } catch (nix::Error &) {
    }
    return 0;
}
