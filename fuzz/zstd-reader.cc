// ZstdReaderSource decodes peer-controlled bytes: it must throw, not crash.

#include <array>
#include <cstddef>
#include <cstdint>

#include <nix/util/error.hh>

#include "nix_remote.pb.h"
#include "../src/pump.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    using namespace nixgrpc::fuzz;
    ChunkReader<nix::remote::Chunk> reader{.data = {}, .chunk = takeByte(raw, size) + 1U};
    reader.data = view(raw, size);
    nixgrpc::ZstdReaderSource<decltype(reader), nix::remote::Chunk> source(reader, {});
    std::array<char, 4096> buf{};
    try {
        while (true) {
            source.read(buf.data(), buf.size());
        }
    } catch (nix::EndOfFile &) {
    } catch (nix::Error & err) {
        rejected(err);
    }
    return 0;
}
