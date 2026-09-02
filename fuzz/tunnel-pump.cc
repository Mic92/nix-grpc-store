// Connect tunnel: pumpStreamToFd decodes client-controlled zstd Chunks into
// the daemon socket.

#include <cstddef>
#include <cstdint>

#include <fcntl.h>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>

#include "nix_remote.pb.h"
#include "../src/pump.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    using namespace nixgrpc::fuzz;
    static nix::AutoCloseFD devNull{::open("/dev/null", O_WRONLY | O_CLOEXEC)};
    ChunkReader<nix::remote::Chunk> reader{.data = {}, .chunk = (takeByte(raw, size) + 1U) * 64U};
    reader.data = view(raw, size);
    try {
        nixgrpc::pumpStreamToFd(reader, devNull.get());
    } catch (nix::Error & err) {
        rejected(err);
    }
    return 0;
}
