// Client side of FetchNars: server-controlled NarFrame path_index/eof/data
// demultiplexed into per-path spools. Input is a list of frames
// [index][flags][len][payload]; flag bit 1 sends payload through a shared
// zstd stream (as a real server would), otherwise raw bytes hit the decoder.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include <zstd.h>

#include <nix/util/error.hh>

#include "nix_remote.pb.h"
#include "../src/nar-fetcher.hh"
#include "../src/pump.hh"
#include "support.hh"

namespace {

struct FrameReader
{
    std::vector<nix::remote::NarFrame> frames;
    size_t next = 0;

    auto Read(nix::remote::NarFrame * msg) -> bool
    {
        if (next == frames.size()) {
            return false;
        }
        *msg = std::move(frames[next++]);
        return true;
    }
};

} // namespace

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    using namespace nixgrpc::fuzz;
    constexpr unsigned kTargets = 3;

    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> const cctx{ZSTD_createCCtx(), ZSTD_freeCCtx};
    FrameReader reader;
    while (size > 0) {
        nix::remote::NarFrame frame;
        // Occasionally out of range on purpose.
        frame.set_path_index(takeByte(raw, size) % (kTargets + 1));
        uint8_t const flags = takeByte(raw, size);
        frame.set_eof((flags & 1U) != 0);
        size_t const len = std::min<size_t>(takeByte(raw, size), size);
        auto payload = view(raw, len);
        raw += len;
        size -= len;
        if ((flags & 2U) != 0) {
            nixgrpc::zstdCompressInto(
                *cctx,
                *frame.mutable_data(),
                {.src = payload.data(), .size = payload.size(), .pos = 0},
                (flags & 4U) != 0 ? ZSTD_e_end : ZSTD_e_flush,
                []() -> void {});
        } else {
            frame.set_data(std::string(payload));
        }
        reader.frames.push_back(std::move(frame));
    }

    std::vector<std::shared_ptr<nixgrpc::NarSpool>> targets;
    for (unsigned idx = 0; idx < kTargets; ++idx) {
        targets.push_back(std::make_shared<nixgrpc::NarSpool>());
    }
    try {
        nixgrpc::demuxNarFrames(reader, targets);
    } catch (std::exception & err) { // mirrors NarFetcher::Session::run()
        rejected(err);
    }
    return 0;
}
