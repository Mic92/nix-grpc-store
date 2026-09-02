// AddMultipleToStore body as the daemon sees it: the fuzz input is the
// *plaintext* framing (count, ValidPathInfos, NARs), which we compress with
// ZstdWriterSink at input-chosen flush points and feed back through
// ZstdReaderSource in input-chosen message sizes. That way libFuzzer mutates
// the parser input directly while the real decode path (short reads across
// message boundaries) is still exercised.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string_view>

#include <nix/util/archive.hh>
#include <nix/util/error.hh>
#include <nix/util/fs-sink.hh>
#include <nix/util/serialise.hh>

#include "nix_remote.pb.h"
#include "../src/import-paths.hh"
#include "../src/pump.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    using namespace nixgrpc::fuzz;
    using Msg = nix::remote::AddMultipleChunk;
    auto & dummy = store();

    size_t const msgSize = (takeByte(raw, size) + 1U) * 16U;
    size_t const flushEvery = (takeByte(raw, size) + 1U) * 8U;
    auto plain = view(raw, size);

    CollectWriter<Msg> collected;
    {
        nixgrpc::ZstdWriterSink<CollectWriter<Msg>, Msg> sink(collected);
        while (!plain.empty()) {
            auto piece = plain.substr(0, flushEvery);
            sink(piece);
            sink.flush();
            plain.remove_prefix(piece.size());
        }
        sink.finish();
    }

    ChunkReader<Msg> reader{.data = collected.out, .chunk = msgSize};
    nixgrpc::ZstdReaderSource<ChunkReader<Msg>, Msg> source(reader, {});
    try {
        nixgrpc::importPaths(dummy, source, [](const nix::ValidPathInfo &, nix::Source & nar) -> void {
            // The daemon hands this to addToStore(), which parses a NAR.
            nix::NullFileSystemObjectSink null;
            nix::parseDump(null, nar);
        });
    } catch (std::exception & err) { // mirrors NixRemoteService::guarded()
        rejected(err);
    }
    return 0;
}
