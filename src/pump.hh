#pragma once
// Shared pipe ↔ gRPC-stream pump used by both the client plugin and the
// server. Factored out so throughput tuning and compression live in one place.
//
// Design constraints:
//  - The tunnelled worker protocol is request/response, so every "batch" read
//    from the pipe must be flushed to the peer immediately; buffering across
//    batches would deadlock the handshake.
//  - gRPC per-message overhead (grpc_call_start_batch / ExecCtx::Flush) was
//    ~20% of cycles in the baseline profile. Coalescing pipe reads into fewer,
//    larger Chunks amortises that.
//  - nix::makeCompressionSink(zstd) emits 16 MiB frames and cannot be flushed
//    mid-stream, so we drive libzstd directly with ZSTD_e_flush per batch.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <poll.h>
#include <unistd.h>

#ifdef __linux__
#  include <fcntl.h> // F_SETPIPE_SZ
#endif

#include <zstd.h>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>

#include "nix_remote.grpc.pb.h"

namespace nixgrpc {

constexpr size_t kChunkSize = 256UL * 1024;
constexpr size_t kPipeSize = 1024UL * 1024;

inline void growPipe([[maybe_unused]] nix::Pipe & pipe)
{
#ifdef F_SETPIPE_SZ
    // A larger pipe lets the worker-protocol side run ahead of the pump
    // thread, so the coalescing read below can pick up more per iteration.
    // Best-effort; capped by fs.pipe-max-size.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): C API.
    ::fcntl(pipe.readSide.get(), F_SETPIPE_SZ, static_cast<int>(kPipeSize));
#endif
}

// Blocking wait for data on `sourceFd`, then drain everything currently
// available (up to `buf.size()`) without waiting for more. This turns
// FdSink's ~32 KiB writes into one large Chunk during NAR streaming, but
// still returns immediately with a short batch for small request/response
// ops.
//
// Uses zero-timeout poll() for the drain probe rather than O_NONBLOCK because
// on the server the same fd is the daemon socket also used for writeFull() in
// the other pump; flipping the shared file-status flag would make that side
// spin in retryOnBlock under backpressure.
inline auto readCoalesced(int sourceFd, std::span<char> buf) -> ssize_t
{
    struct pollfd pollFd{.fd = sourceFd, .events = POLLIN, .revents = 0};
    size_t got = 0;
    while (got < buf.size()) {
        // First probe blocks; subsequent probes are non-blocking so we only
        // pick up what has already arrived.
        int const ready = ::poll(&pollFd, 1, got == 0 ? -1 : 0);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return got != 0 ? static_cast<ssize_t>(got) : -1;
        }
        if (ready == 0) {
            break; // drained
        }

        auto rest = buf.subspan(got);
        ssize_t const count = ::read(sourceFd, rest.data(), rest.size());
        if (count > 0) {
            got += static_cast<size_t>(count);
            continue;
        }
        if (count == 0) {
            return static_cast<ssize_t>(got); // EOF; caller sees short (possibly zero) batch
        }
        if (errno == EINTR) {
            continue;
        }
        return got != 0 ? static_cast<ssize_t>(got) : -1;
    }
    return static_cast<ssize_t>(got);
}

inline void zstdCheck(size_t code, const char * what)
{
    if (ZSTD_isError(code) != 0) {
        throw nix::Error("zstd %s: %s", what, ZSTD_getErrorName(code));
    }
}

// Compress `input` into `out` (appending) with the given directive, shipping
// full chunks through `ship`. Per the zstd API contract, ZSTD_e_flush /
// ZSTD_e_end must be repeated until they return 0; ZSTD_e_continue only until
// the input is consumed (its return value is a hint).
template<class ShipFn>
inline void zstdCompressInto(
    ZSTD_CCtx & cctx, std::string & out, ZSTD_inBuffer input, ZSTD_EndDirective directive, const ShipFn & ship)
{
    while (true) {
        size_t const off = out.size();
        out.resize(off + ZSTD_CStreamOutSize());
        ZSTD_outBuffer zout{.dst = &out.at(off), .size = out.size() - off, .pos = 0};
        size_t const remaining = ZSTD_compressStream2(&cctx, &zout, &input, directive);
        zstdCheck(remaining, "compress");
        out.resize(off + zout.pos);
        if (out.size() >= kChunkSize) {
            ship();
        }
        bool const done = directive == ZSTD_e_continue ? input.pos == input.size : remaining == 0;
        if (done) {
            break;
        }
    }
}

// Read from `sourceFd` until EOF, forwarding zstd-compressed batches to the
// gRPC stream. Each batch is flushed as a self-contained decodable unit so
// the peer never stalls waiting for more compressed bytes.
// Returns the number of uncompressed bytes forwarded.
template<class Stream>
inline auto pumpFdToStream(int sourceFd, Stream & stream) -> uint64_t
{
    uint64_t total = 0;
    nix::remote::Chunk chunk;
    chunk.mutable_data()->reserve(ZSTD_compressBound(kChunkSize));
    std::string input(kChunkSize, '\0');

    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> const cctx{ZSTD_createCCtx(), ZSTD_freeCCtx};
    // Level 1: even on loopback, the bytes saved through gRPC/TCP/pipe hops
    // outweigh the CPU for compressible data, and the penalty on
    // incompressible data is a few percent.
    ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, 1);

    // ZSTD_e_flush per batch is what makes the request/response protocol
    // work: the peer can decode each Chunk in isolation.
    auto encode = [&](ZSTD_inBuffer zin, ZSTD_EndDirective directive) -> void {
        auto & out = *chunk.mutable_data();
        out.clear();
        zstdCompressInto(*cctx, out, zin, directive, []() -> void {});
    };

    while (true) {
        ssize_t const count = readCoalesced(sourceFd, input);
        if (count <= 0) {
            break;
        }
        total += static_cast<uint64_t>(count);
        encode({.src = input.data(), .size = static_cast<size_t>(count), .pos = 0}, ZSTD_e_flush);
        if (!stream.Write(chunk)) {
            return total;
        }
    }

    encode({.src = nullptr, .size = 0, .pos = 0}, ZSTD_e_end);
    if (!chunk.data().empty()) {
        stream.Write(chunk);
    }
    return total;
}

// Read Chunks from the gRPC stream until it closes, forwarding
// zstd-decompressed bytes into `sinkFd`.
// Returns the number of decompressed bytes forwarded.
template<class Stream>
inline auto pumpStreamToFd(Stream & stream, int sinkFd) -> uint64_t
{
    uint64_t total = 0;
    nix::remote::Chunk chunk;
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> const dctx{ZSTD_createDCtx(), ZSTD_freeDCtx};
    std::string out;
    out.resize(ZSTD_DStreamOutSize());

    while (stream.Read(&chunk)) {
        ZSTD_inBuffer zin{.src = chunk.data().data(), .size = chunk.data().size(), .pos = 0};
        // Drain until input is consumed *and* the decoder produced less than a
        // full buffer — a full buffer means more output may be pending.
        while (true) {
            ZSTD_outBuffer zout{.dst = out.data(), .size = out.size(), .pos = 0};
            zstdCheck(ZSTD_decompressStream(dctx.get(), &zout, &zin), "decompress");
            if (zout.pos != 0) {
                nix::writeFull(sinkFd, {out.data(), zout.pos});
                total += zout.pos;
            }
            if (zin.pos == zin.size && zout.pos < zout.size) {
                break;
            }
        }
    }
    return total;
}

// Sink that zstd-compresses written data into gRPC messages of type `Msg`
// (must have a `data` bytes field) sent through the writer. Unlike the tunnel
// pump above there is no per-batch flush: the Sink's lifetime is one zstd
// stream, so the window spans all paths of a bulk import. Call finish()
// before WritesDone().
template<class Writer, class Msg>
class ZstdWriterSink : public nix::Sink
{
    Writer * writer;
    Msg msg;
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> cctx{ZSTD_createCCtx(), ZSTD_freeCCtx};

    void ship()
    {
        if (msg.data().empty()) {
            return;
        }
        if (!writer->Write(msg)) {
            throw nix::Error("gRPC stream closed by peer during bulk import");
        }
        msg.mutable_data()->clear();
    }

    void compress(ZSTD_inBuffer input, ZSTD_EndDirective directive)
    {
        zstdCompressInto(*cctx, *msg.mutable_data(), input, directive, [this]() -> void { ship(); });
    }

public:
    explicit ZstdWriterSink(Writer & writer)
        : writer(&writer)
    {
        ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, 1);
        // The buffer is reused across ship() calls; avoid regrowing it.
        msg.mutable_data()->reserve(kChunkSize + ZSTD_CStreamOutSize());
    }

    void operator()(std::string_view data) override
    {
        compress({.src = data.data(), .size = data.size(), .pos = 0}, ZSTD_e_continue);
    }

    // Make everything written so far decodable by the peer without ending the
    // zstd stream.
    void flush()
    {
        compress({.src = nullptr, .size = 0, .pos = 0}, ZSTD_e_flush);
        ship();
    }

    void finish()
    {
        compress({.src = nullptr, .size = 0, .pos = 0}, ZSTD_e_end);
        ship();
    }
};

// Inverse of ZstdWriterSink: reads `Msg`s from the reader and yields the
// decompressed byte stream.
template<class Reader, class Msg>
class ZstdReaderSource : public nix::Source
{
    Reader * reader;
    Msg msg;
    // Always points into msg.data(); reset whenever msg is replaced.
    ZSTD_inBuffer zin{.src = nullptr, .size = 0, .pos = 0};
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx{ZSTD_createDCtx(), ZSTD_freeDCtx};

    static auto makeInitialMsg(std::string initial) -> Msg
    {
        Msg result;
        *result.mutable_data() = std::move(initial);
        return result;
    }

public:
    // `initial`: compressed bytes already read from the stream (the first
    // message may carry flags alongside data).
    ZstdReaderSource(Reader & reader, std::string initial)
        : reader(&reader)
        , msg(makeInitialMsg(std::move(initial)))
        , zin{.src = msg.data().data(), .size = msg.data().size(), .pos = 0}
    {
    }

    auto read(char * data, size_t len) -> size_t override
    {
        while (true) {
            // Always run the decoder first: it may hold buffered output from a
            // previous call whose destination buffer was smaller than the
            // decompressed data, even when all input has been consumed.
            ZSTD_outBuffer zout{.dst = data, .size = len, .pos = 0};
            zstdCheck(ZSTD_decompressStream(dctx.get(), &zout, &zin), "decompress");
            if (zout.pos != 0) {
                return zout.pos;
            }
            if (zin.pos == zin.size) {
                if (!reader->Read(&msg)) {
                    throw nix::EndOfFile("gRPC stream ended");
                }
                zin = {.src = msg.data().data(), .size = msg.data().size(), .pos = 0};
            }
        }
    }
};

} // namespace nixgrpc
