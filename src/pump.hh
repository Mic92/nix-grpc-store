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
#include <poll.h>
#include <unistd.h>

#ifdef __linux__
#  include <fcntl.h> // F_SETPIPE_SZ
#endif

#include <zstd.h>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>

#include "nix_remote.grpc.pb.h"

namespace nixgrpc {

constexpr size_t kChunkSize = 256 * 1024;
constexpr size_t kPipeSize = 1024 * 1024;

inline void growPipe([[maybe_unused]] nix::Pipe & p)
{
#ifdef F_SETPIPE_SZ
    // A larger pipe lets the worker-protocol side run ahead of the pump
    // thread, so the coalescing read below can pick up more per iteration.
    // Best-effort; capped by fs.pipe-max-size.
    ::fcntl(p.readSide.get(), F_SETPIPE_SZ, (int) kPipeSize);
#endif
}

// Blocking wait for data on `fd`, then drain everything currently available
// (up to `cap`) without waiting for more. This turns FdSink's ~32 KiB writes
// into one large Chunk during NAR streaming, but still returns immediately
// with a short batch for small request/response ops.
//
// Uses zero-timeout poll() for the drain probe rather than O_NONBLOCK because
// on the server the same fd is the daemon socket also used for writeFull() in
// the other pump; flipping the shared file-status flag would make that side
// spin in retryOnBlock under backpressure.
inline ssize_t readCoalesced(int fd, char * buf, size_t cap)
{
    struct pollfd pfd{fd, POLLIN, 0};
    size_t got = 0;
    while (got < cap) {
        // First probe blocks; subsequent probes are non-blocking so we only
        // pick up what has already arrived.
        int rc = ::poll(&pfd, 1, got == 0 ? -1 : 0);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return got ? (ssize_t) got : -1;
        }
        if (rc == 0)
            break; // drained

        ssize_t n = ::read(fd, buf + got, cap - got);
        if (n > 0) {
            got += n;
            continue;
        }
        if (n == 0)
            return got; // EOF; caller sees short (possibly zero) batch
        if (errno == EINTR)
            continue;
        return got ? (ssize_t) got : -1;
    }
    return got;
}

inline void zstdCheck(size_t rc, const char * what)
{
    if (ZSTD_isError(rc))
        throw nix::Error("zstd %s: %s", what, ZSTD_getErrorName(rc));
}

// Read from `fd` until EOF, forwarding zstd-compressed batches to the gRPC
// stream. Each batch is flushed as a self-contained decodable unit so the peer
// never stalls waiting for more compressed bytes.
template<class Stream>
inline void pumpFdToStream(int fd, Stream & stream)
{
    nix::remote::Chunk chunk;
    chunk.mutable_data()->reserve(ZSTD_compressBound(kChunkSize));
    std::string in(kChunkSize, '\0');

    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> cctx{ZSTD_createCCtx(), ZSTD_freeCCtx};
    // Level 1: even on loopback, the bytes saved through gRPC/TCP/pipe hops
    // outweigh the CPU for compressible data, and the penalty on
    // incompressible data is a few percent.
    ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, 1);

    // Compress `zin` into `chunk` with the given directive. Per the zstd API
    // contract, ZSTD_e_flush / ZSTD_e_end must be called until they return 0;
    // the encoder may still hold buffered output after input is consumed.
    // ZSTD_e_flush per batch is what makes the request/response protocol work:
    // the peer can decode each Chunk in isolation.
    auto encode = [&](ZSTD_inBuffer zin, ZSTD_EndDirective op) {
        auto * out = chunk.mutable_data();
        out->clear();
        size_t rc;
        do {
            size_t off = out->size();
            out->resize(off + ZSTD_CStreamOutSize());
            ZSTD_outBuffer zout{out->data() + off, out->size() - off, 0};
            rc = ZSTD_compressStream2(cctx.get(), &zout, &zin, op);
            zstdCheck(rc, "compress");
            out->resize(off + zout.pos);
        } while (rc != 0 || zin.pos < zin.size);
    };

    while (true) {
        ssize_t n = readCoalesced(fd, in.data(), in.size());
        if (n <= 0)
            break;
        encode({in.data(), (size_t) n, 0}, ZSTD_e_flush);
        if (!stream.Write(chunk))
            return;
    }

    encode({nullptr, 0, 0}, ZSTD_e_end);
    if (!chunk.data().empty())
        stream.Write(chunk);
}

// Read Chunks from the gRPC stream until it closes, forwarding
// zstd-decompressed bytes into `fd`.
template<class Stream>
inline void pumpStreamToFd(Stream & stream, int fd)
{
    nix::remote::Chunk chunk;
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx{ZSTD_createDCtx(), ZSTD_freeDCtx};
    std::string out;
    out.resize(ZSTD_DStreamOutSize());

    while (stream.Read(&chunk)) {
        ZSTD_inBuffer zin{chunk.data().data(), chunk.data().size(), 0};
        // Drain until input is consumed *and* the decoder produced less than a
        // full buffer — a full buffer means more output may be pending.
        while (true) {
            ZSTD_outBuffer zout{out.data(), out.size(), 0};
            zstdCheck(ZSTD_decompressStream(dctx.get(), &zout, &zin), "decompress");
            if (zout.pos)
                nix::writeFull(fd, {out.data(), zout.pos});
            if (zin.pos == zin.size && zout.pos < zout.size)
                break;
        }
    }
}

} // namespace nixgrpc
