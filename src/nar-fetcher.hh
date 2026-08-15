#pragma once
// NAR download client for the grpc store plugin.
//
// The paths recorded via recordOrder() are partitioned across N FetchNars
// streams (one TCP connection each, since a single connection cannot fill
// a high bandwidth-delay-product link), largest paths first so their
// restores start early. Each stream's reader thread decompresses tagged
// frames into per-path spool files; fetchInto() streams a path's bytes to
// the caller while later frames are still arriving, so restores overlap
// the download.

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>
#include <map>
#include <memory>
#include <mutex>
#include <nix/store/path.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
#include <zstd.h>

#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

namespace nixgrpc {

class NarFetcher
{
public:
    NarFetcher(
        std::function<std::shared_ptr<grpc::Channel>()> channelFactory,
        std::string authority,
        unsigned connections)
        : channelFactory(std::move(channelFactory))
        , authority(std::move(authority))
        , connections(std::max(1U, connections))
    {
    }

    NarFetcher(const NarFetcher &) = delete;
    NarFetcher(NarFetcher &&) = delete;
    auto operator=(const NarFetcher &) -> NarFetcher & = delete;
    auto operator=(NarFetcher &&) -> NarFetcher & = delete;

    void recordOrder(std::vector<nix::StorePath> order, std::map<nix::StorePath, uint64_t> sizes)
    {
        std::scoped_lock const lock(narMutex);
        for (auto & path : order) {
            if (!buffers.contains(path)) {
                pending.push_back(std::move(path));
            }
        }
        narSizes.merge(sizes);
    }

    void fetchInto(const nix::StorePath & path, nix::Sink & sink)
    {
        std::shared_ptr<SpoolBuffer> buffer;
        {
            std::scoped_lock const lock(narMutex);
            auto found = buffers.find(path);
            if (found == buffers.end()) {
                startPending();
                found = buffers.find(path);
            }
            if (found == buffers.end()) {
                startSession({path});
                found = buffers.find(path);
            }
            buffer = found->second;
        }
        buffer->readInto(sink);
        {
            std::scoped_lock const lock(narMutex);
            auto found = buffers.find(path);
            if (found != buffers.end() && found->second == buffer) {
                buffers.erase(found);
            }
        }
    }

    ~NarFetcher()
    {
        for (const auto & session : sessions) {
            session->ctx.TryCancel();
            if (session->thread.joinable()) {
                session->thread.join();
            }
        }
    }

private:
    class SpoolBuffer
    {
        nix::AutoCloseFD fd;
        std::mutex mutex;
        std::condition_variable grown;
        uint64_t written = 0;
        bool eof = false;
        std::exception_ptr failure;

    public:
        SpoolBuffer()
        {
            auto [tmpFd, tmpPath] = nix::createTempFile("nix-grpc-nar");
            ::unlink(tmpPath.c_str());
            fd = std::move(tmpFd);
        }

        void append(std::string_view data)
        {
            auto const size = data.size();
            uint64_t off = written;
            while (!data.empty()) {
                auto count = ::pwrite(fd.get(), data.data(), data.size(), static_cast<off_t>(off));
                if (count < 0) {
                    throw nix::SysError("writing NAR spool file");
                }
                data.remove_prefix(static_cast<size_t>(count));
                off += static_cast<uint64_t>(count);
            }
            {
                std::scoped_lock const lock(mutex);
                written += size;
            }
            grown.notify_all();
        }

        void finish()
        {
            {
                std::scoped_lock const lock(mutex);
                eof = true;
            }
            grown.notify_all();
        }

        void fail(const std::exception_ptr & cause)
        {
            {
                std::scoped_lock const lock(mutex);
                if (eof || failure) {
                    return;
                }
                failure = cause;
            }
            grown.notify_all();
        }

        void readInto(nix::Sink & sink)
        {
            uint64_t pos = 0;
            std::vector<char> buf(kChunkSize);
            while (true) {
                uint64_t avail = 0;
                {
                    std::unique_lock lock(mutex);
                    grown.wait(lock, [&] -> bool { return written > pos || eof || failure; });
                    if (failure) {
                        std::rethrow_exception(failure);
                    }
                    avail = written;
                    if (avail == pos && eof) {
                        fd.close();
                        return;
                    }
                }
                while (pos < avail) {
                    auto want = std::min<uint64_t>(buf.size(), avail - pos);
                    auto count = ::pread(fd.get(), buf.data(), want, static_cast<off_t>(pos));
                    if (count <= 0) {
                        throw nix::SysError("reading NAR spool file");
                    }
                    sink({buf.data(), static_cast<size_t>(count)});
                    pos += static_cast<uint64_t>(count);
                }
            }
        }
    };

    class Session
    {
        friend class NarFetcher;

        std::unique_ptr<nix::remote::NixRemote::Stub> stub;
        grpc::ClientContext ctx;
        std::unique_ptr<grpc::ClientReader<nix::remote::NarFrame>> reader;
        std::vector<std::shared_ptr<SpoolBuffer>> targets;
        std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx{ZSTD_createDCtx(), ZSTD_freeDCtx};
        std::thread thread;

        void run()
        {
            try {
                std::string out(ZSTD_DStreamOutSize(), '\0');
                nix::remote::NarFrame frame;
                while (reader->Read(&frame)) {
                    auto & target = targets.at(frame.path_index());
                    ZSTD_inBuffer zin{.src = frame.data().data(), .size = frame.data().size(), .pos = 0};
                    while (true) {
                        ZSTD_outBuffer zout{.dst = out.data(), .size = out.size(), .pos = 0};
                        zstdCheck(ZSTD_decompressStream(dctx.get(), &zout, &zin), "decompress");
                        if (zout.pos != 0) {
                            target->append({out.data(), zout.pos});
                        }
                        if (zin.pos == zin.size && zout.pos < zout.size) {
                            break;
                        }
                    }
                    if (frame.eof()) {
                        target->finish();
                    }
                }
                auto status = reader->Finish();
                auto cause = std::make_exception_ptr(
                    status.ok() ? nix::Error("gRPC FetchNars stream ended early")
                                : nix::Error("gRPC FetchNars failed: %s", status.error_message()));
                for (const auto & target : targets) {
                    target->fail(cause);
                }
            } catch (...) {
                for (const auto & target : targets) {
                    target->fail(std::current_exception());
                }
            }
        }
    };

    std::function<std::shared_ptr<grpc::Channel>()> channelFactory;
    std::string authority;
    unsigned connections;

    std::mutex narMutex;
    std::vector<std::unique_ptr<Session>> sessions;
    std::map<nix::StorePath, std::shared_ptr<SpoolBuffer>> buffers;
    std::vector<nix::StorePath> pending;
    std::map<nix::StorePath, uint64_t> narSizes;

    [[nodiscard]] auto narSizeOf(const nix::StorePath & path) const -> uint64_t
    {
        auto found = narSizes.find(path);
        return found == narSizes.end() ? 0 : found->second;
    }

    // Caller holds narMutex.
    void startPending()
    {
        if (pending.empty()) {
            return;
        }
        // Largest-first LPT partition, so every stream starts with its
        // biggest NAR and restores of large paths begin immediately.
        std::ranges::stable_sort(pending, [&](const auto & lhs, const auto & rhs) -> bool {
            return narSizeOf(lhs) > narSizeOf(rhs);
        });
        std::vector<std::vector<nix::StorePath>> groups(connections);
        std::vector<uint64_t> load(connections, 0);
        for (auto & path : pending) {
            auto smallest = static_cast<size_t>(
                std::ranges::min_element(load) - load.begin());
            groups.at(smallest).push_back(std::move(path));
            load.at(smallest) += narSizeOf(groups.at(smallest).back());
        }
        pending.clear();
        for (auto & group : groups) {
            if (!group.empty()) {
                startSession(group);
            }
        }
    }

    // Caller holds narMutex.
    void startSession(const std::vector<nix::StorePath> & paths)
    {
        auto session = std::make_unique<Session>();
        session->stub = nix::remote::NixRemote::NewStub(channelFactory());
        nix::remote::FetchNarsRequest request;
        for (const auto & path : paths) {
            request.add_paths(std::string(path.to_string()));
            auto buffer = std::make_shared<SpoolBuffer>();
            session->targets.push_back(buffer);
            buffers.emplace(path, std::move(buffer));
        }
        session->reader = session->stub->FetchNars(&session->ctx, request);
        if (!session->reader) {
            throw nix::Error("failed to open gRPC FetchNars stream to '%s'", authority);
        }
        session->thread = std::thread([raw = session.get()] -> void { raw->run(); });
        sessions.push_back(std::move(session));
    }
};

} // namespace nixgrpc
