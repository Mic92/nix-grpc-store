#pragma once
// NAR download client for the grpc store plugin. Recorded paths are
// partitioned largest-first across N FetchNars streams, one TCP
// connection each. Reader threads decompress tagged frames into per-path
// spool files while fetchInto() streams them out, so restores overlap
// the download.

#include <algorithm>
#include <atomic>
#include <chrono>
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
#include <ranges>
#include <nix/store/path.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>
#include <nix/util/logging.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signals.hh>
#include <nix/util/util.hh>
#include <string>
#include <string_view>
#include <thread>
#include <stop_token>
#include <unistd.h>
#include <utility>
#include <vector>
#include <zstd.h>

#include "channel.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"
#include "retry.hh"

namespace nixgrpc {

// A descriptor per path ran out on big copies.
struct SpoolFile
{
    nix::AutoCloseFD fd;
    uint64_t tail = 0; // only the session's reader thread appends

    auto get() -> int
    {
        if (!fd) {
            auto [tmpFd, tmpPath] = nix::createTempFile("nix-grpc-nar");
            ::unlink(tmpPath.c_str());
            fd = std::move(tmpFd);
        }
        return fd.get();
    }
};

// One path's range of the session file. The reader thread appends while
// fetchInto() streams it out, so a large NAR never has to fit in memory.
class NarSpool
{
    std::shared_ptr<SpoolFile> file;
    uint64_t base = 0;
    std::mutex mutex;
    std::condition_variable grown;
    uint64_t written = 0;
    bool eof = false;
    std::exception_ptr failure;

public:
    explicit NarSpool(std::shared_ptr<SpoolFile> file)
        : file(std::move(file))
    {
    }

    void append(std::string_view data)
    {
        auto const size = data.size();
        if (eof) {
            throw nix::Error("gRPC FetchNars sent data after EOF for a path");
        }
        if (written == 0) {
            base = file->tail;
        } else if (base + written != file->tail) {
            throw nix::Error("gRPC FetchNars interleaved paths");
        }
        uint64_t off = file->tail;
        auto const descriptor = file->get();
        while (!data.empty()) {
            auto const count = ::pwrite(descriptor, data.data(), data.size(), static_cast<off_t>(off));
            if (count < 0) {
                throw nix::SysError("writing NAR spool file");
            }
            data.remove_prefix(static_cast<size_t>(count));
            off += static_cast<uint64_t>(count);
        }
        file->tail = off;
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
            if (eof) {
                throw nix::Error("gRPC FetchNars sent duplicate EOF for a path");
            }
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

    [[nodiscard]] auto failed() -> bool
    {
        std::scoped_lock const lock(mutex);
        return failure != nullptr;
    }

    // Nothing delivered yet, so a fresh stream can start this path over.
    [[nodiscard]] auto untouched() -> bool
    {
        std::scoped_lock const lock(mutex);
        return written == 0 && !eof && !failure;
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
                    return;
                }
            }
            while (pos < avail) {
                auto const want = std::min<uint64_t>(buf.size(), avail - pos);
                // fd and base are set before `written` grows, under the mutex.
                auto const count = ::pread(file->fd.get(), buf.data(), want, static_cast<off_t>(base + pos));
                if (count <= 0) {
                    throw nix::SysError("reading NAR spool file");
                }
                sink({buf.data(), static_cast<size_t>(count)});
                pos += static_cast<uint64_t>(count);
            }
        }
    }
};

// Decode one FetchNars stream (server-controlled path_index/data/eof) into
// the per-path spools. Throws on malformed input; the caller fails all
// targets.
template<class Reader>
inline void demuxNarFrames(Reader & reader, const std::vector<std::shared_ptr<NarSpool>> & targets)
{
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> const dctx{ZSTD_createDCtx(), ZSTD_freeDCtx};
    std::string out(ZSTD_DStreamOutSize(), '\0');
    nix::remote::NarFrame frame;
    while (reader.Read(&frame)) {
        if (frame.path_index() >= targets.size()) {
            throw nix::Error("gRPC FetchNars sent path index %d for %d requested paths", frame.path_index(), targets.size());
        }
        const auto & target = targets.at(frame.path_index());
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
}

class NarFetcher
{
public:
    NarFetcher(
        std::function<std::shared_ptr<grpc::Channel>()> channelFactory,
        std::string authority,
        unsigned connections,
        RetryPolicy retry = {})
        : channelFactory(std::move(channelFactory))
        , authority(std::move(authority))
        , connections(std::max(1U, connections))
        , retry(std::move(retry))
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
            if (!live(path)) {
                pending.push_back(std::move(path));
            }
        }
        narSizes.merge(sizes);
    }

    void fetchInto(const nix::StorePath & path, nix::Sink & sink)
    {
        std::shared_ptr<NarSpool> buffer;
        {
            std::scoped_lock const lock(narMutex);
            if (!live(path)) {
                startPending();
            }
            auto found = buffers.find(path);
            if (found == buffers.end()) {
                startSession({path});
                found = buffers.find(path);
            }
            buffer = found->second;
        }
        // Also on failure: a cached failure would be rethrown for every later
        // request of this path, even after the cause (^C in a repl) is gone.
        auto const forget = [&] -> void {
            std::scoped_lock const lock(narMutex);
            auto const found = buffers.find(path);
            if (found != buffers.end() && found->second == buffer) {
                buffers.erase(found);
            }
        };
        try {
            buffer->readInto(sink);
        } catch (...) {
            forget();
            throw;
        }
        forget();
    }

    [[nodiscard]] auto sessionCount() -> size_t
    {
        std::scoped_lock const lock(narMutex);
        reap();
        return sessions.size();
    }

    ~NarFetcher()
    {
        for (const auto & session : sessions) {
            session->cancel();
            session->thread = {};
        }
    }

private:
    class Session
    {
        friend class NarFetcher;

        std::function<std::shared_ptr<grpc::Channel>()> channelFactory;
        std::string authority;
        RetryPolicy retry;
        struct Pending
        {
            nix::StorePath path;
            std::shared_ptr<NarSpool> spool;
        };
        std::vector<Pending> pending;
        std::mutex callMutex;
        // Only while an attempt runs: a Call stays registered for SIGINT.
        std::unique_ptr<nixgrpc::Call> live;
        std::atomic<bool> finished = false; // run() is about to return
        std::jthread thread;

    public:
        Session(const NarFetcher & fetcher, std::vector<Pending> pending)
            : channelFactory(fetcher.channelFactory)
            , authority(fetcher.authority)
            , retry(fetcher.retry)
            , pending(std::move(pending))
        {
        }

    private:
        void cancel()
        {
            thread.request_stop();
            std::scoped_lock const lock(callMutex);
            if (live) {
                live->ctx().TryCancel();
            }
        }

        auto fetchOnce(const std::stop_token & stop) -> grpc::Status
        {
            auto const stub = nix::remote::NixRemote::NewStub(channelFactory());
            nix::remote::FetchNarsRequest request;
            for (const auto & item : pending) {
                request.add_paths(std::string(item.path.to_string()));
            }
            {
                std::scoped_lock const lock(callMutex);
                if (stop.stop_requested()) {
                    return {grpc::StatusCode::CANCELLED, "NAR fetcher shut down"};
                }
                live = std::make_unique<nixgrpc::Call>();
            }
            auto const reader = stub->FetchNars(&live->ctx(), request);
            if (!reader) {
                throw nix::Error("failed to open gRPC FetchNars stream to '%s'", authority);
            }
            demuxNarFrames(*reader, pending | std::views::transform(&Pending::spool) | std::ranges::to<std::vector>());
            return reader->Finish();
        }

        // After the attempt's reader is gone, which streams on the call's context.
        void releaseCall()
        {
            std::scoped_lock const lock(callMutex);
            live.reset();
        }

        void run(const std::stop_token & stop)
        {
            attempts(stop);
            pending.clear();
            finished = true;
        }

        void attempts(const std::stop_token & stop)
        {
            Backoff backoff(retry.budget());
            try {
                for (;;) {
                    auto const status = fetchOnce(stop);
                    releaseCall();
                    if (!status.ok()) {
                        nix::checkInterrupt(); // our own cancel; the handler below passes it on
                    }
                    auto cause = std::make_exception_ptr(
                        status.ok() ? nix::Error("gRPC FetchNars stream ended early")
                                    : nix::Error("gRPC FetchNars failed: %s", status.error_message()));
                    // A path cut off mid-NAR cannot resume, but the ones not
                    // started yet can go on a fresh stream.
                    auto const cutOff = std::ranges::stable_partition(pending, &NarSpool::untouched, &Pending::spool);
                    for (const auto & item : cutOff) {
                        item.spool->fail(cause);
                    }
                    pending.erase(cutOff.begin(), cutOff.end());
                    bool retrying = !pending.empty() && retry.shouldRetry(status, backoff);
                    if (retrying) {
                        nix::logger->log(
                            nix::lvlError,
                            nix::fmt("%s: FetchNars: %s, retrying %d paths", authority, status.error_message(),
                                     pending.size()));
                        std::condition_variable_any wake;
                        std::mutex sleepMutex;
                        std::unique_lock sleepLock(sleepMutex);
                        // Returns early only when the fetcher shuts down.
                        wake.wait_for(sleepLock, stop, backoff.next(), [] -> bool { return false; });
                        retrying = !stop.stop_requested();
                    }
                    if (!retrying) {
                        for (const auto & item : pending) {
                            item.spool->fail(cause);
                        }
                        return;
                    }
                }
            } catch (...) {
                releaseCall();
                for (const auto & item : pending) {
                    item.spool->fail(std::current_exception());
                }
            }
        }
    };

    std::function<std::shared_ptr<grpc::Channel>()> channelFactory;
    std::string authority;
    unsigned connections;
    RetryPolicy retry;

    std::mutex narMutex;
    std::vector<std::unique_ptr<Session>> sessions;
    std::map<nix::StorePath, std::shared_ptr<NarSpool>> buffers;
    std::vector<nix::StorePath> pending;
    std::map<nix::StorePath, uint64_t> narSizes;

    [[nodiscard]] auto narSizeOf(const nix::StorePath & path) const -> uint64_t
    {
        auto const found = narSizes.find(path);
        return found == narSizes.end() ? 0 : found->second;
    }

    // Caller holds narMutex.
    void startPending()
    {
        if (pending.empty()) {
            return;
        }
        std::ranges::stable_sort(pending, [&](const auto & lhs, const auto & rhs) -> bool {
            return narSizeOf(lhs) > narSizeOf(rhs);
        });
        std::vector<std::vector<nix::StorePath>> groups(connections);
        std::vector<uint64_t> load(connections, 0);
        for (auto & path : pending) {
            auto const smallest = static_cast<size_t>(
                std::ranges::min_element(load) - load.begin());
            groups.at(smallest).push_back(std::move(path));
            load.at(smallest) += narSizeOf(groups.at(smallest).back());
        }
        pending.clear();
        for (auto const & group : groups) {
            if (!group.empty()) {
                startSession(group);
            }
        }
    }

    // Caller holds narMutex. A failed spool is dropped, so asking again
    // starts over, also for the paths of a failed session nobody read yet.
    auto live(const nix::StorePath & path) -> bool
    {
        auto const found = buffers.find(path);
        if (found == buffers.end()) {
            return false;
        }
        if (found->second->failed()) {
            buffers.erase(found);
            return false;
        }
        return true;
    }

    // Caller holds narMutex.
    void reap()
    {
        std::erase_if(sessions, [](const auto & session) -> bool { return session->finished; });
    }

    // Caller holds narMutex. Spools become visible only once their writer runs.
    void startSession(const std::vector<nix::StorePath> & paths)
    {
        reap();
        auto const file = std::make_shared<SpoolFile>();
        std::vector<Session::Pending> pending;
        pending.reserve(paths.size());
        for (const auto & path : paths) {
            pending.push_back({.path = path, .spool = std::make_shared<NarSpool>(file)});
        }
        auto spools = pending;
        auto session = std::make_unique<Session>(*this, std::move(pending));
        session->thread = std::jthread([raw = session.get()](const std::stop_token & stop) -> void { raw->run(stop); });
        sessions.push_back(std::move(session));
        for (auto & item : spools) {
            buffers.insert_or_assign(std::move(item.path), std::move(item.spool));
        }
    }
};

} // namespace nixgrpc
