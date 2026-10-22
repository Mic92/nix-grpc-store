#pragma once
// One upload's bounded handoff from the registration reader to its importer.
// The importer owns the Nix store; the reader never accumulates a whole NAR.

#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "reverse-frames.hh"
#include "reverse-upload.hh"

namespace nixgrpc {

class ReverseUploadPipe
{
public:
    explicit ReverseUploadPipe(ReverseCapability::Verified delegated, std::function<bool()> assigned = {})
        : decoder(std::move(delegated)), assigned(std::move(assigned)) { }

    // Only the registration reader calls feed and finish. It must never wait
    // on an importer (which may be blocked on a store lock): a full queue
    // fails the registration closed, leaving cancellation/control responsive.
    [[nodiscard]] auto feed(std::string_view bytes) -> bool
    {
        if (assigned && !assigned()) { cancel(); return false; }
        auto const ok = decoder.feed(bytes, [&](nix::remote::UploadBuildInputsChunk && frame) {
            if (assigned && !assigned()) return false;
            auto const size = frame.ByteSizeLong();
            std::scoped_lock lock(mutex);
            if (closed || ended || size > maxQueuedBytes || queuedBytes > maxQueuedBytes - size) return false;
            queuedBytes += size;
            chunks.push_back(std::move(*frame.mutable_chunk()));
            cv.notify_all();
            return true;
        });
        if (!ok) cancel();
        return ok;
    }

    [[nodiscard]] auto finish() -> bool
    {
        if (assigned && !assigned()) { cancel(); return false; }
        std::scoped_lock lock(mutex);
        if (!decoder.complete() || closed) {
            closed = true;
            chunks.clear();
            queuedBytes = 0;
            cv.notify_all();
            return false;
        }
        ended = true;
        cv.notify_all();
        return true;
    }

    // A reader interface suitable for ZstdReaderSource. False means either
    // half-close or cancellation; check complete() before publishing results.
    [[nodiscard]] auto Read(nix::remote::AddMultipleChunk * chunk) -> bool
    {
        std::unique_lock lock(mutex);
        while (true) {
            if (closed) return false;
            lock.unlock();
            bool const live = !assigned || assigned();
            lock.lock();
            if (!live) {
                closed = true;
                chunks.clear();
                queuedBytes = 0;
                cv.notify_all();
                return false;
            }
            if (ended || !chunks.empty()) break;
            cv.wait_for(lock, std::chrono::milliseconds(25));
        }
        if (chunks.empty()) return false;
        *chunk = std::move(chunks.front());
        queuedBytes -= chunk->ByteSizeLong();
        chunks.pop_front();
        cv.notify_all();
        return true;
    }

    [[nodiscard]] auto complete() const -> bool
    {
        std::scoped_lock lock(mutex);
        return ended && !closed && chunks.empty();
    }

    // After importPaths consumes the declared closure, wait for the actual
    // end of the request; a late continuation must not be accepted as a
    // successful import. Returning false means either extra bytes or revoke.
    [[nodiscard]] auto endWithoutTrailingData() -> bool
    {
        std::unique_lock lock(mutex);
        while (!closed && !ended && chunks.empty()) {
            cv.wait_for(lock, std::chrono::milliseconds(25));
            lock.unlock();
            bool const live = !assigned || assigned();
            lock.lock();
            if (!live) {
                closed = true;
                chunks.clear();
                queuedBytes = 0;
            }
        }
        if (!ended || closed || !chunks.empty()) return false;
        lock.unlock();
        return !assigned || assigned();
    }

    void cancel()
    {
        std::scoped_lock lock(mutex);
        closed = true;
        chunks.clear();
        queuedBytes = 0;
        cv.notify_all();
    }

    [[nodiscard]] auto cancelled() const -> bool
    {
        std::scoped_lock lock(mutex);
        return closed;
    }

private:
    static constexpr size_t maxQueuedBytes = ReverseFrames::maxQueuedBytes;
    ReverseUpload::Decoder decoder;
    std::function<bool()> assigned;
    mutable std::mutex mutex;
    std::condition_variable_any cv;
    std::deque<nix::remote::AddMultipleChunk> chunks;
    size_t queuedBytes = 0;
    bool ended = false;
    bool closed = false;
};

} // namespace nixgrpc
