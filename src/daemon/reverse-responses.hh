#pragma once
// Gateway-side, bounded validation of worker replies on one authenticated
// reverse session. A caller must close this state together with the session;
// it never supplies a fallback worker route or dispatches NixRemote RPCs.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <stop_token>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <grpcpp/support/status_code_enum.h>

#include "nix_remote.pb.h"
#include "reverse-frames.hh"

namespace nixgrpc {

class ReverseResponses
{
public:
    static constexpr size_t maxStatusBytes = 1024;

    // Reserve before sending an authenticated request on the *same* active
    // session. A failed request enqueue must cancel this reservation.
    [[nodiscard]] auto open(uint64_t streamId) -> bool
    {
        const std::scoped_lock lock(mutex);
        if (closed || streamId == 0 || streamId <= lastId || streams.size() >= ReverseFrames::maxStreams) {
            return false;
        }
        streams.emplace(streamId, false);
        lastId = streamId;
        return true;
    }

    // False is a protocol error or resource exhaustion: terminate the entire
    // registration instead of losing a finish/data frame or misrouting it.
    [[nodiscard]] auto accept(nix::remote::ReverseWorkerMsg frame) -> bool
    {
        const std::scoped_lock lock(mutex);
        if (closed || !frame.worker_id().empty() || frame.stream_id() == 0) {
            return false;
        }
        auto stream = streams.find(frame.stream_id());
        if (stream == streams.end()) {
            // A cancelled call may already have been sent when its caller
            // disconnected. Only explicitly cancelled IDs are tolerated;
            // late replies after normal completion remain protocol errors.
            if (!cancelled.contains(frame.stream_id())) {
                return false;
            }
            if (frame.has_data()) {
                return !frame.data().data().empty() && frame.data().data().size() <= ReverseFrames::maxFrameBytes;
            }
            if (frame.has_finish() && frame.finish().code() <= static_cast<uint32_t>(grpc::StatusCode::UNAUTHENTICATED)
                && frame.finish().message().size() <= maxStatusBytes) {
                cancelled.erase(frame.stream_id());
                return true;
            }
            return false;
        }
        if (stream->second) {
            return false;
        }
        if (frame.has_data()) {
            if (frame.data().data().empty() || frame.data().data().size() > ReverseFrames::maxFrameBytes) {
                return false;
            }
        } else if (frame.has_finish()) {
            if (frame.finish().code() > static_cast<uint32_t>(grpc::StatusCode::UNAUTHENTICATED)
                || frame.finish().message().size() > maxStatusBytes) {
                return false;
            }
        } else {
            return false;
        }
        auto const size = frame.ByteSizeLong();
        // Reserve room for terminal statuses even when a worker saturates the
        // data queue. A full queue still fails closed instead of dropping data.
        auto const limit = frame.has_data()
            ? ReverseFrames::maxQueuedBytes - ReverseFrames::maxFrameBytes - maxStatusBytes
            : ReverseFrames::maxQueuedBytes;
        if (size > ReverseFrames::maxFrameBytes + maxStatusBytes
            || queuedBytes > limit || size > limit - queuedBytes) {
            return false;
        }
        if (frame.has_finish()) {
            stream->second = true;
        }
        queuedBytes += size;
        queue.push_back(std::move(frame));
        cv.notify_all();
        return true;
    }

    // A client can only consume responses for its own stream. A global FIFO
    // consumer could steal a different client's result under concurrency.
    [[nodiscard]] auto waitTake(uint64_t streamId, nix::remote::ReverseWorkerMsg & frame, const std::stop_token & stop) -> bool
    {
        std::unique_lock lock(mutex);
        auto pending = [&] -> std::deque<nix::remote::ReverseWorkerMsg>::iterator {
            return std::ranges::find_if(queue, [streamId](auto const & next) -> auto { return next.stream_id() == streamId; });
        };
        cv.wait(lock, stop, [&] -> bool { return closed || !streams.contains(streamId) || pending() != queue.end(); });
        if (closed || stop.stop_requested() || !streams.contains(streamId)) {
            return false;
        }
        auto found = pending();
        if (found == queue.end()) {
            return false;
        }
        frame = std::move(*found);
        queuedBytes -= frame.ByteSizeLong();
        queue.erase(found);
        return true;
    }

    [[nodiscard]] auto take(nix::remote::ReverseWorkerMsg & frame) -> bool
    {
        const std::scoped_lock lock(mutex);
        if (queue.empty()) {
            return false;
        }
        frame = std::move(queue.front());
        queuedBytes -= frame.ByteSizeLong();
        queue.pop_front();
        return true;
    }

    // Once a client cancels, stale bytes/status for that ID are discarded;
    // this ID can never be reused within the registration.
    [[nodiscard]] auto cancel(uint64_t streamId, bool sent = true) -> bool
    {
        const std::scoped_lock lock(mutex);
        auto stream = streams.find(streamId);
        if (stream == streams.end()) {
            return false;
        }
        const bool finishAlreadyQueued = stream->second;
        streams.erase(stream);
        // Bound tombstones: a peer that indefinitely withholds finishes on
        // cancelled calls loses the registration rather than growing memory.
        if (sent && !finishAlreadyQueued && cancelled.size() >= maxCancelled) {
            closed = true;
            streams.clear();
            cancelled.clear();
            queue.clear();
            queuedBytes = 0;
            cv.notify_all();
            return false;
        }
        if (sent && !finishAlreadyQueued) {
            cancelled.insert(streamId);
        }
        for (auto it = queue.begin(); it != queue.end();) {
            if (it->stream_id() == streamId) {
                queuedBytes -= it->ByteSizeLong();
                it = queue.erase(it);
            } else {
                ++it;
            }
        }
        cv.notify_all();
        return true;
    }

    [[nodiscard]] auto finished(uint64_t streamId) -> bool
    {
        const std::scoped_lock lock(mutex);
        auto stream = streams.find(streamId);
        if (stream == streams.end() || !stream->second) {
            return false;
        }
        streams.erase(stream);
        cv.notify_all();
        return true;
    }

    void close()
    {
        const std::scoped_lock lock(mutex);
        closed = true;
        streams.clear();
        cancelled.clear();
        queue.clear();
        queuedBytes = 0;
        cv.notify_all();
    }

private:
    std::mutex mutex;
    std::condition_variable_any cv;
    bool closed = false;
    uint64_t lastId = 0;
    size_t queuedBytes = 0;
    // true once a final status is enqueued, even if not consumed yet.
    std::unordered_map<uint64_t, bool> streams;
    static constexpr size_t maxCancelled = 4096;
    std::unordered_set<uint64_t> cancelled;
    std::deque<nix::remote::ReverseWorkerMsg> queue;
};

} // namespace nixgrpc
