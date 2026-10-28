#pragma once
// Bounded outgoing queue for the worker-initiated reverse data stream. This
// is a transport primitive only: authorization and RPC dispatch happen outside.

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <stop_token>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "nix_remote.pb.h"

namespace nixgrpc {

class ReverseFrames
{
public:
    static constexpr size_t maxFrameBytes = 64 * 1024;
    static constexpr size_t maxQueuedBytes = 4 * 1024 * 1024;
    static constexpr size_t maxStreams = 64;

    // Intended only after the caller has been authenticated at the gateway
    // and a scoped capability minted. A nonempty capability field is NOT
    // proof of authentication; the gateway must authorize it independently.
    auto open(uint64_t id, nix::remote::ReverseOpen start) -> bool
    {
        if (id == 0 || start.capability().empty() || start.capability().size() > maxFrameBytes
            || !methodAllowed(start.method())) {
            return false;
        }
        const std::scoped_lock lock(mutex);
        // IDs are never reused during a session, even after a response ends:
        // otherwise queued late frames could be applied to a new RPC.
        if (closed || streams.size() >= maxStreams || id <= lastId) {
            return false;
        }
        nix::remote::ReverseFarmMsg msg;
        msg.set_stream_id(id);
        *msg.mutable_open() = std::move(start);
        if (!enqueue(std::move(msg))) {
            return false;
        }
        streams.emplace(id, false);
        lastId = id;
        return true;
    }

    auto data(uint64_t id, std::string_view bytes) -> bool
    {
        if (bytes.empty() || bytes.size() > maxFrameBytes) {
            return false;
        }
        const std::scoped_lock lock(mutex);
        if (closed || !streams.contains(id) || streams.at(id)) {
            return false;
        }
        nix::remote::ReverseFarmMsg msg;
        msg.set_stream_id(id);
        msg.mutable_data()->set_data(bytes);
        // Leave one full frame of headroom for cancellation/control messages
        // even when a producer saturates the data queue.
        return enqueue(std::move(msg), maxQueuedBytes - maxFrameBytes - 1024);
    }

    // Streaming uploads use bounded backpressure instead of treating a full
    // queue as a failed build. The reader can stop this wait on client cancel,
    // assignment loss or deadline; session teardown wakes it via close().
    auto waitData(uint64_t id, std::string_view bytes, std::stop_token stop) -> bool
    {
        if (bytes.empty() || bytes.size() > maxFrameBytes) return false;
        nix::remote::ReverseFarmMsg msg;
        msg.set_stream_id(id);
        msg.mutable_data()->set_data(bytes);
        auto const size = msg.ByteSizeLong();
        auto constexpr limit = maxQueuedBytes - maxFrameBytes - 1024;
        if (size > limit) return false;
        std::unique_lock lock(mutex);
        cv.wait(lock, stop, [&] {
            auto it = streams.find(id);
            return closed || it == streams.end() || it->second || queuedBytes + size <= limit;
        });
        auto const it = streams.find(id);
        if (closed || stop.stop_requested() || it == streams.end() || it->second) return false;
        return enqueue(std::move(msg), limit);
    }

    // Half-close preserves the response path; cancel terminates both sides.
    auto halfClose(uint64_t id) -> bool { return terminal(id, false); }
    auto cancel(uint64_t id) -> bool { return terminal(id, true); }

    auto acknowledge(uint64_t id) -> bool
    {
        const std::scoped_lock lock(mutex);
        // One acknowledgement per result; a second one would make the worker
        // treat its next deferred completion as already accepted.
        if (closed || !streams.contains(id) || acknowledged.contains(id)) return false;
        nix::remote::ReverseFarmMsg msg;
        msg.set_stream_id(id);
        msg.set_acknowledged(true);
        if (!enqueue(std::move(msg))) return false;
        acknowledged.insert(id);
        return true;
    }

    // A later transport reader would own gRPC Write and call take() only when
    // ready to send; producer threads never write under this queue's mutex.
    auto take(nix::remote::ReverseFarmMsg & msg) -> bool
    {
        const std::scoped_lock lock(mutex);
        if (queue.empty()) {
            return false;
        }
        msg = std::move(queue.front());
        queuedBytes -= msg.ByteSizeLong();
        queue.pop_front();
        cv.notify_all();
        return true;
    }

    // One dedicated stream writer consumes frames; producer threads only
    // enqueue. Closing the queue wakes it without a busy poll. A live writer
    // must recheck registry.current(session) before each gRPC Write.
    auto waitTake(nix::remote::ReverseFarmMsg & msg, std::stop_token stop) -> bool
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, stop, [&] { return closed || !queue.empty(); });
        if (closed || stop.stop_requested() || queue.empty()) {
            return false;
        }
        msg = std::move(queue.front());
        queuedBytes -= msg.ByteSizeLong();
        queue.pop_front();
        cv.notify_all();
        return true;
    }

    // Called on response finish (or tunnel teardown). Leave an acknowledged
    // build result's reservation alive until the writer sends its pending ack.
    // Stale stream IDs are never mapped to a new call.
    auto finished(uint64_t id) -> bool
    {
        const std::scoped_lock lock(mutex);
        if (streams.contains(id) && acknowledged.contains(id)) return true;
        auto const removed = streams.erase(id) != 0;
        if (removed) cv.notify_all();
        return removed;
    }

    // The writer retires this reservation after gRPC accepts the ack. A
    // failed Write closes the enclosing data session instead.
    void acknowledgementSent(uint64_t id)
    {
        const std::scoped_lock lock(mutex);
        if (acknowledged.erase(id)) streams.erase(id);
        cv.notify_all();
    }

    void close()
    {
        const std::scoped_lock lock(mutex);
        closed = true;
        streams.clear();
        acknowledged.clear();
        queue.clear();
        queuedBytes = 0;
        cv.notify_all();
    }

private:
    std::mutex mutex;
    std::condition_variable_any cv;
    std::deque<nix::remote::ReverseFarmMsg> queue;
    // true after a request half-close; response may still be in flight.
    std::unordered_map<uint64_t, bool> streams;
    std::unordered_set<uint64_t> acknowledged;
    size_t queuedBytes = 0;
    uint64_t lastId = 0;
    bool closed = false;

    static auto methodAllowed(std::string_view method) -> bool
    {
        return method == "BuildDerivation" || method == "UploadBuildInputs" || method == "QueryValidPaths"
            || method == "AddMultipleToStore" || method == "QueryPathInfos" || method == "FetchNars"
            || method == "QueryMissing" || method == "StoreInfo" || method == "Connect";
    }

    // Lock held by caller.
    auto enqueue(nix::remote::ReverseFarmMsg msg, size_t limit = maxQueuedBytes) -> bool
    {
        auto const bytes = msg.ByteSizeLong();
        if (bytes > maxFrameBytes + 1024 || queuedBytes > limit || bytes > limit - queuedBytes) {
            return false;
        }
        queue.push_back(std::move(msg));
        queuedBytes += bytes;
        // The same CV wakes both the writer (queue nonempty) and streaming
        // producers (queue has space). notify_one may wake the wrong kind of
        // waiter and leave a full upload stuck with an idle writer.
        cv.notify_all();
        return true;
    }

    auto terminal(uint64_t id, bool abort) -> bool
    {
        const std::scoped_lock lock(mutex);
        if (closed || !streams.contains(id) || (!abort && streams.at(id))) {
            return false;
        }
        // Cancellation releases queued payload before reserving room for
        // its terminal frame, so a saturated upload can still be cancelled.
        if (abort) {
            for (auto it = queue.begin(); it != queue.end();) {
                if (it->stream_id() == id && it->has_data()) {
                    queuedBytes -= it->ByteSizeLong();
                    it = queue.erase(it);
                } else {
                    ++it;
                }
            }
            cv.notify_all();
        }
        nix::remote::ReverseFarmMsg msg;
        msg.set_stream_id(id);
        if (abort) {
            msg.set_cancel(true);
        } else {
            msg.set_half_close(true);
        }
        if (!enqueue(std::move(msg))) {
            return false;
        }
        if (abort) {
            streams.erase(id);
        } else {
            streams.at(id) = true;
        }
        return true;
    }
};

} // namespace nixgrpc
