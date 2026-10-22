#pragma once
// Bounded worker-side response queue. The session owns the only gRPC writer;
// build threads never call Write and cannot buffer an unbounded build log.

#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_set>

#include <grpcpp/support/status.h>

#include "nix_remote.pb.h"
#include "reverse-relay.hh"

namespace nixgrpc {
class ReverseWorkerOutbound
{
public:
    [[nodiscard]] auto message(uint64_t id, const nix::remote::BuildDerivationChunk & chunk,
                               std::stop_token stop, const std::function<bool()> & cancelled = {}) -> bool
    {
        auto const size = chunk.ByteSizeLong();
        if (size > ReverseRelay::maxMessageBytes) return false;
        auto wire = chunk.SerializeAsString();
        if (wire.size() > ReverseRelay::maxMessageBytes) return false;
        std::string bytes;
        bytes.reserve(wire.size() + 4);
        auto length = static_cast<uint32_t>(wire.size());
        for (unsigned i = 0; i < 4; ++i) bytes.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        bytes += wire;
        for (size_t offset = 0; offset < bytes.size(); offset += ReverseFrames::maxFrameBytes) {
            nix::remote::ReverseWorkerMsg frame;
            frame.set_stream_id(id);
            frame.mutable_data()->set_data(bytes.substr(offset, ReverseFrames::maxFrameBytes));
            if (!push(std::move(frame), stop, cancelled)) return false;
        }
        return true;
    }

    [[nodiscard]] auto finish(uint64_t id, const grpc::Status & status, std::stop_token stop,
                               bool waitForSpace = true, std::function<void()> afterAck = {}) -> bool
    {
        nix::remote::ReverseWorkerMsg frame;
        frame.set_stream_id(id);
        frame.mutable_finish()->set_code(static_cast<uint32_t>(status.error_code()));
        frame.mutable_finish()->set_message(status.error_message().substr(0, 1024));
        // A cancel can originate on the only reader thread. That caller must
        // use waitForSpace=false and reconnect on a saturated queue.
        return push(std::move(frame), stop, {}, waitForSpace, std::move(afterAck));
    }

    [[nodiscard]] auto take(nix::remote::ReverseWorkerMsg & frame, std::stop_token stop) -> bool
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, stop, [&] { return closed || !queue.empty(); });
        if (closed || stop.stop_requested() || queue.empty()) return false;
        frame = std::move(queue.front());
        queued -= frame.ByteSizeLong();
        queue.pop_front();
        cv.notify_all();
        return true;
    }

    [[nodiscard]] auto acknowledged(uint64_t id) -> bool
    {
        std::function<void()> completion;
        {
            std::scoped_lock lock(mutex);
            auto it = afterAcks.find(id);
            if (it == afterAcks.end()) return false;
            completion = std::move(it->second);
            afterAcks.erase(it);
        }
        if (completion) completion();
        return true;
    }

    void close()
    {
        std::scoped_lock lock(mutex);
        closed = true;
        queue.clear();
        queued = 0;
        cancelled.clear();
        cancelOrder.clear();
        cv.notify_all();
    }

    void abandonUnacknowledged()
    {
        std::unordered_map<uint64_t, std::function<void()>> pending;
        {
            std::scoped_lock lock(mutex);
            pending.swap(afterAcks);
        }
        for (auto & [id, complete] : pending) {
            (void) id;
            complete();
        }
    }

    void cancel(uint64_t id)
    {
        std::scoped_lock lock(mutex);
        if (cancelled.insert(id).second) {
            if (cancelOrder.size() == maxCancelled) {
                closed = true;
                queue.clear();
                queued = 0;
                cv.notify_all();
                return;
            }
            cancelOrder.push_back(id);
        }
        for (auto it = queue.begin(); it != queue.end();) {
            if (it->stream_id() == id && it->has_data()) {
                queued -= it->ByteSizeLong();
                it = queue.erase(it);
            } else ++it;
        }
        cv.notify_all();
    }

    [[nodiscard]] auto healthy() const -> bool
    {
        std::scoped_lock lock(mutex);
        return !closed;
    }

private:
    [[nodiscard]] auto push(nix::remote::ReverseWorkerMsg frame, std::stop_token stop,
                              const std::function<bool()> & cancelled = {}, bool waitForSpace = true,
                              std::function<void()> afterAck = {}) -> bool
    {
        auto const size = frame.ByteSizeLong();
        if (size > ReverseFrames::maxFrameBytes + 1024) return false;
        std::unique_lock lock(mutex);
        bool const terminal = frame.has_finish();
        while (waitForSpace && !closed && !stop.stop_requested() && queued + size > ReverseFrames::maxQueuedBytes
               && (!cancelled || !cancelled()))
            cv.wait_for(lock, std::chrono::milliseconds(25));
        if (closed || stop.stop_requested() || queued + size > ReverseFrames::maxQueuedBytes
            || (!terminal && this->cancelled.contains(frame.stream_id()))
            || (cancelled && cancelled())) return false;
        if (afterAck) {
            if (afterAcks.size() >= ReverseFrames::maxStreams
                || !afterAcks.emplace(frame.stream_id(), std::move(afterAck)).second) return false;
        }
        queued += size;
        queue.push_back(std::move(frame));
        // Cancelled IDs are kept for this registration, bounded by the
        // fail-closed cap in cancel().
        cv.notify_all();
        return true;
    }

    mutable std::mutex mutex;
    std::condition_variable_any cv;
    std::deque<nix::remote::ReverseWorkerMsg> queue;
    std::unordered_map<uint64_t, std::function<void()>> afterAcks;
    std::unordered_set<uint64_t> cancelled;
    std::deque<uint64_t> cancelOrder;
    static constexpr size_t maxCancelled = 4096;
    size_t queued = 0;
    bool closed = false;
};
} // namespace nixgrpc
