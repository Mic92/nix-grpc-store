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
#include <utility>

#include <grpcpp/support/status.h>

#include "nix_remote.pb.h"
#include "reverse-relay.hh"

namespace nixgrpc {
class ReverseWorkerOutbound
{
public:
    [[nodiscard]] auto message(uint64_t streamId, const nix::remote::BuildDerivationChunk & chunk,
                               const std::stop_token & stop, const std::function<bool()> & cancelled = {}) -> bool
    {
        auto const size = chunk.ByteSizeLong();
        if (size > ReverseRelay::maxMessageBytes) {
            return false;
        }
        auto wire = chunk.SerializeAsString();
        if (wire.size() > ReverseRelay::maxMessageBytes) {
            return false;
        }
        std::string bytes;
        bytes.reserve(wire.size() + 4);
        auto length = static_cast<uint32_t>(wire.size());
        for (unsigned i = 0; i < 4; ++i) {
            bytes.push_back(static_cast<char>((length >> (bitsPerByte * i)) & byteMask));
        }
        bytes += wire;
        for (size_t offset = 0; offset < bytes.size(); offset += ReverseFrames::maxFrameBytes) {
            nix::remote::ReverseWorkerMsg frame;
            frame.set_stream_id(streamId);
            frame.mutable_data()->set_data(bytes.substr(offset, ReverseFrames::maxFrameBytes));
            if (!push(std::move(frame), stop, cancelled)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] auto finish(uint64_t streamId, const grpc::Status & status, const std::stop_token & stop,
                               bool waitForSpace = true, std::function<void()> afterAck = {}) -> bool
    {
        nix::remote::ReverseWorkerMsg frame;
        frame.set_stream_id(streamId);
        frame.mutable_finish()->set_code(static_cast<uint32_t>(status.error_code()));
        frame.mutable_finish()->set_message(status.error_message().substr(0, maxFinishMessageBytes));
        // A cancel can originate on the only reader thread. That caller must
        // use waitForSpace=false and reconnect on a saturated queue.
        return push(std::move(frame), stop, {}, waitForSpace, std::move(afterAck));
    }

    // A rotated tunnel token. Not tied to a stream; never waits for space.
    [[nodiscard]] auto credential(std::string token) -> bool
    {
        nix::remote::ReverseWorkerMsg frame;
        frame.set_credential(std::move(token));
        return push(std::move(frame), {}, {}, false);
    }

    [[nodiscard]] auto take(nix::remote::ReverseWorkerMsg & frame, const std::stop_token & stop) -> bool
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, stop, [&] -> bool { return closed || !queue.empty(); });
        if (closed || stop.stop_requested() || queue.empty()) {
            return false;
        }
        frame = std::move(queue.front());
        queued -= frame.ByteSizeLong();
        queue.pop_front();
        cv.notify_all();
        return true;
    }

    [[nodiscard]] auto acknowledged(uint64_t streamId) -> bool
    {
        std::function<void()> completion;
        {
            std::scoped_lock const lock(mutex);
            auto entry = afterAcks.find(streamId);
            if (entry == afterAcks.end()) {
                return false;
            }
            completion = std::move(entry->second);
            afterAcks.erase(entry);
        }
        if (completion) {
            completion();
        }
        return true;
    }

    void close()
    {
        std::scoped_lock const lock(mutex);
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
            std::scoped_lock const lock(mutex);
            pending.swap(afterAcks);
        }
        for (auto & [streamId, complete] : pending) {
            (void) streamId;
            complete();
        }
    }

    void cancel(uint64_t streamId)
    {
        std::scoped_lock const lock(mutex);
        if (cancelled.insert(streamId).second) {
            if (cancelOrder.size() == maxCancelled) {
                closed = true;
                queue.clear();
                queued = 0;
                cv.notify_all();
                return;
            }
            cancelOrder.push_back(streamId);
        }
        for (auto entry = queue.begin(); entry != queue.end();) {
            if (entry->stream_id() == streamId && entry->has_data()) {
                queued -= entry->ByteSizeLong();
                entry = queue.erase(entry);
            } else {
                ++entry;
            }
        }
        cv.notify_all();
    }

    [[nodiscard]] auto healthy() const -> bool
    {
        std::scoped_lock const lock(mutex);
        return !closed;
    }

private:
    [[nodiscard]] auto push(nix::remote::ReverseWorkerMsg frame, const std::stop_token & stop,
                              const std::function<bool()> & cancelled = {}, bool waitForSpace = true,
                              std::function<void()> afterAck = {}) -> bool
    {
        auto const size = frame.ByteSizeLong();
        if (size > ReverseFrames::maxFrameBytes + frameOverheadBytes) {
            return false;
        }
        std::unique_lock lock(mutex);
        bool const terminal = frame.has_finish();
        while (waitForSpace && !closed && !stop.stop_requested() && queued + size > ReverseFrames::maxQueuedBytes
               && (!cancelled || !cancelled())) {
            cv.wait_for(lock, spaceWaitInterval);
        }
        if (closed || stop.stop_requested() || queued + size > ReverseFrames::maxQueuedBytes
            || (!terminal && this->cancelled.contains(frame.stream_id()))
            || (cancelled && cancelled())) {
            return false;
        }
        if (afterAck) {
            if (afterAcks.size() >= ReverseFrames::maxStreams
                || !afterAcks.emplace(frame.stream_id(), std::move(afterAck)).second) {
                return false;
            }
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
    static constexpr unsigned bitsPerByte = 8;
    static constexpr uint32_t byteMask = 0xff;
    static constexpr size_t maxFinishMessageBytes = 1024;
    static constexpr size_t frameOverheadBytes = 1024;
    static constexpr std::chrono::milliseconds spaceWaitInterval{25};
    size_t queued = 0;
    bool closed = false;
};
} // namespace nixgrpc
