#pragma once
// Worker-side validation before a reverse stream can reach any NixRemote
// handler. This state machine does not dispatch RPCs.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <deque>

#include "reverse-capability.hh"
#include "reverse-frames.hh"

namespace nixgrpc
{

class ReverseInbound
{
  public:
    struct Event
    {
        enum class Kind : uint8_t
        {
            open,
            data,
            halfClose,
            cancel
        } kind;
        uint64_t streamId;
        std::optional<ReverseCapability::Verified> caller;
        // A cancel for a stream that already finished: nothing to stop.
        bool late = false;
    };

    ReverseInbound(std::string workerId, uint64_t generation, std::string sessionKey)
        : workerId(std::move(workerId)), generation(generation), sessionKey(std::move(sessionKey))
    {
    }

    // Called serially by the single worker stream reader. The caller may
    // hand a validated event to a bounded dispatcher, but must never treat
    // an open as a complete RPC: it must validate the decoded request too.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity): one cohesive per-frame state machine.
    [[nodiscard]] auto accept(const nix::remote::ReverseFarmMsg &frame) -> std::optional<Event>
    {
        if (closed || frame.generation() != 0 || !frame.session_key().empty() || frame.stream_id() == 0)
        {
            rejection = closed ? "session closed" : "bad frame header";
            return std::nullopt;
        }
        auto const streamId = frame.stream_id();
        if (frame.has_open())
        {
            if (streamId <= lastId || streams.size() >= ReverseFrames::maxStreams)
            {
                rejection = streamId <= lastId ? "stream id not increasing" : "too many streams";
                return std::nullopt;
            }
            auto caller = ReverseCapability::authorizeOpen(sessionKey, frame.open(), workerId, generation, streamId);
            if (!caller)
            {
                rejection = "capability rejected";
                return std::nullopt;
            }
            streams.emplace(streamId, false);
            lastId = streamId;
            return Event{.kind = Event::Kind::open, .streamId = streamId, .caller = std::move(caller)};
        }
        auto stream = streams.find(streamId);
        if (stream == streams.end())
        {
            // A cancel can arrive after the final response crossed the wire.
            // Discard it once, without opening another stream or accepting data.
            if (frame.frame_case() == nix::remote::ReverseFarmMsg::kCancel && frame.cancel()
                && (finishedIds.erase(streamId) != 0U))
            {
                return Event{.kind = Event::Kind::cancel, .streamId = streamId, .caller = std::nullopt, .late = true};
            }
            rejection = "unknown stream";
            return std::nullopt;
        }
        if (frame.has_data())
        {
            if (stream->second || frame.data().data().empty() || frame.data().data().size() > ReverseFrames::maxFrameBytes)
            {
                rejection = "bad data frame";
                return std::nullopt;
            }
            return Event{.kind = Event::Kind::data, .streamId = streamId};
        }
        if (frame.frame_case() == nix::remote::ReverseFarmMsg::kHalfClose && frame.half_close() && !stream->second)
        {
            stream->second = true;
            return Event{.kind = Event::Kind::halfClose, .streamId = streamId};
        }
        if (frame.frame_case() == nix::remote::ReverseFarmMsg::kCancel && frame.cancel())
        {
            if (cancelledIds.size() >= maxCancelled) { closed = true; rejection = "too many cancels"; return std::nullopt; }
            streams.erase(stream);
            cancelledIds.insert(streamId);
            return Event{.kind = Event::Kind::cancel, .streamId = streamId};
        }
        rejection = "unexpected frame";
        return std::nullopt;
    }

    // Why the last accept() returned nothing; for logs only.
    [[nodiscard]] auto lastRejection() const -> const char * { return rejection; }

    // Called after response status was sent. A late frame cannot revive a
    // finished stream or use the same ID for a different caller.
    auto finished(uint64_t streamId) -> bool
    {
        if (cancelledIds.erase(streamId) != 0U)
        {
            return true;
        }
        if (streams.erase(streamId) == 0U)
        {
            return false;
        }
        if (finishedOrder.size() == maxFinished) {
            finishedIds.erase(finishedOrder.front());
            finishedOrder.pop_front();
        }
        finishedIds.insert(streamId);
        finishedOrder.push_back(streamId);
        return true;
    }

    void close()
    {
        closed = true;
        streams.clear();
        finishedIds.clear();
        finishedOrder.clear();
        cancelledIds.clear();
        sessionKey.clear();
    }

  private:
    std::string workerId;
    uint64_t generation;
    std::string sessionKey;
    uint64_t lastId = 0;
    bool closed = false;
    const char * rejection = "";
    std::unordered_map<uint64_t, bool> streams;
    static constexpr size_t maxFinished = 4096;
    std::unordered_set<uint64_t> finishedIds;
    std::deque<uint64_t> finishedOrder;
    // Cancelled requests already dispatched to a handler may finish later.
    // At most maxStreams handlers can be live on one registration.
    std::unordered_set<uint64_t> cancelledIds;
    static constexpr size_t maxCancelled = 4096;
};

} // namespace nixgrpc
