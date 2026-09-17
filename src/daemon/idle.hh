#pragma once
// Tracks in-flight RPCs so the daemon can exit after a period without any.

#include <atomic>
#include <chrono>
#include <cstdint>

namespace nixgrpc {

class IdleTracker
{
    using Clock = std::chrono::steady_clock;

    std::atomic<uint32_t> inflight{0};
    std::atomic<Clock::rep> lastFinished{Clock::now().time_since_epoch().count()};

public:
    class Guard
    {
        IdleTracker & tracker;

    public:
        explicit Guard(IdleTracker & tracker)
            : tracker(tracker)
        {
            ++tracker.inflight;
        }

        Guard(const Guard &) = delete;
        Guard(Guard &&) = delete;
        auto operator=(const Guard &) -> Guard & = delete;
        auto operator=(Guard &&) -> Guard & = delete;

        ~Guard()
        {
            tracker.lastFinished = Clock::now().time_since_epoch().count();
            --tracker.inflight;
        }
    };

    [[nodiscard]] auto idleFor() const -> Clock::duration
    {
        if (inflight != 0) {
            return Clock::duration::zero();
        }
        return Clock::now() - Clock::time_point(Clock::duration(lastFinished.load()));
    }
};

} // namespace nixgrpc
