#pragma once
// The one pause schedule of every retry loop, in the client and the daemon.

#include <algorithm>
#include <chrono>
#include <limits>

namespace nixgrpc {

constexpr std::chrono::milliseconds firstPause{500};
constexpr std::chrono::milliseconds maxPause{4000};

// How long a retry loop waits before its `retry`th
// retry, counted from 1: `firstPause`, doubling up to `maxPause`.
[[nodiscard]] constexpr auto retryPause(unsigned retry) noexcept -> std::chrono::milliseconds
{
    auto pause = firstPause;
    for (unsigned done = 1; done < retry && pause < maxPause; done++) {
        pause *= 2;
    }
    return std::min(pause, maxPause);
}

static_assert(retryPause(0) == firstPause);
static_assert(retryPause(1) == firstPause);
static_assert(retryPause(2) == 2 * firstPause);
static_assert(retryPause(std::numeric_limits<unsigned>::max()) == maxPause);

} // namespace nixgrpc
