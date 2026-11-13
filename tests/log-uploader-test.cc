// The log store uploader: one log at a time off the caller's thread, logs
// over the cap skipped, a full queue dropping its oldest, and idle() for a
// drain to wait on.

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "log-uploader.hh"
#include "metrics.hh"

using nixgrpc::LogUploader;

// NOLINTBEGIN(*-magic-numbers): waits and counts.
namespace {

auto waitIdle(const LogUploader & uploader) -> bool
{
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!uploader.idle()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}

void uploadsSkipsAndFailures()
{
    nixgrpc::Metrics metrics("");
    std::mutex mutex;
    std::map<std::string, std::string> stored;
    LogUploader uploader(
        [](const std::string & drv, uint64_t limit) -> std::optional<std::string> {
            if (drv == "none.drv") {
                return std::nullopt;
            }
            if (drv == "huge.drv") {
                return std::string(limit + 1, 'x');
            }
            return "log of " + drv;
        },
        [&](const std::string & drv, std::string_view log) -> void {
            if (drv == "refused.drv") {
                throw std::runtime_error("the store said no");
            }
            const std::scoped_lock lock(mutex);
            stored[drv] = log;
        },
        metrics);
    for (auto const * drv : {"a.drv", "none.drv", "huge.drv", "refused.drv", "b.drv"}) {
        uploader.enqueue(drv);
    }
    assert(waitIdle(uploader));
    const std::scoped_lock lock(mutex);
    assert(stored.size() == 2 && stored.at("a.drv") == "log of a.drv" && stored.at("b.drv") == "log of b.drv");
}

// While one upload hangs, the queue keeps its newest 256, and idle() says
// no until all are through.
void aFullQueueDropsTheOldest()
{
    nixgrpc::Metrics metrics("");
    std::mutex mutex;
    std::condition_variable changed;
    bool started = false;
    bool open = false;
    std::atomic<int> uploaded{0};
    std::map<std::string, int> seen;
    LogUploader uploader(
        [](const std::string & drv, uint64_t) -> std::optional<std::string> { return drv; },
        [&](const std::string & drv, std::string_view) -> void {
            std::unique_lock lock(mutex);
            started = true;
            changed.notify_all();
            changed.wait(lock, [&] -> bool { return open; });
            seen[drv]++;
            uploaded++;
        },
        metrics);
    uploader.enqueue("first.drv");
    {
        // Once it is uploading, everything after queues behind it.
        std::unique_lock lock(mutex);
        auto const uploading = changed.wait_for(lock, std::chrono::seconds(10), [&] -> bool { return started; });
        assert(uploading);
    }
    for (size_t idx = 0; idx < LogUploader::maxQueue + 10; idx++) {
        uploader.enqueue("q" + std::to_string(idx) + ".drv");
    }
    assert(!uploader.idle());
    {
        const std::scoped_lock lock(mutex);
        open = true;
    }
    changed.notify_all();
    assert(waitIdle(uploader));
    assert(uploaded == static_cast<int>(LogUploader::maxQueue) + 1);
    const std::scoped_lock lock(mutex);
    assert(seen.contains("first.drv") && !seen.contains("q0.drv") && !seen.contains("q9.drv"));
    assert(seen.contains("q10.drv") && seen.contains("q265.drv"));
}

} // namespace
// NOLINTEND(*-magic-numbers)

auto main() -> int
{
    try {
        uploadsSkipsAndFailures();
        aFullQueueDropsTheOldest();
    } catch (std::exception & err) {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
    return 0;
}
