#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <stdlib.h> // NOLINT(modernize-deprecated-headers): setenv is POSIX, not in <cstdlib>.

#include "log-spill.hh"

using nixgrpc::LogSpill;
using nixgrpc::SpillKind;
using Records = std::vector<std::pair<SpillKind, std::string>>;

constexpr std::size_t kBigLine = 300000;
constexpr std::size_t kOverflow = 5;
constexpr std::size_t kSmallCap = 64;
constexpr std::size_t kTinyCap = 3;
constexpr int kSmallCapRecords = 50;
constexpr int kChattyLines = 20000;
constexpr std::size_t kLateStart = 1000;
constexpr std::chrono::milliseconds kPoll{50};

namespace {

auto readAll(const LogSpill & spill) -> Records
{
    Records out;
    auto const end = spill.drain(0, [&](SpillKind kind, std::string_view text) -> void { out.emplace_back(kind, std::string(text)); });
    assert(end == spill.written());
    return out;
}

auto follow(const LogSpill & spill, std::mutex & mutex, std::condition_variable & wakeup, const bool & finished) -> Records
{
    Records out;
    auto const done = nixgrpc::followSpill(
        spill, mutex, wakeup, finished, kPoll, [] -> bool { return false; },
        [&](SpillKind kind, std::string_view text) -> void { out.emplace_back(kind, std::string(text)); });
    assert(done);
    return out;
}

void roundTrip()
{
    LogSpill spill;
    assert(readAll(spill).empty());
    spill.append(SpillKind::line, "first");
    spill.append(SpillKind::phase, "build");
    spill.append(SpillKind::line, "");
    spill.append(SpillKind::line, std::string(kBigLine, 'x'));
    auto const got = readAll(spill);
    assert(got.size() == 4);
    assert(got.at(0) == std::make_pair(SpillKind::line, std::string("first")));
    assert(got.at(1) == std::make_pair(SpillKind::phase, std::string("build")));
    assert(got.at(2).second.empty());
    assert(got.at(3).second.size() == kBigLine);

    // resuming from a returned offset yields only what is new
    auto const mid = spill.drain(0, [](SpillKind, std::string_view) -> void {});
    assert(mid == spill.written());
    spill.append(SpillKind::line, "later");
    Records rest;
    auto const end = spill.drain(mid, [&](SpillKind kind, std::string_view text) -> void { rest.emplace_back(kind, std::string(text)); });
    assert(end == spill.written());
    assert(rest.size() == 1 && rest.front().second == "later");
}

void clampsOversizedLines()
{
    LogSpill spill;
    spill.append(SpillKind::line, std::string(LogSpill::kMaxText + kOverflow, 'x'));
    auto const got = readAll(spill);
    assert(got.size() == 1 && got.at(0).second.size() == LogSpill::kMaxText);
}

void stopsAtTheCap()
{
    LogSpill spill(kSmallCap);
    for (int i = 0; i < kSmallCapRecords; i++) {
        spill.append(SpillKind::line, "0123456789");
    }
    auto const got = readAll(spill);
    assert(!got.empty());
    assert(got.back().second == LogSpill::kTruncated);
    auto const size = spill.written();
    spill.append(SpillKind::line, "dropped");
    assert(spill.written() == size);

    // a cap below one record still ends in the marker
    LogSpill tiny(kTinyCap);
    tiny.append(SpillKind::line, "hello");
    auto const only = readAll(tiny);
    assert(only.size() == 1 && only.front().second == LogSpill::kTruncated);
}

void unusableTmpdirLosesTheReplayOnly()
{
    setenv("TMPDIR", "/nonexistent-dir", 1); // NOLINT(concurrency-mt-unsafe): single-threaded here.
    LogSpill spill;
    spill.append(SpillKind::line, "a");
    spill.append(SpillKind::line, "b");
    assert(readAll(spill).empty() && spill.written() == 0);
    unsetenv("TMPDIR"); // NOLINT(concurrency-mt-unsafe): single-threaded here.
}

// A chatty owner with an early and a late follower: both see every line in order.
void followersSeeEveryLine()
{
    LogSpill spill;
    std::mutex mutex;
    std::condition_variable wakeup;
    bool finished = false;
    constexpr int lines = kChattyLines;
    Records early;
    Records late;
    std::thread first([&] -> void { early = follow(spill, mutex, wakeup, finished); });
    std::thread owner([&] -> void {
        for (int i = 0; i < lines; i++) {
            spill.append(SpillKind::line, "line " + std::to_string(i));
            const std::scoped_lock lock(mutex);
            wakeup.notify_all();
        }
        const std::scoped_lock lock(mutex);
        finished = true;
        wakeup.notify_all();
    });
    while (spill.written() < kLateStart) {
        std::this_thread::yield();
    }
    std::thread second([&] -> void { late = follow(spill, mutex, wakeup, finished); });
    owner.join();
    first.join();
    second.join();
    for (auto const * got : {&early, &late}) {
        assert(got->size() == lines);
        for (int i = 0; i < lines; i++) {
            assert(got->at(static_cast<std::size_t>(i)).second == "line " + std::to_string(i));
        }
    }
}

void cancelledReaderGivesUp()
{
    LogSpill spill;
    std::mutex mutex;
    std::condition_variable wakeup;
    bool const finished = false;
    int polls = 0;
    auto const done = nixgrpc::followSpill(
        spill, mutex, wakeup, finished, std::chrono::milliseconds{1}, [&] -> bool { return ++polls > 3; },
        [](SpillKind, std::string_view) -> void {});
    assert(!done);
}

} // namespace

auto main() -> int
try {
    roundTrip();
    clampsOversizedLines();
    stopsAtTheCap();
    unusableTmpdirLosesTheReplayOnly();
    followersSeeEveryLine();
    cancelledReaderGivesUp();
    return 0;
} catch (...) {
    return 1;
}
