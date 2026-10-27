#pragma once
// The client's build run as one state machine. No locking or I/O: the
// caller holds its lock around every call and sends what take*() returns.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace nixgrpc::exec {

using JobId = uint32_t;
using Clock = std::chrono::steady_clock;

struct Placement
{
    std::string addr;
    uint64_t assignId = 0;
    std::string system; // the worker's native one, empty from older schedulers
};

enum class Outcome : uint8_t {
    built,            // or failed: the build's own result tells
    cached,
    dependencyFailed,
    lost,             // the scheduler is gone for good
    unplaceable,      // no worker could take it within the grace
    gaveUp,           // bounced too often
    cancelled,        // another build failed and keep-going is off
};

namespace state {
struct Blocked
{
    size_t waiting; // inputs not done
};

struct Wantable
{
};

struct Wanted
{
    std::optional<Clock::time_point> unplaceableSince;
    std::string reason;
};

struct Assigned
{
    Placement at;
};

struct Running
{
    Placement at;
    // A later Assigned elsewhere: followed when this worker bounces us.
    std::optional<Placement> redirect;
};

struct Done
{
    Outcome outcome;
    bool succeeded;
    std::string message;
};
} // namespace state

using JobState =
    std::variant<state::Blocked, state::Wantable, state::Wanted, state::Assigned, state::Running, state::Done>;

class Executor
{
public:
    static constexpr unsigned defaultRescheduleRetries = 8;

    struct Config
    {
        size_t threads = 1;
        unsigned rescheduleRetries = defaultRescheduleRetries;
        bool keepGoing = true;
    };

    explicit Executor(Config cfg)
        : cfg(cfg)
    {
    }

    // Inputs must be added first.
    auto add(double prio, const std::vector<JobId> & inputs) -> JobId
    {
        auto const jid = static_cast<JobId>(jobs.size());
        jobs.push_back({.state = state::Blocked{.waiting = 0}, .prio = prio, .dependants = {}, .failedInput = {}, .bounced = 0});
        remaining++;
        size_t waiting = 0;
        for (auto const input : inputs) {
            if (!std::holds_alternative<state::Done>(jobs.at(input).state)) {
                jobs.at(input).dependants.push_back(jid);
                waiting++;
            } else if (!std::get<state::Done>(jobs.at(input).state).succeeded) {
                jobs.at(jid).failedInput = input;
            }
        }
        std::get<state::Blocked>(jobs.at(jid).state).waiting = waiting;
        if (waiting == 0) {
            release(jid);
        }
        return jid;
    }

        void blockOn(JobId jid, JobId input)
    {
        auto & job = jobs.at(jid);
        if (!std::holds_alternative<state::Running>(job.state)) {
            return;
        }
        if (const auto * done = std::get_if<state::Done>(&jobs.at(input).state)) {
            if (!done->succeeded) {
                job.failedInput = input;
            }
            to(jid, state::Blocked{.waiting = 0});
            release(jid);
            return;
        }
        jobs.at(input).dependants.push_back(jid);
        to(jid, state::Blocked{.waiting = 1});
    }

    [[nodiscard]] auto at(JobId jid) const -> const JobState &
    {
        return jobs.at(jid).state;
    }

        [[nodiscard]] auto failedInput(JobId jid) const -> std::optional<JobId>
    {
        return jobs.at(jid).failedInput;
    }

    [[nodiscard]] auto remainingJobs() const -> size_t
    {
        return remaining;
    }

    // A reconnect budget restarts when this moved.
    [[nodiscard]] auto progress() const -> uint64_t
    {
        return progressed;
    }

    // ---- scheduler stream

    // Placed jobs are Wanted again too, so the scheduler keeps their builds.
    void connected()
    {
        stream = true;
        for (JobId jid = 0; jid < jobs.size(); jid++) {
            auto & job = jobs.at(jid);
            if (std::holds_alternative<state::Wanted>(job.state)) {
                to(jid, state::Wantable{});
                wantable.push_back(jid);
            } else if (std::holds_alternative<state::Assigned>(job.state)
                       || std::holds_alternative<state::Running>(job.state)) {
                rewant.push_back(jid);
            }
        }
    }

    void disconnected()
    {
        stream = false;
    }

    // Capped at the build threads, so the scheduler holds no slot we can't use.
    auto takeWants() -> std::vector<JobId>
    {
        std::vector<JobId> out;
        if (!stream) {
            return out;
        }
        out = std::exchange(rewant, {});
        std::erase_if(wantable, [&](JobId jid) -> bool { return !std::holds_alternative<state::Wantable>(jobs.at(jid).state); });
        auto const room = std::min(wantable.size(), cfg.threads > active ? cfg.threads - active : 0);
        std::ranges::partial_sort(
            wantable, wantable.begin() + static_cast<std::ptrdiff_t>(room),
            [&](JobId lhs, JobId rhs) -> bool { return jobs.at(lhs).prio > jobs.at(rhs).prio; });
        for (size_t idx = 0; idx < room; idx++) {
            to(wantable.at(idx), state::Wanted{});
            out.push_back(wantable.at(idx));
        }
        wantable.erase(wantable.begin(), wantable.begin() + static_cast<std::ptrdiff_t>(room));
        return out;
    }

    // A write failed: these go out again after the next connect.
    void unsent(const std::vector<JobId> & ids)
    {
        for (auto const jid : ids) {
            if (std::holds_alternative<state::Wanted>(jobs.at(jid).state)) {
                to(jid, state::Wantable{});
                wantable.push_back(jid);
            }
        }
    }

    auto takeCancels() -> std::vector<JobId>
    {
        return std::exchange(cancels, {});
    }

    void onAssigned(JobId jid, Placement placement)
    {
        auto & job = jobs.at(jid);
        if (placement.addr.empty()) {
            return;
        }
        if (auto * run = std::get_if<state::Running>(&job.state)) {
            if (placement.addr != run->at.addr) {
                run->redirect = std::move(placement);
            }
            return;
        }
        if (auto * asg = std::get_if<state::Assigned>(&job.state)) {
            asg->at = std::move(placement);
            return;
        }
        if (std::holds_alternative<state::Wanted>(job.state)) {
            progressed++;
            to(jid, state::Assigned{.at = std::move(placement)});
            ready.push_back(jid);
        }
    }

    void onCached(JobId jid)
    {
        if (std::holds_alternative<state::Wanted>(jobs.at(jid).state)) {
            settle(jid, {.outcome = Outcome::cached, .succeeded = true, .message = {}});
        }
    }

        auto onUnplaceable(JobId jid, std::string reason, Clock::time_point now) -> bool
    {
        auto * wanted = std::get_if<state::Wanted>(&jobs.at(jid).state);
        if (wanted == nullptr) {
            return false;
        }
        wanted->reason = std::move(reason);
        bool const first = !wanted->unplaceableSince;
        if (first) {
            wanted->unplaceableSince = now;
        }
        return first;
    }

    void onPlaceable(JobId jid)
    {
        if (auto * wanted = std::get_if<state::Wanted>(&jobs.at(jid).state)) {
            wanted->unplaceableSince.reset();
        }
    }

    // Fails what stayed unplaceable past `grace`. When to call again.
    auto reap(Clock::time_point now, Clock::duration grace) -> Clock::time_point
    {
        auto wake = now + grace;
        for (JobId jid = 0; jid < jobs.size(); jid++) {
            auto * wanted = std::get_if<state::Wanted>(&jobs.at(jid).state);
            if (wanted == nullptr || !wanted->unplaceableSince) {
                continue;
            }
            if (auto const due = *wanted->unplaceableSince + grace; due > now) {
                wake = std::min(wake, due);
            } else {
                settle(jid, {.outcome = Outcome::unplaceable, .succeeded = false, .message = wanted->reason});
            }
        }
        return wake;
    }

    // ---- build threads

        auto next() -> std::optional<std::pair<JobId, Placement>>
    {
        while (!ready.empty()) {
            auto const jid = ready.front();
            ready.pop_front();
            if (auto * asg = std::get_if<state::Assigned>(&jobs.at(jid).state)) {
                auto placement = std::move(asg->at);
                to(jid, state::Running{.at = placement, .redirect = {}});
                return std::pair(jid, std::move(placement));
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] auto hasReady() const -> bool
    {
        return !ready.empty();
    }

    void finished(JobId jid, bool succeeded)
    {
        if (std::holds_alternative<state::Running>(jobs.at(jid).state)) {
            settle(jid, {.outcome = Outcome::built, .succeeded = succeeded, .message = {}});
        }
    }

        void bounced(JobId jid)
    {
        auto & job = jobs.at(jid);
        auto * run = std::get_if<state::Running>(&job.state);
        if (run == nullptr) {
            return;
        }
        if (run->redirect) {
            auto placement = std::move(*run->redirect);
            to(jid, state::Assigned{.at = std::move(placement)});
            ready.push_back(jid);
            return;
        }
        if (++job.bounced > cfg.rescheduleRetries) {
            settle(
                jid,
                {.outcome = Outcome::gaveUp,
                 .succeeded = false,
                 .message = "gave up after " + std::to_string(job.bounced) + " reschedules"});
            return;
        }
        to(jid, state::Blocked{.waiting = 0});
        release(jid);
    }

    // No scheduler any more. Placed jobs still build; everything else, now
    // or released later, fails as lost.
    void abandon()
    {
        abandoned = true;
        for (JobId jid = 0; jid < jobs.size(); jid++) {
            auto const & cur = jobs.at(jid).state;
            if (std::holds_alternative<state::Wantable>(cur) || std::holds_alternative<state::Wanted>(cur)) {
                settle(jid, lostDone());
            }
        }
    }

private:
    struct Job
    {
        JobState state;
        double prio;
        std::vector<JobId> dependants;
        std::optional<JobId> failedInput;
        unsigned bounced;
    };

    Config cfg;
    std::vector<Job> jobs;
    std::vector<JobId> wantable;
    std::vector<JobId> rewant;
    std::vector<JobId> cancels;
    std::deque<JobId> ready;
    size_t remaining = 0;
    size_t active = 0; // Wanted, Assigned or Running
    uint64_t progressed = 0;
    bool stream = false;
    bool abandoned = false;
    bool stopping = false;

    static auto lostDone() -> state::Done
    {
        return {.outcome = Outcome::lost, .succeeded = false, .message = "scheduler connection lost"};
    }

    static auto isActive(const JobState & cur) -> bool
    {
        return std::holds_alternative<state::Wanted>(cur) || std::holds_alternative<state::Assigned>(cur)
               || std::holds_alternative<state::Running>(cur);
    }

        void to(JobId jid, JobState next)
    {
        auto & cur = jobs.at(jid).state;
        active -= isActive(cur) ? 1 : 0;
        active += isActive(next) ? 1 : 0;
        cur = std::move(next);
    }

    static auto cancelledDone() -> state::Done
    {
        return {.outcome = Outcome::cancelled, .succeeded = false, .message = "cancelled: another build failed"};
    }

    auto unblock(JobId jid) -> std::optional<state::Done>
    {
        if (jobs.at(jid).failedInput) {
            return state::Done{.outcome = Outcome::dependencyFailed, .succeeded = false, .message = "dependency failed"};
        }
        if (stopping) {
            return cancelledDone();
        }
        if (abandoned) {
            return lostDone();
        }
        to(jid, state::Wantable{});
        wantable.push_back(jid);
        return std::nullopt;
    }

    void release(JobId jid)
    {
        if (auto done = unblock(jid)) {
            settle(jid, std::move(*done));
        }
    }

    using Work = std::vector<std::pair<JobId, state::Done>>;

    // Iterative: a failure can cascade through a deep graph.
    void settle(JobId first, state::Done done)
    {
        Work work;
        work.emplace_back(first, std::move(done));
        while (!work.empty()) {
            auto [jid, result] = std::move(work.back());
            work.pop_back();
            auto & job = jobs.at(jid);
            if (std::holds_alternative<state::Done>(job.state)) {
                continue;
            }
            // The scheduler would place it for nobody.
            bool const placed =
                std::holds_alternative<state::Wanted>(job.state) || std::holds_alternative<state::Assigned>(job.state);
            if (placed && stream && result.outcome != Outcome::cached) {
                cancels.push_back(jid);
            }
            bool const failed = !result.succeeded;
            to(jid, std::move(result));
            remaining--;
            progressed++;
            releaseDependants(jid, failed, work);
            if (failed && !cfg.keepGoing && !stopping) {
                stopAll(work);
            }
        }
    }

    void releaseDependants(JobId jid, bool failed, Work & work)
    {
        for (auto const dep : jobs.at(jid).dependants) {
            auto & dnt = jobs.at(dep);
            auto * blk = std::get_if<state::Blocked>(&dnt.state);
            if (blk == nullptr) {
                continue;
            }
            if (failed && !dnt.failedInput) {
                dnt.failedInput = jid;
            }
            if (--blk->waiting == 0) {
                if (auto next = unblock(dep)) {
                    work.emplace_back(dep, std::move(*next));
                }
            }
        }
    }

        void stopAll(Work & work)
    {
        stopping = true;
        for (JobId other = 0; other < jobs.size(); other++) {
            auto const & cur = jobs.at(other).state;
            if (!std::holds_alternative<state::Running>(cur) && !std::holds_alternative<state::Done>(cur)) {
                work.emplace_back(other, cancelledDone());
            }
        }
    }
};

} // namespace nixgrpc::exec
