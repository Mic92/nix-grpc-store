// The client's executor: Wants, placements, bounces and failures.

#include <cassert>
#include <chrono>
#include <exception>
#include <iostream>
#include <variant>
#include <vector>

#include "executor.hh"

namespace {

using nixgrpc::exec::Clock;
using nixgrpc::exec::Executor;
using nixgrpc::exec::JobId;
using nixgrpc::exec::Outcome;
using nixgrpc::exec::Placement;
namespace st = nixgrpc::exec::state;

auto outcome(const Executor & exe, JobId jid) -> Outcome
{
    return std::get<st::Done>(exe.at(jid)).outcome;
}

auto place(const char * addr) -> Placement
{
    return {.addr = addr, .assignId = 1, .system = {}};
}

// Wants plus placed jobs never exceed the build threads; the urgent go first.
void wantsFollowThreads()
{
    Executor exe({.threads = 2, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const low = exe.add(1, {});
    auto const high = exe.add(9, {});
    auto const mid = exe.add(5, {});
    assert(exe.takeWants().empty()); // no stream yet
    exe.connected();
    auto wants = exe.takeWants();
    assert((wants == std::vector<JobId>{high, mid}));
    exe.onAssigned(high, place("a"));
    assert(exe.takeWants().empty());
    exe.onCached(mid);
    assert((exe.takeWants() == std::vector<JobId>{low}));
}

// A dependant is Wanted once its inputs are done, and fails with them.
void dependantsWait()
{
    Executor exe({.threads = 4, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const input = exe.add(0, {});
    auto const top = exe.add(0, {input});
    exe.connected();
    assert((exe.takeWants() == std::vector<JobId>{input}));
    exe.onAssigned(input, place("a"));
    auto const job = exe.next();
    assert(job && job->first == input);
    exe.finished(input, false);
    assert(outcome(exe, top) == Outcome::dependencyFailed);
    assert(exe.remainingJobs() == 0);
}

// A bounce follows a redirect, else asks again, and gives up eventually.
void bounces()
{
    Executor exe({.threads = 1, .rescheduleRetries = 1, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.onAssigned(job, place("b"));
    exe.bounced(job);
    auto again = exe.next();
    assert(again && again->second.addr == "b");
    exe.bounced(job);
    assert(std::holds_alternative<st::Wantable>(exe.at(job)));
    exe.takeWants();
    exe.onAssigned(job, place("c"));
    exe.next();
    exe.bounced(job);
    assert(outcome(exe, job) == Outcome::gaveUp);
}

// Without keep-going the first failure cancels what has not started, and
// the scheduler hears it.
void stopsWithoutKeepGoing()
{
    Executor exe({.threads = 3, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = false});
    auto const bad = exe.add(0, {});
    auto const other = exe.add(0, {});
    auto const later = exe.add(0, {other});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(bad, place("a"));
    exe.next();
    exe.finished(bad, false);
    assert(outcome(exe, other) == Outcome::cancelled && outcome(exe, later) == Outcome::cancelled);
    assert((exe.takeCancels() == std::vector<JobId>{other}));
}

// Unplaceable fails a job only after the grace, Placeable resets it.
void unplaceableGrace()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    auto const now = Clock::now();
    assert(exe.onUnplaceable(job, "no kvm", now));
    assert(!exe.onUnplaceable(job, "no kvm", now));
    exe.onPlaceable(job);
    exe.reap(now + std::chrono::hours(1), std::chrono::minutes(2));
    assert(std::holds_alternative<st::Wanted>(exe.at(job)));
    exe.onUnplaceable(job, "no kvm", now);
    exe.reap(now + std::chrono::hours(1), std::chrono::minutes(2));
    assert(outcome(exe, job) == Outcome::unplaceable);
}

// A reconnect re-Wants placed jobs, and abandoning fails late releases too.
void reconnectAndAbandon()
{
    Executor exe({.threads = 2, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const placed = exe.add(0, {});
    auto const dependant = exe.add(0, {placed});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(placed, place("a"));
    exe.next();
    exe.disconnected();
    exe.connected();
    assert((exe.takeWants() == std::vector<JobId>{placed}));
    exe.disconnected();
    exe.abandon();
    exe.finished(placed, true);
    assert(outcome(exe, dependant) == Outcome::lost);
}

// A missing input becomes a job that the build waits for.
void missingInput()
{
    Executor exe({.threads = 2, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    auto const input = exe.add(0, {});
    exe.blockOn(job, input);
    assert(std::holds_alternative<st::Blocked>(exe.at(job)));
    assert((exe.takeCancels() == std::vector<JobId>{job}));
    assert((exe.takeWants() == std::vector<JobId>{input}));
    exe.onCached(input);
    assert((exe.takeWants() == std::vector<JobId>{job}));
}

} // namespace

auto main() -> int
{
    try {
        wantsFollowThreads();
        dependantsWait();
        bounces();
        stopsWithoutKeepGoing();
        unplaceableGrace();
        reconnectAndAbandon();
        missingInput();
    } catch (const std::exception & err) {
        std::cerr << err.what() << "\n";
        return 1;
    }
    return 0;
}
