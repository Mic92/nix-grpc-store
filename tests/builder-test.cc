// A worker can have more than one reason to take no builds. It tells the
// scheduler it takes builds again only when the last one is gone, and its
// metric shows which ones hold.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <print>
#include <thread>
#include <vector>

#include "builder.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

namespace {

using nixgrpc::Builder;

// A scheduler ends a session whose first message is not Hello.
auto greetsFirst() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    std::vector<bool> hellos;
    builder.setSend([&](const nix::remote::WorkerMsg & msg) -> bool {
        hellos.push_back(msg.has_hello());
        return true;
    });
    builder.setUnavailable(Builder::Unavailable::lowDisk, true);
    builder.greet();
    return hellos == std::vector<bool>{true, false};
}

constexpr std::chrono::seconds hung{20};
constexpr std::chrono::milliseconds headStart{100};

auto expectOf(uint64_t assignId) -> nix::remote::Expect
{
    nix::remote::Expect exp;
    exp.set_drv_path("a.drv");
    exp.set_assign_id(assignId);
    return exp;
}

// The scheduler tells the worker and the client at the same time, on two
// streams. The client's call may be the first to arrive.
auto earlyCallWaits() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    auto call = std::async(std::launch::async, [&] -> bool { return builder.claim("a.drv", 2); });
    std::this_thread::sleep_for(headStart);
    builder.onExpect(expectOf(2));
    return call.wait_for(hung) == std::future_status::ready && call.get();
}

// The worker read a later assignment already, so this one is not on its way.
auto staleCallIsRefusedAtOnce() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    builder.onExpect(expectOf(2));
    auto call = std::async(std::launch::async, [&] -> bool { return builder.claim("b.drv", 1); });
    return call.wait_for(hung) == std::future_status::ready && !call.get();
}

auto run() -> int
{
    if (!earlyCallWaits()) {
        std::println("FAIL a call that arrives before its assignment is not served once the assignment arrives");
        return EXIT_FAILURE;
    }
    if (!staleCallIsRefusedAtOnce()) {
        std::println("FAIL a call for an assignment that is over waits for it");
        return EXIT_FAILURE;
    }
    if (!greetsFirst()) {
        std::println("FAIL a worker that takes no builds from the start says so before its Hello");
        return EXIT_FAILURE;
    }
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    std::vector<bool> told;
    builder.setSend([&](const nix::remote::WorkerMsg & msg) -> bool {
        if (msg.has_load()) {
            told.push_back(msg.load().draining());
        }
        return true;
    });
    builder.greet();

    builder.setUnavailable(Builder::Unavailable::lowDisk, true);
    builder.setUnavailable(Builder::Unavailable::stopping, true);
    builder.setUnavailable(Builder::Unavailable::lowDisk, false);
    if (told != std::vector<bool>{true}) {
        std::println("FAIL a stopping worker takes builds again once its disk has room");
        return EXIT_FAILURE;
    }
    if (metrics.unavailableNow("low_disk") != 0 || metrics.unavailableNow("stopping") != 1
        || metrics.unavailableNow("local_daemon") != 0) {
        std::println("FAIL the metric does not show stopping as the only reason");
        return EXIT_FAILURE;
    }
    builder.setUnavailable(Builder::Unavailable::stopping, false);
    if (told != std::vector<bool>{true, false}) {
        std::println("FAIL a worker with no reason left does not take builds again");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
