// A worker can have more than one reason to take no builds. It tells the
// scheduler it takes builds again only when the last one is gone, and its
// metric shows which ones hold.

#include <cstdlib>
#include <print>
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

auto run() -> int
{
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
