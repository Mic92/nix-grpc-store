#include <atomic>
#include <cassert>
#include <exception>
#include <iostream>
#include <memory>
#include <string_view>
#include <thread>

#include "nix_remote.pb.h"
#include "reverse-registry.hh"
#include "reverse-transport.hh"

auto main() -> int
try {
    nixgrpc::ReverseRegistry registry;
    int firstCancelled = 0;
    int secondCancelled = 0;
    assert(!registry.registerWorker("worker", "worker", [] -> void {})); // standby
    registry.setActive(true);
    assert(!registry.registerWorker("", "worker", [] -> void {}));
    assert(!registry.registerWorker("other", "worker", [] -> void {}));
    assert(!registry.registerWorker(std::string_view("worker\0extra", 12), std::string_view("worker\0extra", 12), [] -> void {}));
    assert(!registry.registerWorker("worker", "worker", {}));
    assert(!registry.find("worker"));

    auto firstTransport = std::make_shared<nixgrpc::ReverseTransport>();
    auto firstHandle = registry.registerWorker("worker", "worker", [&] -> void {
        ++firstCancelled;
        // A session teardown can synchronously inspect the replacement.
        assert(registry.find("worker"));
    }, {}, firstTransport);
    assert(firstHandle);
    auto const & first = *firstHandle;
    assert(firstTransport->responses.open(1));
    assert(registry.current(first));
    assert(!registry.find("other")); // no fallback to a different worker
    auto otherHandle = registry.registerWorker("other", "other", [] -> void {});
    assert(otherHandle);
    auto const & other = *otherHandle;
    auto secondHandle = registry.registerWorker("worker", "worker", [&] -> void { ++secondCancelled; });
    assert(secondHandle);
    auto const & second = *secondHandle;
    assert(firstCancelled == 1 && second->generation > first->generation);
    assert(!registry.current(first) && registry.current(second));
    nix::remote::ReverseWorkerMsg stale;
    stale.set_stream_id(1);
    stale.mutable_data()->set_data("displaced");
    assert(!firstTransport->responses.accept(stale));
    assert(!firstTransport->responses.waitTake(1, stale, {}));
    registry.disconnected(first); // late close must not remove replacement
    assert(registry.find("worker") == second);
    registry.disconnected(other);
    assert(!registry.find("other") && registry.current(second));

    registry.setActive(false); // election loss invalidates every route
    assert(secondCancelled == 1 && !registry.find("worker") && !registry.current(second));
    registry.disconnected(second);
    registry.setActive(true);
    int thirdCancelled = 0;
    auto thirdHandle = registry.registerWorker("worker", "worker", [&] -> void { ++thirdCancelled; });
    assert(thirdHandle);
    auto const & third = *thirdHandle;
    assert(third->generation > second->generation && registry.current(third));
    registry.disconnected(second);
    assert(registry.current(third));
    registry.disconnected(third);
    assert(!registry.find("worker") && thirdCancelled == 1);

    int normalCloseCancelled = 0;
    auto normalCloseHandle = registry.registerWorker("worker", "worker", [&] -> void { ++normalCloseCancelled; });
    assert(normalCloseHandle);
    auto const & normalClose = *normalCloseHandle;
    registry.disconnected(normalClose, false); // a returning handler must preserve its status
    assert(!registry.find("worker") && normalCloseCancelled == 0);

    // Interleave a late stream close with registration of a replacement.
    // Regardless of ordering, closing the old stream cannot evict the new.
    auto oldHandle = registry.registerWorker("worker", "worker", [] -> void {});
    assert(oldHandle);
    auto old = *oldHandle;
    std::atomic<bool> start{false};
    std::thread closeOld([&] -> void {
        while (!start.load()) {
            std::this_thread::yield();
        }
        registry.disconnected(old);
    });
    start = true;
    auto replacementHandle = registry.registerWorker("worker", "worker", [] -> void {});
    assert(replacementHandle);
    auto const & replacement = *replacementHandle;
    closeOld.join();
    assert(registry.current(replacement) && registry.find("worker") == replacement);
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << '\n';
    return 1;
}
