#include <atomic>
#include <cassert>
#include <memory>
#include <string_view>
#include <thread>

#include "reverse-registry.hh"

int main()
{
    nixgrpc::ReverseRegistry registry;
    int firstCancelled = 0;
    int secondCancelled = 0;
    assert(!registry.registerWorker("worker", "worker", [] {})); // standby
    registry.setActive(true);
    assert(!registry.registerWorker("", "worker", [] {}));
    assert(!registry.registerWorker("other", "worker", [] {}));
    assert(!registry.registerWorker(std::string_view("worker\0extra", 12), std::string_view("worker\0extra", 12), [] {}));
    assert(!registry.registerWorker("worker", "worker", {}));
    assert(!registry.find("worker"));

    auto firstTransport = std::make_shared<nixgrpc::ReverseTransport>();
    auto first = *registry.registerWorker("worker", "worker", [&] {
        ++firstCancelled;
        // A session teardown can synchronously inspect the replacement.
        assert(registry.find("worker"));
    }, {}, firstTransport);
    assert(firstTransport->responses.open(1));
    assert(registry.current(first));
    assert(!registry.find("other")); // no fallback to a different worker
    auto other = *registry.registerWorker("other", "other", [] {});
    auto second = *registry.registerWorker("worker", "worker", [&] { ++secondCancelled; });
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
    auto third = *registry.registerWorker("worker", "worker", [&] { ++thirdCancelled; });
    assert(third->generation > second->generation && registry.current(third));
    registry.disconnected(second);
    assert(registry.current(third));
    registry.disconnected(third);
    assert(!registry.find("worker") && thirdCancelled == 1);

    int normalCloseCancelled = 0;
    auto normalClose = *registry.registerWorker("worker", "worker", [&] { ++normalCloseCancelled; });
    registry.disconnected(normalClose, false); // a returning handler must preserve its status
    assert(!registry.find("worker") && normalCloseCancelled == 0);

    // Interleave a late stream close with registration of a replacement.
    // Regardless of ordering, closing the old stream cannot evict the new.
    auto old = *registry.registerWorker("worker", "worker", [] {});
    std::atomic<bool> go{false};
    std::thread closeOld([&] {
        while (!go.load()) {
            std::this_thread::yield();
        }
        registry.disconnected(old);
    });
    go = true;
    auto replacement = *registry.registerWorker("worker", "worker", [] {});
    closeOld.join();
    assert(registry.current(replacement) && registry.find("worker") == replacement);
}
