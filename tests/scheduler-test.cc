// Unit tests for the placement core (no gRPC).

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nix/util/error.hh>

#include "scheduler.hh"

using namespace nixgrpc::sched;

// NOLINTBEGIN(*-magic-numbers, *-avoid-unchecked-container-access, bugprone-unchecked-optional-access, readability-identifier-length): test vectors, asserted sizes.
namespace {

void testHeapAgainstMap()
{
    IndexedHeap heap;
    std::map<DrvId, double> ref;
    std::mt19937 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed): reproducible
    for (int step = 0; step < 20000; step++) {
        auto const id = static_cast<DrvId>(rng() % 200);
        switch (rng() % 3) {
        case 0:
        case 1: {
            auto const key = static_cast<double>(rng() % 1000);
            heap.set(id, key);
            ref[id] = key;
            break;
        }
        default:
            heap.erase(id);
            ref.erase(id);
        }
        assert(heap.size() == ref.size());
        if (!ref.empty()) {
            auto const top = *heap.top();
            double minKey = ref.begin()->second;
            for (auto & [k, v] : ref) {
                minKey = std::min(minKey, v);
            }
            assert(ref.at(top) == minKey);
        }
    }
}

void testFreeIndex()
{
    FreeIndex fi;
    for (WorkerId w = 0; w < 5; w++) {
        fi.push(w);
    }
    assert(!fi.emptiest());
    fi.set(2, 4);
    fi.set(3, 2);
    assert(fi.maxFree() == 4 && *fi.emptiest() == 2);
    fi.set(2, 1);
    assert(*fi.emptiest() == 3);
    fi.set(3, -5); // trimmed below running
    assert(fi.get(3) == -5 && *fi.emptiest() == 2);
    fi.set(2, 0);
    assert(!fi.emptiest());
    fi.set(4, 100); // clamped bucket, value kept
    assert(fi.get(4) == 100 && *fi.emptiest() == 4);
}

auto hello(Core & shard, std::string_view addr, uint32_t jobs, const std::vector<std::string_view> & running = {}, std::vector<std::string> features = {}, std::vector<Core::Superseded> * superseded = nullptr) -> WorkerId
{
    std::vector<std::pair<std::string_view, uint64_t>> runs;
    runs.reserve(running.size());
    for (auto const drv : running) {
        runs.emplace_back(drv, 7);
    }
    std::vector<Core::Superseded> ignored;
    return shard.hello({.addr = addr, .systems = {"x"}, .features = std::move(features), .maxJobs = jobs, .running = std::move(runs)}, superseded != nullptr ? *superseded : ignored);
}

void testAssignAndDedup()
{
    Core shard;
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(2, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.empty()); // no workers
    auto const w0 = hello(shard, "10.0.0.1:1", 2);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0 && out[0].clients.size() == 2);
    // third client after assignment: immediate
    auto res = shard.want(3, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(res.assigned && res.assigned->first == w0 && res.assigned->second == out[0].assignId);
    shard.done(w0, "a.drv", {{"/nix/store/a-out", 100}});
    assert(!shard.entry(out[0].drv));
}

void testMostFreeAndLocality()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 4);
    auto const w1 = hello(shard, "w1", 4);
    // w1 produced a big input
    shard.done(w1, "x.drv", {{"/nix/store/big", 1'000'000'000}});
    shard.want(1, {.drvPath = "b.drv", .inputs = {"/nix/store/big"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w1);
    // without locality: most free (w0 has 4, w1 has 3)
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0);
}

void testPriority()
{
    Core shard;
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "low.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "high.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 100000}, 0);
    shard.want(2, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(3, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(4, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(5, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(6, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0); // 4 extra followers * 30e3 = 120e3 > 100e3
    hello(shard, "w", 1);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out[0].drv)->drvPath == "fan.drv");
    shard.done(0, "fan.drv", {});
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out[1].drv)->drvPath == "high.drv");
}

void testWorkerGoneRequeues()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.workerGone(w0);
    assert(shard.queued() == 1);
    auto const w1 = hello(shard, "w1", 1);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w1 && out[1].clients.size() == 1);
}

void testHelloReconciles()
{
    Core shard;
    std::vector<Assign> out;
    // scheduler restarted: worker says it is running a.drv
    auto const w0 = hello(shard, "w0", 2, {"a.drv"});
    auto res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(res.assigned && res.assigned->first == w0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1); // only one slot left
    shard.done(w0, "a.drv", {});
    shard.dispatch(out);
    assert(out.size() == 2);
}

// Scheduler restarted while w1 built a.drv. A client re-Wants before w1 is
// back, so it goes to w0. Then w1 says Hello: w1 keeps it, w0 is superseded
// and the client is a follower of w1's build.
void testHelloSupersedes()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    std::vector<Core::Superseded> sup;
    auto const w1 = hello(shard, "w1", 1, {"a.drv"}, {}, &sup);
    assert(sup.size() == 1 && sup[0].loser == w0);
    const auto & ent = shard.entry(sup[0].drv);
    assert(ent && ent->worker == w1 && ent->assignId == 7);
    assert(ent->followers.size() == 1 && ent->followers[0] == 1);
    assert(shard.worker(w0).running.empty());
    // w0's slot is free again for something else.
    shard.want(2, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
}

void testCancelAndClientGone()
{
    Core shard;
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(2, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(!shard.cancel(1, "a.drv"));
    assert(shard.queued() == 1);
    assert(!shard.cancel(2, "a.drv"));
    assert(shard.queued() == 0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    auto const w0 = hello(shard, "w0", 1);
    shard.dispatch(out);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    shard.clientGone(1, revokes);
    assert(revokes.size() == 1 && revokes[0].second == w0);
    assert(!shard.cancel(9, "nope.drv"));
}

void testDraining()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.setDraining(w0, true);
    auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.empty());
    assert(!shard.placeable(res.drv));
    shard.setDraining(w0, false);
    assert(shard.placeable(res.drv));
    shard.dispatch(out);
    assert(out.size() == 1);
}

void testFeatures()
{
    Core shard;
    std::vector<Assign> out;
    auto const plain = hello(shard, "plain", 4);
    auto const kvm = hello(shard, "kvm", 1, {}, {"kvm"});
    shard.want(1, {.drvPath = "vm.drv", .inputs = {}, .system = "x", .features = {"kvm"}, .cpHintMs = 9e9}, 0);
    shard.want(1, {.drvPath = "vm2.drv", .inputs = {}, .system = "x", .features = {"kvm"}, .cpHintMs = 9e9}, 0);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "gpu.drv", .inputs = {}, .system = "x", .features = {"cuda"}, .cpHintMs = 0}, 0);
    auto const gpu = *shard.findDrv("gpu.drv");
    assert(!shard.placeable(gpu));
    assert((shard.offeredFeatures("x") == std::vector<std::string>{"kvm"}));
    assert(shard.offeredFeatures("y").empty());
    shard.dispatch(out);
    // vm on kvm, a on plain; vm2 waits behind a despite higher priority; gpu unplaceable
    assert(out.size() == 2);
    assert(shard.entry(out[0].drv)->drvPath == "vm.drv" && out[0].worker == kvm);
    assert(shard.entry(out[1].drv)->drvPath == "a.drv" && out[1].worker == plain);
    assert(shard.queued() == 2);
    shard.done(kvm, "vm.drv", {});
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == kvm);
    hello(shard, "gpu", 1, {}, {"cuda", "kvm"});
    assert(shard.placeable(gpu));
    shard.dispatch(out);
    assert(out.size() == 4 && shard.entry(out[3].drv)->drvPath == "gpu.drv");
}

// Stats of the testSystems fixture after its first dispatch.
void checkStats(Core & shard)
{
    auto sts = shard.stats();
    auto const stat = [&](const char * sys, const char * feats = "") -> Core::Stats { return sts[{.system = sys, .features = feats}]; };
    assert(stat("aarch64").slots == 2 && stat("aarch64").free == 1 && stat("aarch64").running == 1);
    assert(stat("x86_64").slots == 1 && stat("x86_64").free == 0 && stat("i686").slots == 0);
    assert(stat("x86_64").queued + stat("i686").queued == 1 && stat("x86_64").running + stat("i686").running == 1);
    assert(stat("x86_64").unplaceable + stat("i686").unplaceable == 0);
    assert(stat("riscv").queued == 1 && stat("riscv").unplaceable == 1);
    // Required features are their own series.
    shard.want(1, {.drvPath = "e.drv", .inputs = {}, .system = "aarch64", .features = {"kvm", "big"}, .cpHintMs = 0}, 0);
    sts = shard.stats();
    assert(stat("aarch64", "big,kvm").queued == 1 && stat("aarch64", "big,kvm").unplaceable == 1);
    assert(stat("aarch64").queued == 0);
    shard.cancel(1, "e.drv");
}

void testSystems()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    auto const arm = shard.hello({.addr = "arm", .systems = {"aarch64"}, .features = {}, .maxJobs = 2, .running = {}}, sup);
    auto const both = shard.hello({.addr = "both", .systems = {"x86_64", "i686"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "i686", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "aarch64", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "d.drv", .inputs = {}, .system = "riscv", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    // both has one slot: a or c, not both. b goes to arm. d has nobody.
    assert(out.size() == 2);
    int onBoth = 0;
    for (auto const & asg : out) {
        const auto & ent = shard.entry(asg.drv);
        if (ent->drvPath == "b.drv") {
            assert(asg.worker == arm);
        } else {
            assert(asg.worker == both);
            onBoth++;
        }
    }
    assert(onBoth == 1);
    assert(!shard.placeable(shard.drvId("d.drv")));
    assert(shard.queued() == 2);
    checkStats(shard);
    // Slot on `both` frees: the other x86-ish drv goes there.
    shard.done(both, shard.entry(out[0].worker == both ? out[0].drv : out[1].drv)->drvPath, {});
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == both);
}

void testStaleDoneAndUnknown()
{
    Core shard;
    auto const w0 = hello(shard, "w0", 1);
    shard.done(w0, "never.drv", {});
    shard.done(w0, "never.drv", {});
    shard.workerGone(w0);
    shard.workerGone(w0);
    hello(shard, "w0", 1);
    hello(shard, "w0", 1); // duplicate hello
    assert(shard.workersUp() == 1);
    bool threw = false;
    try {
        hello(shard, "", 1);
    } catch (nix::Error &) {
        threw = true;
    }
    assert(threw);
}

} // namespace
// NOLINTEND(*-magic-numbers, *-avoid-unchecked-container-access, bugprone-unchecked-optional-access, readability-identifier-length)

auto main() -> int
try {
    testHeapAgainstMap();
    testFreeIndex();
    testAssignAndDedup();
    testMostFreeAndLocality();
    testPriority();
    testWorkerGoneRequeues();
    testHelloReconciles();
    testHelloSupersedes();
    testCancelAndClientGone();
    testDraining();
    testFeatures();
    testSystems();
    testStaleDoneAndUnknown();
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
