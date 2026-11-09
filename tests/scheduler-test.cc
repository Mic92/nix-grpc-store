// Unit tests for the placement core (no gRPC).

#include <algorithm>
#include <cassert>
#include <cstddef>
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

// Wants for systems no worker offers are kept, but only for a few systems,
// and a system is forgotten once nothing wants it.
void testUnofferedSystems()
{
    Core shard;
    for (size_t idx = 0; idx < Core::maxUnofferedSystems; idx++) {
        auto const name = "sys" + std::to_string(idx);
        assert(shard.admitsSystem(name));
        shard.want(1, {.drvPath = name + ".drv", .inputs = {}, .system = name, .features = {}, .cpHintMs = 0}, 0);
    }
    assert(!shard.admitsSystem("one-too-many"));
    assert(shard.admitsSystem("sys0"));
    shard.cancel(1, "sys0.drv");
    assert(shard.admitsSystem("one-too-many"));
    assert(!shard.stats().contains({.system = "sys0", .features = ""}));
}

// A worker reporting a drv that was still queued takes it, and its
// followers must be told.
void testHelloAdoptsQueued()
{
    Core shard;
    std::vector<Core::Superseded> sup;
    shard.want(1, {.drvPath = "q.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    auto const wkr = shard.hello(
        {.addr = "w", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {{"q.drv", 7}}}, sup);
    assert(sup.size() == 1 && sup.at(0).loser == noWorker);
    assert(shard.entry(sup.at(0).drv)->worker == wkr && shard.entry(sup.at(0).drv)->assignId == 7);
}

// Followers of a queued entry hear when a capable worker comes or goes.
void testPlaceabilityFlips()
{
    Core shard;
    std::vector<Core::Superseded> sup;
    std::vector<DrvId> changed;
    auto const res = shard.want(1, {.drvPath = "k.drv", .inputs = {}, .system = "x86_64", .features = {"kvm"}, .cpHintMs = 0}, 0);
    shard.markUnplaceable(res.drv, true);
    shard.hello({.addr = "plain", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.placeabilityChanges(changed);
    assert(changed.empty());
    auto const kvm = shard.hello({.addr = "kvm", .systems = {"x86_64"}, .features = {"kvm"}, .maxJobs = 1, .running = {}}, sup);
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && !shard.entry(res.drv)->unplaceable);
    changed.clear();
    shard.workerGone(kvm);
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && shard.entry(res.drv)->unplaceable);
}

// A given-back drv goes to another worker, or back to the same one if
// nobody else can run it.
void testGiveBack()
{
    Core shard;
    std::vector<Core::Superseded> sup;
    std::vector<Assign> out;
    auto const first = shard.hello({.addr = "a", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    auto const res = shard.want(1, {.drvPath = "g.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first);
    out.clear();
    shard.giveBack(first, "g.drv");
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first); // sole worker
    out.clear();
    auto const second = shard.hello({.addr = "b", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.giveBack(first, "g.drv");
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == second);
    out.clear();
    shard.want(2, {.drvPath = "h.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first);
    out.clear();
    // Another worker can run it, just not now: it waits rather than going back.
    shard.giveBack(second, "g.drv");
    shard.dispatch(out);
    assert(out.empty() && shard.entry(res.drv)->worker == noWorker);
}

// Given back by w0, it goes to the most free other worker, w2; given back
// by w2 too, to w1, though w0 has more slots free and the input it needs.
void testGiveBackPassesOverEveryWorkerThatGaveItBack()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 3);
    auto const w1 = hello(shard, "w1", 1);
    auto const w2 = hello(shard, "w2", 2);
    shard.done(w0, "x.drv", {{"/nix/store/big", 1'000'000'000}});
    shard.want(1, {.drvPath = "a.drv", .inputs = {"/nix/store/big"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    shard.giveBack(w0, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w2);
    shard.giveBack(w2, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == w1);
}

// w0 (2 slots) and w1 each give it back as soon as they get it: they take
// turns, and each is remembered once, the latest last.
void testRepeatGiveBacksTakeTurns()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    std::vector<WorkerId> placed;
    for (int round = 0; round < 5; round++) {
        assert(out.size() == placed.size() + 1);
        placed.push_back(out.back().worker);
        shard.giveBack(placed.back(), "a.drv");
        shard.dispatch(out);
    }
    placed.push_back(out.back().worker);
    assert((placed == std::vector<WorkerId>{w0, w1, w0, w1, w0, w1}));
    assert((shard.entry(res.drv)->avoid == std::vector<WorkerId>{w1, w0}));
}

// w2 is busy: given back by w0 and then by w1, it goes back to the first of
// them rather than wait for w2. Only the last to give it back is passed over.
void testGiveBackDoesNotWaitForABusyWorkerThatNeverHadIt()
{
    Core shard;
    std::vector<Assign> out;
    auto const w2 = hello(shard, "w2", 1);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    hello(shard, "w0", 1);
    hello(shard, "w1", 1);
    auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (size_t round = 1; round <= 2; round++) {
        shard.dispatch(out);
        assert(out.size() == round + 1 && out[round].worker != w2);
        shard.giveBack(out[round].worker, "a.drv");
    }
    shard.dispatch(out);
    assert(out.size() == 4 && out[3].worker == out[1].worker && out[3].drv == res.drv);
}

// w1 has no slot, or is draining: given back by w0, it goes back to w0
// rather than wait for w1.
void testGiveBackWaitsOnlyForAWorkerThatCanRunIt()
{
    for (bool const draining : {false, true}) {
        Core shard;
        std::vector<Assign> out;
        auto const w0 = hello(shard, "w0", 1);
        auto const w1 = hello(shard, "w1", draining ? 1 : 0);
        shard.setDraining(w1, draining);
        shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
        shard.dispatch(out);
        shard.giveBack(w0, "a.drv");
        shard.dispatch(out);
        assert(out.size() == 2 && out[0].worker == w0 && out[1].worker == w0);
    }
}

// Each worker gave it back, w1 (which built its input) first. Only the last
// to give it back is passed over, so it goes back to w1 rather than to w0,
// which has more slots free.
void testLastResortKeepsLocality()
{
    Core shard;
    std::vector<Assign> out;
    hello(shard, "w0", 3);
    auto const w1 = hello(shard, "w1", 1);
    hello(shard, "w2", 1);
    shard.done(w1, "x.drv", {{"/nix/store/big", 1'000'000'000}});
    shard.want(1, {.drvPath = "a.drv", .inputs = {"/nix/store/big"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (int round = 0; round < 3; round++) {
        shard.dispatch(out);
        shard.giveBack(out.back().worker, "a.drv");
    }
    shard.dispatch(out);
    assert(out.size() == 4 && out[3].worker == w1);
    assert(out[0].worker == w1 && out[1].worker != out[2].worker && out[2].worker != w1);
}

// Each worker gave it back, w1 last, and w1 built its input: w1 is passed
// over for locality too, so it goes to w0.
void testLastResortPassesOverTheLastForLocalityToo()
{
    Core shard;
    std::vector<Assign> out;
    auto const w1 = hello(shard, "w1", 1);
    shard.want(2, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    auto const w0 = hello(shard, "w0", 3);
    auto const w2 = hello(shard, "w2", 1);
    shard.done(w1, "x.drv", {{"/nix/store/big", 1'000'000'000}});
    shard.want(1, {.drvPath = "a.drv", .inputs = {"/nix/store/big"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (auto const giver : std::vector<WorkerId>{w0, w2, w1}) {
        shard.dispatch(out);
        assert(out.back().worker == giver);
        shard.giveBack(giver, "a.drv");
        if (giver == w2) {
            shard.done(w1, "b.drv", {});
        }
    }
    shard.dispatch(out);
    assert(out.size() == 5 && out[4].worker == w0);
}

// It needs f, which w1 lacks: given back by w0, it waits for w2 rather than
// going to w1; given back by w2 too, it goes back to w0.
void testGiveBackWaitsOnlyForAWorkerWithItsFeatures()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1, {}, {"f"});
    hello(shard, "w1", 4);
    auto const w2 = hello(shard, "w2", 1, {"busy.drv"}, {"f"});
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {"f"}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    shard.giveBack(w0, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.done(w2, "busy.drv", {});
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w2);
    shard.giveBack(w2, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == w0);
}

// A give-back from a worker that does not have it passes nobody over: given
// back by w0 after that, it goes to w1, which has the most slots free.
void testStaleGiveBackPassesNobodyOver()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 3);
    auto const w1 = hello(shard, "w1", 2);
    hello(shard, "w2", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    shard.giveBack(w1, "a.drv");
    shard.giveBack(w0, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w1);
}

// w1 is busy. Given back by w0 for draining, before w0's Load, it waits
// rather than go back to w0, which still takes other work; after w0's next
// Load, or Hello, it goes back to w0, though w2 has joined.
void testDrainingGiveBackEndsAtLoad()
{
    for (bool const viaHello : {false, true}) {
        Core shard;
        std::vector<Assign> out;
        hello(shard, "w1", 1, {"busy.drv"});
        auto const w0 = hello(shard, "w0", 2);
        auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
        shard.dispatch(out);
        shard.giveBack(w0, "a.drv", PassOver::untilLoad);
        shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
        shard.dispatch(out);
        assert(out.size() == 2 && out[1].worker == w0 && out[1].drv != res.drv);
        if (viaHello) {
            hello(shard, "w0", 2, {"b.drv"});
        } else {
            shard.setDraining(w0, false);
        }
        hello(shard, "w2", 1);
        shard.dispatch(out);
        assert(out.size() == 3 && out[2].worker == w0 && out[2].drv == res.drv);
    }
}

// w2 is busy. w0 gives it back for a reason about the drv, then again for
// one about itself: once w2 is free, w0 is still passed over.
void testWorkerWideGiveBackKeepsAnEarlierOne()
{
    Core shard;
    std::vector<Assign> out;
    auto const w2 = hello(shard, "w2", 1, {"busy.drv"});
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (auto const [giver, pass] : std::vector<std::pair<WorkerId, PassOver>>{
             {w0, PassOver::always},
             {w1, PassOver::untilNextGiveBack},
             {w0, PassOver::untilNextGiveBack},
             {w1, PassOver::untilNextGiveBack}}) {
        shard.dispatch(out);
        assert(out.back().worker == giver);
        shard.giveBack(giver, "a.drv", pass);
    }
    shard.done(w2, "busy.drv", {});
    shard.dispatch(out);
    assert(out.size() == 5 && out[4].worker == w2);
}

// w2 is busy. Given back by w0 for a reason about the cache, it goes back
// to w0 rather than wait for w2. Given back by w0 for a reason about the drv,
// then by w1, now busy, for one about the cache: it goes back to w0 rather
// than wait. Given back by w0 for one about the cache once w2 is free, it
// goes to w2: w0's earlier reason still holds.
void testCacheGiveBackPassesNobodyOver()
{
    Core shard;
    std::vector<Assign> out;
    auto const w2 = hello(shard, "w2", 1, {"busy.drv"});
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    shard.giveBack(w0, "a.drv", PassOver::never);
    shard.dispatch(out);
    assert(out.size() == 2 && out[0].worker == w0 && out[1].worker == w0);
    auto const w1 = hello(shard, "w1", 1);
    shard.giveBack(w0, "a.drv");
    shard.dispatch(out);
    shard.giveBack(w1, "a.drv", PassOver::never);
    hello(shard, "w1", 1, {"other.drv"});
    shard.dispatch(out);
    assert(out.size() == 4 && out[2].worker == w1 && out[3].worker == w0);
    shard.done(w2, "busy.drv", {});
    shard.giveBack(w0, "a.drv", PassOver::never);
    shard.dispatch(out);
    assert(out.size() == 5 && out[4].worker == w2);
}

// w0, alone, gives it back twice for no client coming; w1 and w2 join, and
// w0 gives it back once more the same way, then w1 for a reason about the
// drv: w0 is passed over no longer. The same once more, by w0 and then w2:
// it goes to w0 again.
void testRepeatWorkerWideGiveBackLastsUntilTheNext()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (int round = 0; round < 2; round++) {
        shard.dispatch(out);
        shard.giveBack(w0, "a.drv", PassOver::untilNextGiveBack);
    }
    shard.dispatch(out);
    auto const w1 = hello(shard, "w1", 1);
    auto const w2 = hello(shard, "w2", 1);
    for (auto const second : {w1, w2}) {
        shard.giveBack(w0, "a.drv", PassOver::untilNextGiveBack);
        shard.dispatch(out);
        assert(out.back().worker == second);
        shard.giveBack(second, "a.drv");
        shard.dispatch(out);
        assert(out.back().worker == w0);
    }
}

// w0 and then w1 give it back for a reason about the drv, then w0 for one
// about the cache. With w0 full, it goes to w1 rather than wait: only the
// last giver, w0, is passed over, as on main.
void testKnownGiverIsTheLastGiver()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (auto const giver : {w0, w1}) {
        shard.dispatch(out);
        shard.giveBack(giver, "a.drv");
    }
    shard.dispatch(out);
    shard.giveBack(w0, "a.drv", PassOver::never);
    hello(shard, "w0", 2, {"x.drv", "y.drv"});
    shard.dispatch(out);
    assert(out.size() == 4 && out[0].worker == w0 && out[1].worker == w1 && out[2].worker == w0 && out[3].worker == w1);
}

// w0 gives it back for a reason about the drv, then w1 for draining, and w1
// says Hello. If w1 is full, nobody is passed over and it goes to w0, as on
// main. If w1 has a slot and w2, which never had it, joins: only w0 is
// passed over, and it goes to w1.
void testExpiredDrainingGiverIsTheOnlyOneDropped()
{
    for (bool const fresh : {false, true}) {
        Core shard;
        std::vector<Assign> out;
        auto const w0 = hello(shard, "w0", 1);
        auto const w1 = hello(shard, "w1", 2);
        shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
        for (auto const [giver, pass] : std::vector<std::pair<WorkerId, PassOver>>{
                 {w1, PassOver::untilNextGiveBack}, {w0, PassOver::always}, {w1, PassOver::untilLoad}}) {
            shard.dispatch(out);
            assert(out.back().worker == giver);
            shard.giveBack(giver, "a.drv", pass);
        }
        hello(shard, "w1", 2, fresh ? std::vector<std::string_view>{"x.drv"} : std::vector<std::string_view>{"x.drv", "y.drv"});
        if (fresh) {
            hello(shard, "w2", 1);
        }
        shard.dispatch(out);
        assert(out.size() == 4 && out[3].worker == (fresh ? w1 : w0));
    }
}

// A worker serving two systems gives its slot to the more urgent drv.
void testCrossSystemPriority()
{
    for (auto const * urgent : {"x86_64", "i686"}) {
        Core shard;
        std::vector<Core::Superseded> sup;
        std::vector<Assign> out;
        shard.hello({.addr = "w", .systems = {"x86_64", "i686"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
        for (auto const * system : {"x86_64", "i686"}) {
            auto const hint = std::string_view(system) == urgent ? 1000.0 : 0.0;
            shard.want(1, {.drvPath = std::string(system) + ".drv", .inputs = {}, .system = system, .features = {}, .cpHintMs = hint}, 0);
        }
        shard.dispatch(out);
        assert(out.size() == 1 && shard.entry(out.at(0).drv)->system == urgent);
    }
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

// A worker with max_jobs 0 runs nothing and offers no features. A drv given
// back by the only other worker goes back there, and is unplaceable without it.
void testNoSlotsRunsNothing()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1, {}, {"kvm"});
    hello(shard, "w1", 0, {}, {"f"});
    assert((shard.offeredFeatures("x") == std::vector<std::string>{"kvm"}));
    auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    shard.giveBack(w0, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0);
    shard.workerGone(w0);
    assert(!shard.placeable(res.drv));
    std::vector<DrvId> changed;
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && changed[0] == res.drv && shard.entry(res.drv)->unplaceable);
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

// A build holds its worker's slot until its Done, followed or not: one known
// only from the worker's Hello after a scheduler restart, or one whose last
// client cancelled it.
void testRunningFollowsTheWorker()
{
    Core shard;
    auto const w0 = hello(shard, "w0", 1, {"orphan.drv"});
    auto const stat = [&] -> Core::Stats { return shard.stats()[{.system = "x", .features = ""}]; };
    assert(stat().slots == 1 && stat().free == 0 && stat().running == 1);
    shard.done(w0, "orphan.drv", {});
    assert(stat().free == 1 && stat().running == 0);
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.cancel(1, "a.drv") == w0);
    assert(stat().free == 0 && stat().running == 1);
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
    testRunningFollowsTheWorker();
    testDraining();
    testFeatures();
    testSystems();
    testUnofferedSystems();
    testHelloAdoptsQueued();
    testGiveBack();
    testGiveBackPassesOverEveryWorkerThatGaveItBack();
    testRepeatGiveBacksTakeTurns();
    testGiveBackDoesNotWaitForABusyWorkerThatNeverHadIt();
    testGiveBackWaitsOnlyForAWorkerThatCanRunIt();
    testLastResortKeepsLocality();
    testLastResortPassesOverTheLastForLocalityToo();
    testGiveBackWaitsOnlyForAWorkerWithItsFeatures();
    testStaleGiveBackPassesNobodyOver();
    testDrainingGiveBackEndsAtLoad();
    testWorkerWideGiveBackKeepsAnEarlierOne();
    testCacheGiveBackPassesNobodyOver();
    testRepeatWorkerWideGiveBackLastsUntilTheNext();
    testKnownGiverIsTheLastGiver();
    testExpiredDrainingGiverIsTheOnlyOneDropped();
    testCrossSystemPriority();
    testPlaceabilityFlips();
    testNoSlotsRunsNothing();
    testStaleDoneAndUnknown();
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
