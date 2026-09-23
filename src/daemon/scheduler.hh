#pragma once
// Placement core. No I/O, no locking; ported from sim/src/sim.rs.
// Queue and free-slot index are kept per system so a full system costs O(1)
// per dispatch; a worker offering several systems sits in each index.
// Inputs arrive from the network, so every id is range-checked and every
// state transition tolerates duplicates and stale messages.

#include <algorithm>
#include <array>
#include <bit>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>


#include <nix/util/error.hh>

namespace nixgrpc::sched {

using ClientId = uint64_t;
using WorkerId = uint32_t;
using DrvId = uint32_t;
constexpr WorkerId noWorker = std::numeric_limits<WorkerId>::max();

// Everything here runs under the dispatcher's one mutex, so worst case per
// call counts. Store paths are keyed by their 32-char hash part held inline
// in an open-addressing table: no allocation per key, and growth is a flat
// copy of 40-byte slots. std::unordered_map (node re-bucketing) or heap
// string keys each stalled dispatcher-bench for 20-40 ms per rehash.
struct PathKey
{
    static constexpr size_t len = 32;
    std::array<char, len> hash{};

    // `/nix/store/<hash>-name`, or a bare name in tests.
    explicit PathKey(std::string_view path)
    {
        if (auto const slash = path.rfind('/'); slash != std::string_view::npos) {
            path.remove_prefix(slash + 1);
        }
        path = path.substr(0, len);
        std::ranges::copy(path, hash.begin());
    }
    auto operator==(const PathKey &) const -> bool = default;
    template<typename H>
    friend auto AbslHashValue(H state, const PathKey & key) -> H
    {
        return H::combine_contiguous(std::move(state), key.hash.data(), len);
    }
};
template<typename V>
using PathMap = absl::flat_hash_map<PathKey, V>;

// Min-heap of (key, id) with update-key and erase by id.
class IndexedHeap
{
    std::vector<std::pair<double, DrvId>> heap;
    std::vector<uint32_t> pos;
    static constexpr uint32_t npos = std::numeric_limits<uint32_t>::max();

    void place(size_t idx, std::pair<double, DrvId> item)
    {
        pos.at(item.second) = static_cast<uint32_t>(idx);
        heap.at(idx) = item;
    }

    void siftUp(size_t idx)
    {
        auto item = heap.at(idx);
        while (idx > 0) {
            const size_t parent = (idx - 1) / 4;
            if (heap.at(parent) <= item) {
                break;
            }
            place(idx, heap.at(parent));
            idx = parent;
        }
        place(idx, item);
    }

    void siftDown(size_t idx)
    {
        auto item = heap.at(idx);
        const size_t len = heap.size();
        for (;;) {
            const size_t first = (4 * idx) + 1;
            if (first >= len) {
                break;
            }
            size_t best = first;
            for (size_t child = first + 1; child < std::min(first + 4, len); child++) {
                if (heap.at(child) < heap.at(best)) {
                    best = child;
                }
            }
            if (item <= heap.at(best)) {
                break;
            }
            place(idx, heap.at(best));
            idx = best;
        }
        place(idx, item);
    }

public:
    [[nodiscard]] auto size() const -> size_t
    {
        return heap.size();
    }

    [[nodiscard]] auto contains(DrvId drv) const -> bool
    {
        return drv < pos.size() && pos.at(drv) != npos;
    }

    [[nodiscard]] auto top() const -> std::optional<DrvId>
    {
        return heap.empty() ? std::nullopt : std::optional(heap.front().second);
    }

    void set(DrvId drv, double key)
    {
        if (drv == npos) {
            throw nix::Error("IndexedHeap: id out of range");
        }
        if (drv >= pos.size()) {
            pos.resize(size_t{drv} + 1, npos);
        }
        if (pos.at(drv) == npos) {
            pos.at(drv) = static_cast<uint32_t>(heap.size());
            heap.emplace_back(key, drv);
            siftUp(heap.size() - 1);
            return;
        }
        auto const idx = pos.at(drv);
        auto const old = heap.at(idx).first;
        heap.at(idx).first = key;
        if (key < old) {
            siftUp(idx);
        } else {
            siftDown(idx);
        }
    }

    void erase(DrvId drv)
    {
        if (!contains(drv)) {
            return;
        }
        auto const idx = pos.at(drv);
        pos.at(drv) = npos;
        auto last = heap.back();
        heap.pop_back();
        if (idx >= heap.size()) {
            return;
        }
        place(idx, last);
        siftUp(idx);
        siftDown(pos.at(last.second));
    }
};

// Workers bucketed by free slot count: O(1) "most free" and O(1) update.
class FreeIndex
{
public:
    static constexpr int32_t maxSlots = 63;

private:
    std::vector<int32_t> free; // may go negative (draining below running)
    std::vector<uint32_t> pos;
    std::vector<bool> tracked;
    std::vector<std::vector<WorkerId>> byFree = std::vector<std::vector<WorkerId>>(maxSlots + 1);
    uint64_t nonempty = 0;

    [[nodiscard]] static auto bucket(int32_t val) -> size_t
    {
        return static_cast<size_t>(std::clamp<int32_t>(val, 0, maxSlots));
    }

    void check(WorkerId wid) const
    {
        if (wid >= free.size() || !tracked.at(wid)) {
            throw nix::Error("FreeIndex: unknown worker %d", wid);
        }
    }

public:
    // Make `wid` known with zero free slots. Ids not pushed read as zero.
    void push(WorkerId wid)
    {
        if (wid < free.size() && tracked.at(wid)) {
            return;
        }
        if (wid >= free.size()) {
            free.resize(wid + 1, 0);
            pos.resize(wid + 1, 0);
            tracked.resize(wid + 1, false);
        }
        tracked.at(wid) = true;
        pos.at(wid) = static_cast<uint32_t>(byFree.at(0).size());
        byFree.at(0).push_back(wid);
        nonempty |= uint64_t{1};
    }

    [[nodiscard]] auto get(WorkerId wid) const -> int32_t
    {
        return wid < free.size() ? free.at(wid) : 0;
    }

    void set(WorkerId wid, int32_t val)
    {
        check(wid);
        auto const from = bucket(free.at(wid));
        auto const dest = bucket(val);
        free.at(wid) = val;
        if (from == dest) {
            return;
        }
        auto & src = byFree.at(from);
        auto const idx = pos.at(wid);
        if (idx >= src.size() || src.at(idx) != wid) {
            throw nix::Error("FreeIndex: corrupt bucket for worker %d", wid);
        }
        auto const last = src.back();
        src.at(idx) = last;
        pos.at(last) = idx;
        src.pop_back();
        if (src.empty()) {
            nonempty &= ~(uint64_t{1} << from);
        }
        pos.at(wid) = static_cast<uint32_t>(byFree.at(dest).size());
        byFree.at(dest).push_back(wid);
        nonempty |= uint64_t{1} << dest;
    }

    [[nodiscard]] auto maxFree() const -> int32_t
    {
        return nonempty == 0 ? 0 : maxSlots - std::countl_zero(nonempty);
    }

    [[nodiscard]] auto emptiest() const -> std::optional<WorkerId>
    {
        auto const top = maxFree();
        if (top <= 0 || byFree.at(static_cast<size_t>(top)).empty()) {
            return std::nullopt;
        }
        return byFree.at(static_cast<size_t>(top)).front();
    }
};

struct Config
{
    static constexpr double defaultAgeWeight = 4.0;
    static constexpr double defaultFaninWeight = 30e3;
    static constexpr double defaultLocLambda = 256e6;
    double ageWeight = defaultAgeWeight;     // cp_hint ms per ms waited
    double faninWeight = defaultFaninWeight; // cp_hint ms per extra follower
    double locLambda = defaultLocLambda;     // input bytes a fully free worker is worth
};

struct Assign
{
    DrvId drv = 0;
    WorkerId worker = 0;
    uint64_t assignId = 0;
    std::vector<ClientId> clients;
};

struct Entry
{
    std::string drvPath;
    std::vector<PathKey> inputs;       // for locality only
    std::string system;
    std::vector<std::string> features; // required
    std::vector<ClientId> followers;
    WorkerId worker = noWorker;
    uint64_t assignId = 0;
    double enqMs = 0;
    double cpHint = 0;
};

struct Worker
{
    std::string addr;
    std::vector<std::string> systems;
    std::vector<std::string> features;
    int32_t maxJobs = 0;
    bool up = false;
    bool draining = false;
    std::vector<DrvId> running;
};

class Core
{
public:
    explicit Core(Config cfg = {})
        : cfg(cfg)
    {
        drvIds.reserve(initialEntries);
        entries.reserve(initialEntries);
        // To its cap so it never grows: even a flat rehash of 2^18 slots is
        // ~10 ms. 12 MB of address space, untouched until that many outputs.
        lastBuilder.reserve(maxLastBuilder);
    }

    auto drvId(std::string_view path) -> DrvId
    {
        const PathKey key(path);
        if (auto const iter = drvIds.find(key); iter != drvIds.end()) {
            return iter->second;
        }
        DrvId drv = 0;
        if (!freeIds.empty()) {
            drv = freeIds.back();
            freeIds.pop_back();
        } else {
            if (entries.size() >= maxEntries) {
                throw nix::Error("scheduler: derivation table full");
            }
            drv = static_cast<DrvId>(entries.size());
            entries.emplace_back();
        }
        drvIds.emplace(key, drv);
        return drv;
    }

    [[nodiscard]] auto findDrv(std::string_view path) const -> std::optional<DrvId>
    {
        auto const iter = drvIds.find(PathKey(path));
        return iter == drvIds.end() ? std::nullopt : std::optional(iter->second);
    }

    auto workerId(std::string_view addr) -> WorkerId
    {
        if (auto const iter = workerIds.find(std::string(addr)); iter != workerIds.end()) {
            return iter->second;
        }
        auto const wid = static_cast<WorkerId>(workers.size());
        workers.push_back(Worker{.addr = std::string(addr)});
        workerIds.emplace(std::string(addr), wid);
        return wid;
    }

    [[nodiscard]] auto entry(DrvId drv) const -> const std::optional<Entry> &
    {
        return entries.at(drv);
    }

    [[nodiscard]] auto worker(WorkerId wid) const -> const Worker &
    {
        return workers.at(wid);
    }

    [[nodiscard]] auto queued() const -> size_t
    {
        size_t total = 0;
        for (const auto & [name, state] : systems) {
            total += state.queue.size();
        }
        return total;
    }

    [[nodiscard]] auto workersUp() const -> size_t
    {
        return workersUp_;
    }

    [[nodiscard]] auto workerCount() const -> size_t
    {
        return workers.size();
    }

    struct WantResult
    {
        DrvId drv = 0;
        std::optional<std::pair<WorkerId, uint64_t>> assigned;
    };

    struct WantInfo
    {
        std::string_view drvPath;
        std::vector<std::string_view> inputs;
        std::string_view system;
        std::vector<std::string> features;
        double cpHintMs = 0;
    };

    // Caller handled the cache-hit case already.
    auto want(ClientId client, WantInfo info, double nowMs) -> WantResult
    {
        auto const drv = drvId(info.drvPath);
        auto const cpHintMs = info.cpHintMs;
        auto & slot = entries.at(drv);
        if (!slot) {
            slot = Entry{
                .drvPath = std::string(info.drvPath),
                .system = std::string(info.system),
                .features = std::move(info.features),
                .enqMs = nowMs};
            slot->inputs.reserve(info.inputs.size());
            for (auto const input : info.inputs) {
                slot->inputs.emplace_back(input);
            }
        } else if (slot->system.empty()) {
            // Known only from a worker's Hello so far.
            slot->system = std::string(info.system);
            slot->features = std::move(info.features);
        }
        // Followers of an assigned drv are told again if its worker vanishes.
        if (std::ranges::find(slot->followers, client) == slot->followers.end()) {
            slot->followers.push_back(client);
            follows[client].push_back(drv);
        }
        slot->cpHint = std::max(slot->cpHint, cpHintMs);
        if (slot->worker != noWorker) {
            return {.drv = drv, .assigned = std::pair(slot->worker, slot->assignId)};
        }
        sys(slot->system).queue.set(drv, prio(*slot));
        return {.drv = drv};
    }

    // Worker to Revoke on, if the last follower left an assigned drv.
    auto cancel(ClientId client, std::string_view drvPath) -> std::optional<WorkerId>
    {
        auto found = findDrv(drvPath);
        if (!found) {
            return std::nullopt;
        }
        return cancel(client, *found);
    }

    // `follows[client]` keeps the id until clientGone; a stale id there is
    // harmless because followers is checked, and cheaper than a linear erase per
    // cancel.
    auto cancel(ClientId client, DrvId drv) -> std::optional<WorkerId> // NOLINT(bugprone-easily-swappable-parameters)
    {
        if (drv >= entries.size()) {
            return std::nullopt;
        }
        auto & slot = entries.at(drv);
        if (!slot || std::erase(slot->followers, client) == 0) {
            return std::nullopt;
        }
        if (!slot->followers.empty()) {
            if (slot->worker == noWorker) {
                sys(slot->system).queue.set(drv, prio(*slot));
            }
            return std::nullopt;
        }
        if (slot->worker == noWorker) {
            retire(drv);
            return std::nullopt;
        }
        return slot->worker;
    }

    void clientGone(ClientId client, std::vector<std::pair<DrvId, WorkerId>> & revokes)
    {
        auto const iter = follows.find(client);
        if (iter == follows.end()) {
            return;
        }
        auto const drvs = std::move(iter->second);
        follows.erase(iter);
        for (auto const drv : drvs) {
            if (auto wid = cancel(client, drv)) {
                revokes.emplace_back(drv, *wid);
            }
        }
    }

    struct HelloInfo
    {
        std::string_view addr;
        std::vector<std::string> systems; // first is the native one
        std::vector<std::string> features;
        uint32_t maxJobs = 0;
        std::vector<std::pair<std::string_view, uint64_t>> running; // running + expecting, with assignId
    };

    // A reported drv that this scheduler had meanwhile placed on `loser`:
    // the reporter keeps it, the caller revokes `loser` and re-points the
    // followers with a fresh Assigned.
    struct Superseded
    {
        DrvId drv;
        WorkerId loser;
    };

    auto hello(const HelloInfo & info, std::vector<Superseded> & superseded) -> WorkerId
    {
        if (info.addr.empty()) {
            throw nix::Error("scheduler: Hello without addr");
        }
        auto const wid = workerId(info.addr);
        if (workers.at(wid).up) {
            workerGone(wid);
        }
        auto & wkr = workers.at(wid);
        wkr.up = true;
        workersUp_++;
        wkr.draining = false;
        wkr.systems = info.systems;
        for (const auto & name : wkr.systems) {
            sys(name).free.push(wid);
        }
        wkr.features = info.features;
        wkr.maxJobs = static_cast<int32_t>(std::min<uint32_t>(info.maxJobs, FreeIndex::maxSlots));
        for (auto [path, assignId] : info.running) {
            auto const drv = drvId(path);
            auto & slot = entries.at(drv);
            if (!slot) {
                slot = Entry{.drvPath = std::string(path)};
            }
            if (slot->worker != noWorker && slot->worker != wid) {
                // Placed since our restart, seconds old. The reporter is further along.
                superseded.push_back({.drv = drv, .loser = slot->worker});
                std::erase(workers.at(slot->worker).running, drv);
                refreshFree(slot->worker);
            }
            slot->worker = wid;
            slot->assignId = assignId;
            sys(slot->system).queue.erase(drv);
            if (std::ranges::find(wkr.running, drv) == wkr.running.end()) {
                wkr.running.push_back(drv);
            }
        }
        refreshFree(wid);
        return wid;
    }

    void
    done(WorkerId wid, std::string_view drvPath, const std::vector<std::pair<std::string_view, uint64_t>> & outputs)
    {
        auto & wkr = workers.at(wid);
        for (auto [path, size] : outputs) {
            rememberBuilder(path, wid, size);
        }
        auto found = findDrv(drvPath);
        if (!found) {
            return;
        }
        std::erase(wkr.running, *found);
        refreshFree(wid);
        auto & slot = entries.at(*found);
        if (slot && slot->worker == wid) {
            retire(*found);
        }
    }

    void setDraining(WorkerId wid, bool draining)
    {
        workers.at(wid).draining = draining;
        refreshFree(wid);
    }

    // Requeues what still has followers; a reconnecting worker's Hello
    // reconciles.
    void workerGone(WorkerId wid)
    {
        auto & wkr = workers.at(wid);
        if (wkr.up) {
            workersUp_--;
        }
        wkr.up = false;
        refreshFree(wid);
        for (auto const drv : std::exchange(wkr.running, {})) {
            if (drv >= entries.size()) {
                continue;
            }
            auto & slot = entries.at(drv);
            if (!slot || slot->worker != wid) {
                continue;
            }
            slot->worker = noWorker;
            if (slot->followers.empty()) {
                retire(drv);
            } else {
                sys(slot->system).queue.set(drv, prio(*slot));
            }
        }
    }

    void dispatch(std::vector<Assign> & out)
    {
        for (auto & [name, state] : systems) {
            dispatch(state, out);
        }
    }

    // Some connected, non-draining worker could ever run it.
    [[nodiscard]] auto placeable(const Entry & ent) const -> bool
    {
        return std::ranges::any_of(
            workers, [&](const Worker & wkr) -> bool { return wkr.up && !wkr.draining && offers(wkr, ent); });
    }

    // Features of the connected, non-draining workers that serve the system.
    [[nodiscard]] auto offeredFeatures(const std::string & system) const -> std::vector<std::string>
    {
        std::set<std::string> feats;
        for (const auto & wkr : workers) {
            if (wkr.up && !wkr.draining && std::ranges::find(wkr.systems, system) != wkr.systems.end()) {
                feats.insert(wkr.features.begin(), wkr.features.end());
            }
        }
        return {feats.begin(), feats.end()};
    }

    [[nodiscard]] auto placeable(DrvId drv) const -> bool
    {
        const auto & slot = entries.at(drv);
        return slot && placeable(*slot);
    }

    struct StatsKey
    {
        std::string system;
        std::string features; // sorted, comma-joined
        auto operator<=>(const StatsKey &) const = default;
    };

    struct Stats
    {
        // Keyed by what the derivation requires.
        size_t queued = 0;      // wanted, not placed
        size_t unplaceable = 0; // of those, no connected worker could ever run it
        size_t running = 0;
        // Keyed by what the worker offers (native system).
        size_t slots = 0; // maxJobs of up, non-draining workers
        size_t free = 0;
    };

    [[nodiscard]] static auto featureKey(std::vector<std::string> feats) -> std::string
    {
        std::ranges::sort(feats);
        std::string key;
        for (const auto & feat : feats) {
            key += key.empty() ? "" : ",";
            key += feat;
        }
        return key;
    }

    // O(entries + workers), for metrics.
    [[nodiscard]] auto stats() const -> std::map<StatsKey, Stats>
    {
        std::map<StatsKey, Stats> out;
        for (const auto & [name, state] : systems) {
            out[{.system = name, .features = ""}];
        }
        for (const auto & wkr : workers) {
            if (!wkr.up || wkr.draining || wkr.systems.empty()) {
                continue;
            }
            auto & sst = out[{.system = wkr.systems.front(), .features = featureKey(wkr.features)}];
            sst.slots += static_cast<size_t>(wkr.maxJobs);
            sst.free += static_cast<size_t>(std::max(0, wkr.maxJobs - static_cast<int32_t>(wkr.running.size())));
        }
        // placeable() is O(workers). Entries sharing system+features share the answer.
        std::map<StatsKey, bool> memo;
        for (const auto & slot : entries) {
            if (!slot || slot->followers.empty()) {
                continue;
            }
            const StatsKey key{.system = slot->system, .features = featureKey(slot->features)};
            auto & sst = out[key];
            if (slot->worker != noWorker) {
                sst.running++;
                continue;
            }
            sst.queued++;
            auto [iter, fresh] = memo.try_emplace(key, false);
            if (fresh) {
                iter->second = placeable(*slot);
            }
            if (!iter->second) {
                sst.unplaceable++;
            }
        }
        return out;
    }

    // Bounds on soft state; inputs come from the network.
    static constexpr size_t maxEntries = 1U << 22U;
    static constexpr size_t maxLastBuilder = 1U << 18U;
    static constexpr size_t initialEntries = 1U << 16U;

private:
    Config cfg;
    PathMap<DrvId> drvIds; // live entries only
    std::vector<DrvId> freeIds;
    std::unordered_map<std::string, WorkerId> workerIds;
    std::vector<std::optional<Entry>> entries;
    std::vector<Worker> workers;
    size_t workersUp_ = 0;
    struct SystemState
    {
        IndexedHeap queue;
        FreeIndex free; // a worker sits in each of its systems' index with the same count
    };
    std::unordered_map<std::string, SystemState> systems;
    std::unordered_map<ClientId, std::vector<DrvId>> follows;
    // Output path -> who built it last, for input locality. A tie-breaker
    // only, so on overflow the table is simply dropped.
    PathMap<std::pair<WorkerId, uint64_t>> lastBuilder;
    uint64_t lastAssignId = 0;

    // Entry is finished or abandoned: free the slot and its id for reuse.
    void retire(DrvId drv)
    {
        auto & slot = entries.at(drv);
        if (!slot) {
            return;
        }
        sys(slot->system).queue.erase(drv);
        drvIds.erase(PathKey(slot->drvPath));
        slot.reset();
        freeIds.push_back(drv);
    }

    void rememberBuilder(std::string_view path, WorkerId wid, uint64_t size)
    {
        const PathKey key(path);
        if (auto const iter = lastBuilder.find(key); iter != lastBuilder.end()) {
            iter->second = {wid, size};
            return;
        }
        if (lastBuilder.size() >= maxLastBuilder) {
            lastBuilder.clear();
        }
        lastBuilder.emplace(key, std::pair(wid, size));
    }

    auto sys(const std::string & name) -> SystemState &
    {
        return systems.try_emplace(name).first->second;
    }

    void dispatch(SystemState & state, std::vector<Assign> & out)
    {
        auto & queue = state.queue;
        std::vector<std::pair<DrvId, double>> skipped; // need a feature no free worker has
        while (auto head = queue.top()) {
            if (state.free.maxFree() <= 0) {
                break;
            }
            auto & slot = entries.at(*head);
            auto const drv = *head;
            if (!slot || slot->followers.empty() || slot->worker != noWorker) {
                queue.erase(drv); // stale
                if (slot && slot->followers.empty() && slot->worker == noWorker) {
                    retire(drv);
                }
                continue;
            }
            auto wid = pick(*slot);
            queue.erase(*head);
            if (!wid) {
                skipped.emplace_back(*head, prio(*slot));
                continue;
            }
            slot->worker = *wid;
            slot->assignId = ++lastAssignId;
            workers.at(*wid).running.push_back(*head);
            refreshFree(*wid);
            out.push_back(
                Assign{
                    .drv = *head,
                    .worker = *wid,
                    .assignId = slot->assignId,
                    .clients = slot->followers,
                });
        }
        for (auto [drv, key] : skipped) {
            queue.set(drv, key);
        }
    }

    [[nodiscard]] auto freeOf(WorkerId wid) const -> int32_t
    {
        const auto & wkr = workers.at(wid);
        if (wkr.systems.empty()) {
            return 0;
        }
        auto const iter = systems.find(wkr.systems.front());
        return iter == systems.end() ? 0 : iter->second.free.get(wid);
    }

    void refreshFree(WorkerId wid)
    {
        auto const & wkr = workers.at(wid);
        auto const running = static_cast<int32_t>(std::min<size_t>(wkr.running.size(), FreeIndex::maxSlots));
        auto const val = (wkr.up && !wkr.draining) ? wkr.maxJobs - running : 0;
        for (const auto & name : wkr.systems) {
            sys(name).free.set(wid, val);
        }
    }

    [[nodiscard]] auto prio(const Entry & ent) const -> double
    {
        auto const extra = ent.followers.empty() ? 0.0 : static_cast<double>(ent.followers.size() - 1);
        return -(ent.cpHint + (cfg.faninWeight * extra) - (cfg.ageWeight * ent.enqMs));
    }

    [[nodiscard]] static auto offers(const Worker & wkr, const Entry & ent) -> bool
    {
        return std::ranges::find(wkr.systems, ent.system) != wkr.systems.end()
               && std::ranges::all_of(ent.features, [&](const std::string & feat) -> bool {
                      return std::ranges::find(wkr.features, feat) != wkr.features.end();
                  });
    }

    // Most free among workers offering system and features. O(1) without
    // features, O(workers) with.
    [[nodiscard]] auto emptiestFor(const Entry & ent) const -> std::optional<WorkerId>
    {
        auto const iter = systems.find(ent.system);
        if (iter == systems.end()) {
            return std::nullopt;
        }
        const auto & fidx = iter->second.free;
        if (ent.features.empty()) {
            return fidx.emptiest();
        }
        std::optional<WorkerId> best;
        for (WorkerId wid = 0; wid < workers.size(); wid++) {
            if (workers.at(wid).up && fidx.get(wid) > 0 && offers(workers.at(wid), ent)
                && (!best || fidx.get(wid) > fidx.get(*best))) {
                best = wid;
            }
        }
        return best;
    }

    [[nodiscard]] auto pick(const Entry & ent) const -> std::optional<WorkerId>
    {
        auto best = emptiestFor(ent);
        if (!best) {
            return std::nullopt;
        }
        const auto topFree = std::max(1.0, static_cast<double>(freeOf(*best)));
        auto const score = [&](WorkerId wid, double bytes) -> double {
            return bytes + (cfg.locLambda * freeOf(wid) / topFree);
        };
        std::vector<std::pair<WorkerId, double>> cands;
        for (const auto & input : ent.inputs) {
            auto const iter = lastBuilder.find(input);
            if (iter == lastBuilder.end()) {
                continue;
            }
            auto [wid, size] = iter->second;
            if (wid >= workers.size() || !workers.at(wid).up || freeOf(wid) <= 0 || !offers(workers.at(wid), ent)) {
                continue;
            }
            auto const cand = std::ranges::find_if(cands, [&](auto & cnd) -> bool { return cnd.first == wid; });
            if (cand == cands.end()) {
                cands.emplace_back(wid, static_cast<double>(size));
            } else {
                cand->second += static_cast<double>(size);
            }
        }
        WorkerId picked = *best;
        double bestScore = score(*best, 0);
        for (auto [wid, bytes] : cands) {
            if (auto const scr = score(wid, bytes); scr > bestScore) {
                bestScore = scr;
                picked = wid;
            }
        }
        return picked;
    }
};

} // namespace nixgrpc::sched
