#pragma once
// Placement core of one shard. No I/O, no locking; ported from sim/src/sim.rs.
// Inputs arrive from the network, so every id is range-checked and every
// state transition tolerates duplicates and stale messages.

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
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
        if (auto slash = path.rfind('/'); slash != std::string_view::npos) {
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
        auto idx = pos.at(drv);
        auto old = heap.at(idx).first;
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
        auto idx = pos.at(drv);
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
    std::vector<std::vector<WorkerId>> byFree = std::vector<std::vector<WorkerId>>(maxSlots + 1);
    uint64_t nonempty = 0;

    [[nodiscard]] static auto bucket(int32_t val) -> size_t
    {
        return static_cast<size_t>(std::clamp<int32_t>(val, 0, maxSlots));
    }

    void check(WorkerId wid) const
    {
        if (wid >= free.size()) {
            throw nix::Error("FreeIndex: unknown worker %d", wid);
        }
    }

public:
    // Ids are dense and pushed in order.
    void push(WorkerId wid)
    {
        if (wid != free.size()) {
            throw nix::Error("FreeIndex: non-dense worker id %d", wid);
        }
        free.push_back(0);
        pos.push_back(static_cast<uint32_t>(byFree.at(0).size()));
        byFree.at(0).push_back(wid);
        nonempty |= 1;
    }

    [[nodiscard]] auto get(WorkerId wid) const -> int32_t
    {
        check(wid);
        return free.at(wid);
    }

    void set(WorkerId wid, int32_t val)
    {
        check(wid);
        auto from = bucket(free.at(wid));
        auto dest = bucket(val);
        free.at(wid) = val;
        if (from == dest) {
            return;
        }
        auto & src = byFree.at(from);
        auto idx = pos.at(wid);
        if (idx >= src.size() || src.at(idx) != wid) {
            throw nix::Error("FreeIndex: corrupt bucket for worker %d", wid);
        }
        auto last = src.back();
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

    void add(WorkerId wid, int32_t delta)
    {
        set(wid, get(wid) + delta);
    }

    [[nodiscard]] auto maxFree() const -> int32_t
    {
        return nonempty == 0 ? 0 : maxSlots - std::countl_zero(nonempty);
    }

    [[nodiscard]] auto emptiest() const -> std::optional<WorkerId>
    {
        auto top = maxFree();
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
    std::vector<std::string> features;
    int32_t maxJobs = 0;
    bool up = false;
    bool draining = false;
    std::vector<DrvId> running;
};

class Shard
{
public:
    explicit Shard(Config cfg = {})
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
        if (auto iter = drvIds.find(key); iter != drvIds.end()) {
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
        auto iter = drvIds.find(PathKey(path));
        return iter == drvIds.end() ? std::nullopt : std::optional(iter->second);
    }

    // Live entries; ids of retired ones are recycled.
    [[nodiscard]] auto live() const -> size_t
    {
        return drvIds.size();
    }

    auto workerId(std::string_view addr) -> WorkerId
    {
        if (auto iter = workerIds.find(std::string(addr)); iter != workerIds.end()) {
            return iter->second;
        }
        auto wid = static_cast<WorkerId>(workers.size());
        workers.push_back(Worker{.addr = std::string(addr)});
        fi.push(wid);
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
        return queue.size();
    }

    [[nodiscard]] auto workersUp() const -> size_t
    {
        return workersUp_;
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
        std::vector<std::string> features;
        double cpHintMs = 0;
    };

    // Caller handled the cache-hit case already.
    auto want(ClientId client, WantInfo info, double nowMs) -> WantResult
    {
        auto drv = drvId(info.drvPath);
        auto cpHintMs = info.cpHintMs;
        auto & slot = entries.at(drv);
        if (!slot) {
            slot = Entry{.drvPath = std::string(info.drvPath), .features = std::move(info.features), .enqMs = nowMs};
            slot->inputs.reserve(info.inputs.size());
            for (auto input : info.inputs) {
                slot->inputs.emplace_back(input);
            }
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
        queue.set(drv, prio(*slot));
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
                queue.set(drv, prio(*slot));
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
        auto iter = follows.find(client);
        if (iter == follows.end()) {
            return;
        }
        auto drvs = std::move(iter->second);
        follows.erase(iter);
        for (auto drv : drvs) {
            if (auto wid = cancel(client, drv)) {
                revokes.emplace_back(drv, *wid);
            }
        }
    }

    struct HelloInfo
    {
        std::string_view addr;
        std::vector<std::string> features;
        uint32_t maxJobs = 0;
        std::vector<std::string_view> running; // running + expecting
    };

    auto hello(const HelloInfo & info) -> WorkerId
    {
        if (info.addr.empty()) {
            throw nix::Error("scheduler: Hello without addr");
        }
        auto wid = workerId(info.addr);
        if (workers.at(wid).up) {
            workerGone(wid);
        }
        auto & wkr = workers.at(wid);
        wkr.up = true;
        workersUp_++;
        wkr.draining = false;
        wkr.features = info.features;
        wkr.maxJobs = static_cast<int32_t>(std::min<uint32_t>(info.maxJobs, FreeIndex::maxSlots));
        for (auto path : info.running) {
            auto drv = drvId(path);
            auto & slot = entries.at(drv);
            if (!slot) {
                slot = Entry{.drvPath = std::string(path)};
            }
            if (slot->worker != noWorker && slot->worker != wid) {
                continue; // double assignment across a restart: first publish wins
            }
            slot->worker = wid;
            queue.erase(drv);
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
        fi.set(wid, 0);
        for (auto drv : std::exchange(wkr.running, {})) {
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
                queue.set(drv, prio(*slot));
            }
        }
    }

    void dispatch(std::vector<Assign> & out)
    {
        std::vector<std::pair<DrvId, double>> skipped; // need a feature no free worker has
        while (auto head = queue.top()) {
            if (fi.maxFree() <= 0) {
                break;
            }
            auto & slot = entries.at(*head);
            auto drv = *head;
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

    // Some connected, non-draining worker could ever run it.
    [[nodiscard]] auto placeable(DrvId drv) const -> bool
    {
        const auto & slot = entries.at(drv);
        return slot && std::ranges::any_of(workers, [&](const Worker & wkr) -> bool {
                   return wkr.up && !wkr.draining && offers(wkr, *slot);
               });
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
    IndexedHeap queue;
    FreeIndex fi;
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
        queue.erase(drv);
        drvIds.erase(PathKey(slot->drvPath));
        slot.reset();
        freeIds.push_back(drv);
    }

    void rememberBuilder(std::string_view path, WorkerId wid, uint64_t size)
    {
        const PathKey key(path);
        if (auto iter = lastBuilder.find(key); iter != lastBuilder.end()) {
            iter->second = {wid, size};
            return;
        }
        if (lastBuilder.size() >= maxLastBuilder) {
            lastBuilder.clear();
        }
        lastBuilder.emplace(key, std::pair(wid, size));
    }

    void refreshFree(WorkerId wid)
    {
        auto & wkr = workers.at(wid);
        auto running = static_cast<int32_t>(std::min<size_t>(wkr.running.size(), FreeIndex::maxSlots));
        fi.set(wid, (wkr.up && !wkr.draining) ? wkr.maxJobs - running : 0);
    }

    [[nodiscard]] auto prio(const Entry & ent) const -> double
    {
        auto extra = ent.followers.empty() ? 0.0 : static_cast<double>(ent.followers.size() - 1);
        return -(ent.cpHint + (cfg.faninWeight * extra) - (cfg.ageWeight * ent.enqMs));
    }

    [[nodiscard]] static auto offers(const Worker & wkr, const Entry & ent) -> bool
    {
        return std::ranges::all_of(ent.features, [&](const std::string & feat) -> bool {
            return std::ranges::find(wkr.features, feat) != wkr.features.end();
        });
    }

    // Most free among workers offering the features; O(workers) only when
    // features are required.
    [[nodiscard]] auto emptiestFor(const Entry & ent) const -> std::optional<WorkerId>
    {
        if (ent.features.empty()) {
            return fi.emptiest();
        }
        std::optional<WorkerId> best;
        for (WorkerId wid = 0; wid < workers.size(); wid++) {
            if (workers.at(wid).up && fi.get(wid) > 0 && offers(workers.at(wid), ent)
                && (!best || fi.get(wid) > fi.get(*best))) {
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
        const auto topFree = std::max(1.0, static_cast<double>(fi.get(*best)));
        auto score = [&](WorkerId wid, double bytes) -> double {
            return bytes + (cfg.locLambda * fi.get(wid) / topFree);
        };
        std::vector<std::pair<WorkerId, double>> cands;
        for (const auto & input : ent.inputs) {
            auto iter = lastBuilder.find(input);
            if (iter == lastBuilder.end()) {
                continue;
            }
            auto [wid, size] = iter->second;
            if (wid >= workers.size() || !workers.at(wid).up || fi.get(wid) <= 0 || !offers(workers.at(wid), ent)) {
                continue;
            }
            auto cand = std::ranges::find_if(cands, [&](auto & cnd) -> bool { return cnd.first == wid; });
            if (cand == cands.end()) {
                cands.emplace_back(wid, static_cast<double>(size));
            } else {
                cand->second += static_cast<double>(size);
            }
        }
        WorkerId picked = *best;
        double bestScore = score(*best, 0);
        for (auto [wid, bytes] : cands) {
            if (auto scr = score(wid, bytes); scr > bestScore) {
                bestScore = scr;
                picked = wid;
            }
        }
        return picked;
    }
};

} // namespace nixgrpc::sched
