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
#include <functional>
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

    [[nodiscard]] auto topKey() const -> std::optional<double>
    {
        return heap.empty() ? std::nullopt : std::optional(heap.front().first);
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

// How many placed drvs each client waits for.
using Served = absl::flat_hash_map<ClientId, uint32_t>;

// Clients of one owner share its place in line.
using OwnerId = uint64_t;

struct Share
{
    OwnerId owner = 0;
    uint32_t ownerServed = 0; // placed drvs the owner's clients wait for
    uint32_t served = 0;      // and this client
};

// The wait queue of one system. The next drv is taken from the owner whose
// clients wait for the fewest placed ones, and there from the client that
// waits for the fewest, so a long queue cannot keep another one waiting
// behind it. A client's own drvs go by key. A drv several clients wait for is
// queued with each. Counts its entries by required feature set, so a dispatch
// pass can tell when everything left is of a set it already found no worker
// for.
//
// A drv is not searched for when it leaves: its items lose their stamp and are
// dropped when they reach the front of a client's heap.
class WaitQueue
{
    struct Item
    {
        double key = 0;
        DrvId drv = 0;
        uint64_t stamp = 0;
        auto operator>(const Item & other) const -> bool
        {
            return std::pair(key, drv) > std::pair(other.key, other.drv);
        }
    };
    // Members of `Turn` are (served, key of the first item, id).
    template<typename Id>
    using Turn = std::set<std::tuple<uint32_t, double, Id>>;
    using Listed = std::optional<std::pair<uint32_t, double>>;
    struct Line
    {
        OwnerId owner = 0;
        uint32_t served = 0;
        std::vector<Item> heap; // smallest key first
        Listed listed;          // as in its group's turn
    };
    struct Group
    {
        uint32_t served = 0;
        Turn<ClientId> turn; // its clients with queued drvs
        // Their first keys. The smallest is the owner's: one that depended
        // on which client is first could be listed too late by a stale item.
        std::set<std::pair<double, ClientId>> keys;
        Listed listed; // as in `turn`
    };
    absl::flat_hash_map<ClientId, Line> lines;
    absl::flat_hash_map<OwnerId, Group> groups;
    // The first owner's first client's first item is always a queued drv.
    // Others may be stale, which can only list them too early.
    Turn<OwnerId> turn;
    std::vector<uint64_t> stamps; // by drv; 0 when not queued
    uint64_t lastStamp = 0;
    size_t queued = 0;
    absl::flat_hash_map<std::string, uint32_t> perFeatures;

    [[nodiscard]] auto stale(const Item & item) const -> bool
    {
        return stamps.at(item.drv) != item.stamp;
    }

    // Moves a member of an ordered set. Its node is kept, which saves an
    // allocation on the scheduler's most frequent queue update.
    template<typename Set>
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): from, then to.
    static void move(Set & into, const Set::value_type & from, const Set::value_type & dest)
    {
        auto node = into.extract(from);
        node.value() = dest;
        into.insert(std::move(node));
    }

    template<typename Id>
    static void list(Turn<Id> & into, Id ident, Listed & listed, Listed now)
    {
        if (listed == now) {
            return;
        }
        if (listed && now) {
            move(into, {listed->first, listed->second, ident}, {now->first, now->second, ident});
        } else if (listed) {
            into.erase({listed->first, listed->second, ident});
        } else {
            into.emplace(now->first, now->second, ident);
        }
        listed = now;
    }

    void relist(OwnerId owner, Group & group)
    {
        list(turn, owner, group.listed,
             group.keys.empty() ? std::nullopt : Listed(std::pair(group.served, group.keys.begin()->first)));
    }

    void relist(ClientId client, Line & line)
    {
        auto const now = line.heap.empty() ? std::nullopt : Listed(std::pair(line.served, line.heap.front().key));
        if (line.listed == now) {
            return;
        }
        auto & group = groups.at(line.owner);
        auto const was = line.listed;
        list(group.turn, client, line.listed, now);
        if (was && now && was->second == now->second) {
            return; // the owner's key is unchanged
        }
        if (was && now) {
            move(group.keys, {was->second, client}, {now->second, client});
        } else if (was) {
            group.keys.erase({was->second, client});
        } else {
            group.keys.emplace(now->second, client);
        }
        relist(line.owner, group);
    }

    // A line whose heap ran empty.
    void drop(absl::flat_hash_map<ClientId, Line>::iterator iter)
    {
        relist(iter->first, iter->second);
        if (auto const group = groups.find(iter->second.owner); group->second.turn.empty()) {
            groups.erase(group);
        }
        lines.erase(iter);
    }

    // Drops the stale items at the front of a client's heap. True if any.
    auto prune(ClientId client) -> bool
    {
        auto const iter = lines.find(client);
        auto & line = iter->second;
        if (!stale(line.heap.front())) {
            return false;
        }
        while (!line.heap.empty() && stale(line.heap.front())) {
            std::ranges::pop_heap(line.heap, std::greater{});
            line.heap.pop_back();
        }
        if (line.heap.empty()) {
            drop(iter);
        } else {
            relist(client, line);
        }
        return true;
    }

    // Until the first owner's key and its first client's first item are
    // those of queued drvs.
    void settle()
    {
        while (!turn.empty()) {
            const auto & group = groups.at(std::get<2>(*turn.begin()));
            if (!prune(group.keys.begin()->second) && !prune(std::get<2>(*group.turn.begin()))) {
                return;
            }
        }
    }

public:
    // Where the first owner stands: its placed drvs, then its smallest key.
    using Rank = std::pair<uint32_t, double>;

    [[nodiscard]] auto size() const -> size_t
    {
        return queued;
    }

    [[nodiscard]] auto top() const -> std::optional<DrvId>
    {
        if (turn.empty()) {
            return std::nullopt;
        }
        auto const client = std::get<2>(*groups.at(std::get<2>(*turn.begin())).turn.begin());
        return lines.at(client).heap.front().drv;
    }

    [[nodiscard]] auto topKey() const -> std::optional<Rank>
    {
        if (turn.empty()) {
            return std::nullopt;
        }
        return Rank{std::get<0>(*turn.begin()), std::get<1>(*turn.begin())};
    }

    [[nodiscard]] auto count(const std::string & features) const -> size_t
    {
        auto const iter = perFeatures.find(features);
        return iter == perFeatures.end() ? 0 : iter->second;
    }

    // `shareOf` is asked about clients and owners with nothing queued here.
    template<typename F>
    void set(DrvId drv, double key, const std::string & features, const std::vector<ClientId> & clients, const F & shareOf)
    {
        if (clients.empty()) {
            erase(drv, features);
            return;
        }
        if (drv >= stamps.size()) {
            stamps.resize(size_t{drv} + 1, 0);
        }
        if (stamps.at(drv) == 0) {
            perFeatures.try_emplace(features).first->second++;
            queued++;
        }
        stamps.at(drv) = ++lastStamp;
        for (auto const client : clients) {
            auto const [iter, fresh] = lines.try_emplace(client);
            auto & line = iter->second;
            if (fresh) {
                const Share share = shareOf(client);
                line.owner = share.owner;
                line.served = share.served;
                if (auto const [group, added] = groups.try_emplace(share.owner); added) {
                    group->second.served = share.ownerServed;
                }
            }
            line.heap.push_back({.key = key, .drv = drv, .stamp = lastStamp});
            std::ranges::push_heap(line.heap, std::greater{});
            relist(client, line);
        }
        settle();
    }

    void erase(DrvId drv, const std::string & features)
    {
        if (drv >= stamps.size() || stamps.at(drv) == 0) {
            return;
        }
        stamps.at(drv) = 0;
        queued--;
        if (auto const iter = perFeatures.find(features); iter != perFeatures.end() && --iter->second == 0) {
            perFeatures.erase(iter);
        }
        settle();
    }

    // The client, and its owner's clients together, now wait for this many
    // placed drvs.
    void serving(ClientId client, const Share & share)
    {
        if (auto const group = groups.find(share.owner); group != groups.end()) {
            group->second.served = share.ownerServed;
            relist(share.owner, group->second);
        }
        if (auto const iter = lines.find(client); iter != lines.end()) {
            iter->second.served = share.served;
            relist(client, iter->second);
        }
        settle();
    }

    void forget(ClientId client)
    {
        auto const iter = lines.find(client);
        if (iter == lines.end()) {
            return;
        }
        iter->second.heap.clear();
        drop(iter);
        settle();
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
    std::string featureKey;            // featureSetKey(features); fixed while queued
    std::vector<ClientId> followers;
    WorkerId worker = noWorker;
    uint64_t assignId = 0;
    double enqMs = 0;
    double cpHint = 0;
    bool unplaceable = false; // as last told to the followers
    std::vector<WorkerId> givers; // could not run it, latest last
};

// What a give-back says: the worker takes no builds at all right now, or it
// could not run this drv.
enum class GaveBack : uint8_t { worker, drv };

struct Worker
{
    std::string addr;
    std::vector<std::string> systems;
    std::vector<std::string> features;
    int32_t maxJobs = 0;
    bool up = false;
    bool draining = false;
    // Give-backs for a failure of its own since it last finished a build.
    uint32_t failures = 0;
    std::optional<double> restsUntilMs;
    std::vector<DrvId> running;

    // A worker that says Hello with max_jobs 0 is up but runs nothing.
    [[nodiscard]] auto serves() const -> bool
    {
        return up && !draining && maxJobs > 0;
    }
    // A resting worker serves again soon: what it offers stays placeable.
    [[nodiscard]] auto takesWork() const -> bool
    {
        return serves() && !restsUntilMs;
    }
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
            slot->featureKey = featureSetKey(slot->features);
            slot->inputs.reserve(info.inputs.size());
            for (auto const input : info.inputs) {
                slot->inputs.emplace_back(input);
            }
        } else if (slot->system.empty()) {
            // Known only from a worker's Hello so far.
            slot->system = std::string(info.system);
            slot->features = std::move(info.features);
            slot->featureKey = featureSetKey(slot->features);
        }
        // Followers of an assigned drv are told again if its worker vanishes.
        if (std::ranges::find(slot->followers, client) == slot->followers.end()) {
            slot->followers.push_back(client);
            follows[client].push_back(drv);
            if (slot->worker != noWorker) {
                serve(client, 1);
            }
        }
        slot->cpHint = std::max(slot->cpHint, cpHintMs);
        if (slot->worker != noWorker) {
            return {.drv = drv, .assigned = std::pair(slot->worker, slot->assignId)};
        }
        enqueue(drv);
        return {.drv = drv};
    }

    // The drv's only client could not use its assignment: the drv is placed
    // again as if its worker had given it back. Returns the worker to Revoke on.
    auto refuse(ClientId client, std::string_view drvPath, uint64_t assignId) -> std::optional<WorkerId>
    {
        auto const found = findDrv(drvPath);
        if (!found) {
            return std::nullopt;
        }
        const auto & slot = entries.at(*found);
        if (!slot || slot->worker == noWorker || slot->assignId != assignId
            || slot->followers != std::vector<ClientId>{client}) {
            return std::nullopt;
        }
        auto const wid = slot->worker;
        giveBack({.worker = wid, .drvPath = drvPath, .assignId = assignId}, GaveBack::drv);
        return wid;
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
        if (slot->worker != noWorker) {
            serve(client, -1);
        }
        if (!slot->followers.empty()) {
            if (slot->worker == noWorker) {
                enqueue(drv);
            }
            return std::nullopt;
        }
        if (slot->worker == noWorker) {
            retire(drv);
            return std::nullopt;
        }
        return slot->worker;
    }

    // Before the client's first want.
    void clientOwner(ClientId client, std::string_view name)
    {
        if (ownerOf.contains(client)) {
            return;
        }
        auto const [iter, fresh] = ownerIds.try_emplace(std::string(name));
        if (fresh) {
            iter->second = namedOwner | ++lastOwner;
            owners.emplace(iter->second, Owner{.name = iter->first});
        }
        owners.at(iter->second).clients++;
        ownerOf.emplace(client, iter->second);
    }

    void clientGone(ClientId client, std::vector<std::pair<DrvId, WorkerId>> & revokes)
    {
        if (auto const iter = follows.find(client); iter != follows.end()) {
            auto const drvs = std::move(iter->second);
            follows.erase(iter);
            for (auto const drv : drvs) {
                if (auto wid = cancel(client, drv)) {
                    revokes.emplace_back(drv, *wid);
                }
            }
            for (auto & [name, state] : systems) {
                state.queue.forget(client);
            }
        }
        if (auto const iter = ownerOf.find(client); iter != ownerOf.end()) {
            if (auto const owner = owners.find(iter->second); --owner->second.clients == 0) {
                ownerIds.erase(owner->second.name);
                owners.erase(owner);
            }
            ownerOf.erase(iter);
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

    // A reported drv that followers wait for: the caller sends them a fresh
    // Assigned. If this scheduler had meanwhile placed it on `loser`, the
    // reporter keeps it and the caller revokes `loser`.
    struct Superseded
    {
        DrvId drv;
        WorkerId loser = noWorker;
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
            } else if (slot->worker == noWorker && !slot->followers.empty()) {
                superseded.push_back({.drv = drv});
            }
            place(*slot, wid);
            slot->assignId = assignId;
            sys(slot->system).queue.erase(drv, slot->featureKey);
            if (std::ranges::find(wkr.running, drv) == wkr.running.end()) {
                wkr.running.push_back(drv);
            }
        }
        refreshFree(wid);
        return wid;
    }

    // An assignment is named by the worker, the drv and the id it was
    // placed with. A worker's word about one that is over changes nothing.
    struct Lease
    {
        WorkerId worker;
        std::string_view drvPath;
        uint64_t assignId;
    };

    void done(Lease lease, const std::vector<std::pair<std::string_view, uint64_t>> & outputs)
    {
        for (auto [path, size] : outputs) {
            rememberBuilder(path, lease.worker, size);
        }
        if (auto const drv = release(lease)) {
            workers.at(lease.worker).failures = 0;
            retire(*drv);
        }
    }

    // The worker can't run it after all: place it elsewhere. False if the
    // lease is over.
    auto giveBack(Lease lease, GaveBack why) -> bool
    {
        auto const found = release(lease);
        if (!found) {
            return false;
        }
        auto & slot = entries.at(*found);
        if (!slot) {
            return true;
        }
        place(*slot, noWorker);
        if (why == GaveBack::drv) {
            std::erase(slot->givers, lease.worker);
            slot->givers.push_back(lease.worker);
        }
        if (slot->followers.empty()) {
            retire(*found);
        } else {
            enqueue(*found);
        }
        return true;
    }

    // The worker gave a drv back for a failure of its own. One that keeps
    // doing so would get every new drv once, so it rests, longer each time.
    // Returns the length of a rest that starts.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): who, then when.
    auto failed(WorkerId wid, double nowMs) -> std::optional<double>
    {
        constexpr uint32_t tolerated = 2;
        constexpr double firstRestMs = 30e3;
        constexpr double longestRestMs = 600e3;
        constexpr uint32_t maxDoublings = 5;
        auto & wkr = workers.at(wid);
        // Builds it gave back while resting were assigned before the rest.
        if (!wkr.up || wkr.restsUntilMs || ++wkr.failures <= tolerated) {
            return std::nullopt;
        }
        auto const doublings = std::min(wkr.failures - tolerated - 1, maxDoublings);
        auto const rest = std::min(firstRestMs * static_cast<double>(1U << doublings), longestRestMs);
        resting_++;
        wkr.restsUntilMs = nowMs + rest;
        refreshFree(wid);
        return rest;
    }

    // True if a worker's rest ended.
    auto wake(double nowMs) -> bool
    {
        if (resting_ == 0) {
            return false;
        }
        bool woke = false;
        for (WorkerId wid = 0; wid < workers.size(); wid++) {
            auto & wkr = workers.at(wid);
            if (wkr.restsUntilMs && *wkr.restsUntilMs <= nowMs) {
                unrest(wid);
                woke = true;
            }
        }
        return woke;
    }

    [[nodiscard]] auto resting() const -> size_t
    {
        return resting_;
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
        wkr.failures = 0;
        unrest(wid);
        for (auto const drv : std::exchange(wkr.running, {})) {
            if (drv >= entries.size()) {
                continue;
            }
            auto & slot = entries.at(drv);
            if (!slot || slot->worker != wid) {
                continue;
            }
            place(*slot, noWorker);
            if (slot->followers.empty()) {
                retire(drv);
            } else {
                enqueue(drv);
            }
        }
    }

    // Across systems, so the most urgent drv wins.
    void dispatch(std::vector<Assign> & out)
    {
        Pass pass;
        for (;;) {
            std::string const * best = nullptr;
            WaitQueue::Rank bestKey;
            for (const auto & [name, state] : systems) {
                auto const key = state.queue.topKey();
                if (key && state.free.maxFree() > 0 && !pass.exhausted(name, state.queue)
                    && (best == nullptr || *key < bestKey)) {
                    best = &name;
                    bestKey = *key;
                }
            }
            if (best == nullptr) {
                break;
            }
            dispatchHead(std::string(*best), out, pass);
        }
        for (auto const drv : pass.skipped) {
            enqueue(drv);
        }
    }

    // Some worker that takes work could ever run it.
    [[nodiscard]] auto placeable(const Entry & ent) const -> bool
    {
        return std::ranges::any_of(workers, [&](const Worker & wkr) -> bool { return wkr.serves() && offers(wkr, ent); });
    }

    // Features of the workers that take work and serve the system.
    [[nodiscard]] auto offeredFeatures(const std::string & system) const -> std::vector<std::string>
    {
        std::set<std::string> feats;
        for (const auto & wkr : workers) {
            if (wkr.serves() && std::ranges::find(wkr.systems, system) != wkr.systems.end()) {
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

    // What a follower was last told about a queued entry.
    void markUnplaceable(DrvId drv, bool unplaceable)
    {
        if (auto & slot = entries.at(drv)) {
            slot->unplaceable = unplaceable;
        }
    }

    // Queued entries whose placeability flipped since the followers were
    // last told. After Hello, workerGone and Load: O(entries), memoised per
    // (system, features).
    void placeabilityChanges(std::vector<DrvId> & out)
    {
        std::map<std::pair<std::string, std::vector<std::string>>, bool> byKey;
        for (size_t idx = 0; idx < entries.size(); idx++) {
            auto & slot = entries.at(idx);
            if (!slot || slot->worker != noWorker || slot->followers.empty()) {
                continue;
            }
            auto [iter, fresh] = byKey.try_emplace({slot->system, slot->features}, false);
            if (fresh) {
                iter->second = placeable(*slot);
            }
            if (slot->unplaceable == iter->second) {
                slot->unplaceable = !iter->second;
                out.push_back(static_cast<DrvId>(idx));
            }
        }
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
        // Keyed by what the worker offers (native system).
        size_t running = 0; // assigned, until the worker's Done
        size_t slots = 0;   // maxJobs of workers that take work
        size_t free = 0;
    };

    // Identifies a set of features. Unlike featureKey(), which is a label,
    // no two sets share one: names come from the network and may hold a comma.
    [[nodiscard]] static auto featureSetKey(std::vector<std::string> feats) -> std::string
    {
        std::ranges::sort(feats);
        std::string key;
        for (const auto & feat : feats) {
            key += std::to_string(feat.size()) + ":" + feat;
        }
        return key;
    }

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
            if (wkr.systems.empty()) {
                continue;
            }
            auto & sst = out[{.system = wkr.systems.front(), .features = featureKey(wkr.features)}];
            sst.running += wkr.running.size();
            if (!wkr.takesWork()) {
                continue;
            }
            sst.slots += static_cast<size_t>(wkr.maxJobs);
            sst.free += static_cast<size_t>(std::max(0, wkr.maxJobs - static_cast<int32_t>(wkr.running.size())));
        }
        // placeable() is O(workers). Entries sharing system+features share the answer.
        std::map<StatsKey, bool> memo;
        for (const auto & slot : entries) {
            if (!slot || slot->followers.empty() || slot->worker != noWorker) {
                continue;
            }
            const StatsKey key{.system = slot->system, .features = featureKey(slot->features)};
            auto & sst = out[key];
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

    // Right after a restart no worker has said Hello yet, so a Want for a
    // system nobody offers is kept, but only for a few such systems.
    [[nodiscard]] auto admitsSystem(const std::string & system) const -> bool
    {
        if (systems.contains(system)) {
            return true;
        }
        return std::ranges::count_if(systems, [&](const auto & named) -> bool { return !offered(named.first); })
            < static_cast<std::ptrdiff_t>(maxUnofferedSystems);
    }

    // Bounds on soft state; inputs come from the network.
    static constexpr size_t maxUnofferedSystems = 16;
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
    size_t resting_ = 0;
    struct SystemState
    {
        WaitQueue queue;
        FreeIndex free; // a worker sits in each of its systems' index with the same count
    };
    std::unordered_map<std::string, SystemState> systems;
    std::unordered_map<ClientId, std::vector<DrvId>> follows;
    // Output path -> who built it last, for input locality. A tie-breaker
    // only, so on overflow the table is simply dropped.
    PathMap<std::pair<WorkerId, uint64_t>> lastBuilder;
    uint64_t lastAssignId = 0;
    Served served;
    // An owner is a certificate name or token subject. A client without one
    // stands for itself, under its own id.
    struct Owner
    {
        std::string name;
        uint32_t clients = 0;
        uint32_t served = 0;
    };
    static constexpr OwnerId namedOwner = OwnerId{1} << 63U; // apart from client ids
    absl::flat_hash_map<std::string, OwnerId> ownerIds;
    absl::flat_hash_map<OwnerId, Owner> owners;
    absl::flat_hash_map<ClientId, OwnerId> ownerOf;
    uint64_t lastOwner = 0;

    [[nodiscard]] auto shareOf(ClientId client) const -> Share
    {
        auto const mine = served.find(client);
        auto const own = mine == served.end() ? 0 : mine->second;
        auto const owner = ownerOf.find(client);
        if (owner == ownerOf.end()) {
            return {.owner = client, .ownerServed = own, .served = own};
        }
        return {.owner = owner->second, .ownerServed = owners.at(owner->second).served, .served = own};
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): who, then +1 or -1.
    void serve(ClientId client, int delta)
    {
        auto const add = [delta](uint32_t & count) -> void {
            count = static_cast<uint32_t>(static_cast<int64_t>(count) + delta);
        };
        auto const iter = served.try_emplace(client).first;
        add(iter->second);
        if (iter->second == 0) {
            served.erase(iter);
        }
        if (auto const owner = ownerOf.find(client); owner != ownerOf.end()) {
            add(owners.at(owner->second).served);
        }
        auto const share = shareOf(client);
        for (auto & [name, state] : systems) {
            state.queue.serving(client, share);
        }
    }

    // The one place an entry gains or loses its worker, so `served` follows.
    void place(Entry & ent, WorkerId wid)
    {
        auto const delta = static_cast<int>(wid != noWorker) - static_cast<int>(ent.worker != noWorker);
        ent.worker = wid;
        if (delta != 0) {
            for (auto const client : ent.followers) {
                serve(client, delta);
            }
        }
    }

    // Entry is finished or abandoned: free the slot and its id for reuse.
    // Frees the worker's slot if the lease is the drv's current assignment.
    auto release(Lease lease) -> std::optional<DrvId>
    {
        auto const found = findDrv(lease.drvPath);
        if (!found) {
            return std::nullopt;
        }
        const auto & slot = entries.at(*found);
        if (!slot || slot->worker != lease.worker || slot->assignId != lease.assignId) {
            return std::nullopt;
        }
        std::erase(workers.at(lease.worker).running, *found);
        refreshFree(lease.worker);
        return found;
    }

    void retire(DrvId drv)
    {
        auto & slot = entries.at(drv);
        if (!slot) {
            return;
        }
        auto const iter = systems.find(slot->system);
        place(*slot, noWorker);
        if (iter != systems.end()) {
            iter->second.queue.erase(drv, slot->featureKey);
            if (iter->second.queue.size() == 0 && !offered(iter->first)) {
                systems.erase(iter);
            }
        }
        drvIds.erase(PathKey(slot->drvPath));
        slot.reset();
        freeIds.push_back(drv);
    }

    [[nodiscard]] auto offered(const std::string & system) const -> bool
    {
        return std::ranges::any_of(
            workers, [&](const Worker & wkr) -> bool { return std::ranges::find(wkr.systems, system) != wkr.systems.end(); });
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

    void enqueue(DrvId drv)
    {
        if (const auto & slot = entries.at(drv)) {
            sys(slot->system).queue.set(drv, prio(*slot), slot->featureKey, slot->followers,
                                        [this](ClientId client) -> Share { return shareOf(client); });
        }
    }

    // What one dispatch pass found it cannot place. Slots only fill during a
    // pass, so a feature set without a worker stays without one.
    struct Pass
    {
        std::vector<DrvId> skipped; // off the queue until the pass ends
        std::unordered_map<std::string, std::vector<std::string>> stuck; // system -> feature keys

        [[nodiscard]] auto isStuck(const Entry & ent) const -> bool
        {
            auto const iter = stuck.find(ent.system);
            return iter != stuck.end() && std::ranges::find(iter->second, ent.featureKey) != iter->second.end();
        }

        // Everything still queued is of a stuck feature set.
        [[nodiscard]] auto exhausted(const std::string & system, const WaitQueue & queue) const -> bool
        {
            auto const iter = stuck.find(system);
            if (iter == stuck.end()) {
                return false;
            }
            size_t left = queue.size();
            for (const auto & key : iter->second) {
                left -= queue.count(key);
            }
            return left == 0;
        }
    };

    void dispatchHead(const std::string & system, std::vector<Assign> & out, Pass & pass)
    {
        auto & queue = sys(system).queue;
        auto const drv = queue.top().value_or(0);
        auto & slot = entries.at(drv);
        queue.erase(drv, slot ? slot->featureKey : std::string());
        if (!slot || slot->followers.empty() || slot->worker != noWorker) {
            if (slot && slot->followers.empty() && slot->worker == noWorker) {
                retire(drv); // stale
            }
            return;
        }
        auto const wid = pass.isStuck(*slot) ? std::nullopt : pick(*slot);
        if (!wid) {
            pass.skipped.push_back(drv);
            // With givers, pick() can fail where the rest of the set would not.
            if (slot->givers.empty() && !pass.isStuck(*slot)) {
                pass.stuck[system].push_back(slot->featureKey);
            }
            return;
        }
        place(*slot, *wid);
        slot->assignId = ++lastAssignId;
        workers.at(*wid).running.push_back(drv);
        refreshFree(*wid);
        out.push_back(Assign{.drv = drv, .worker = *wid, .assignId = slot->assignId, .clients = slot->followers});
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

    void unrest(WorkerId wid)
    {
        if (workers.at(wid).restsUntilMs) {
            workers.at(wid).restsUntilMs.reset();
            resting_--;
        }
        refreshFree(wid);
    }

    void refreshFree(WorkerId wid)
    {
        auto const & wkr = workers.at(wid);
        auto const running = static_cast<int32_t>(std::min<size_t>(wkr.running.size(), FreeIndex::maxSlots));
        auto const val = wkr.takesWork() ? wkr.maxJobs - running : 0;
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

    static auto gave(const Entry & ent, WorkerId wid) -> bool
    {
        return std::ranges::find(ent.givers, wid) != ent.givers.end();
    }

    // Givers are passed over while another worker can run the drv now. If
    // none can, only the latest giver is, as long as someone else offers it.
    [[nodiscard]] auto sparing(const Entry & ent) const -> std::optional<WorkerId>
    {
        auto const latest = ent.givers.back();
        std::optional<WorkerId> fresh;
        std::optional<WorkerId> giver;
        bool others = false;
        for (WorkerId wid = 0; wid < workers.size(); wid++) {
            const auto & wkr = workers.at(wid);
            if (wid == latest || !wkr.takesWork() || !offers(wkr, ent)) {
                continue;
            }
            others = true;
            auto & best = gave(ent, wid) ? giver : fresh;
            if (freeOf(wid) > 0 && (!best || freeOf(wid) > freeOf(*best))) {
                best = wid;
            }
        }
        if (fresh || giver) {
            return fresh ? fresh : giver;
        }
        if (others || freeOf(latest) <= 0) {
            return std::nullopt;
        }
        return latest;
    }

    [[nodiscard]] auto pick(const Entry & ent) const -> std::optional<WorkerId>
    {
        auto best = emptiestFor(ent);
        if (best && gave(ent, *best)) {
            best = sparing(ent);
        }
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
            if (wid >= workers.size() || gave(ent, wid) || !workers.at(wid).up || freeOf(wid) <= 0
                || !offers(workers.at(wid), ent)) {
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
