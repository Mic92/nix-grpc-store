#pragma once
// The scheduler as seen by its transports: one mutex around sched::Core,
// connected clients and workers as send callbacks. No gRPC here so the
// benchmark and the in-process worker drive the same code as the RPCs.
//
// Locking is annotated for clang -Wthread-safety: public entry points must be
// called without `mutex`, *Locked helpers with it. Send closures run under it
// and may only enqueue (see afterUnlock).

#include <map>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <thread>
#include <unordered_set>
#include <flat_map>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>

#include "logfmt.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"
#include "scheduler.hh"

namespace nixgrpc {

class Dispatcher
{
public:
    using ClientSend = std::function<bool(const nix::remote::SchedMsg &)>;
    using WorkerSend = std::function<bool(const nix::remote::SchedCmd &)>;
    // Subset of `outKeys` the shared cache has. May block (HTTP).
    using PresentFn = std::function<std::unordered_set<std::string>(const std::vector<std::string> & outKeys)>;

    struct Config
    {
        std::string defaultSystem; // for Want/Hello with empty or "builtin" system
        PresentFn present;         // unset: nothing is ever Cached
        // present() runs on this many side threads so gRPC threads never block
        // on niks3; throughput is threads / RTT. 0: on the caller's thread.
        static constexpr unsigned defaultLookupThreads = 32;
        unsigned lookupThreads = defaultLookupThreads;
        LogLevel logLevel = LogLevel::info;
    };

    Dispatcher(Config config, Metrics & metrics);
    ~Dispatcher();
    Dispatcher(const Dispatcher &) = delete;
    auto operator=(const Dispatcher &) -> Dispatcher & = delete;
    Dispatcher(Dispatcher &&) = delete;
    auto operator=(Dispatcher &&) -> Dispatcher & = delete;

    // One per Schedule stream.
    struct Client
    {
        sched::ClientId id = 0;
        ClientSend send;
        bool known = false; // under Dispatcher::mutex: in `clients`
        bool gone = false;  // under Dispatcher::mutex
    };

    using ClientPtr = std::shared_ptr<Client>;

    auto connectClient(ClientSend send) -> ClientPtr;
    // May hand the batch to the lookup pool; `client` is kept alive until applied.
    void clientMsgs(const ClientPtr & client, const nix::remote::ClientMsgs & msgs);
    void clientGone(const ClientPtr & client);

    // One per WorkerSession stream (or the in-process builder).
    struct Worker
    {
        WorkerSend send;
        std::optional<sched::WorkerId> id;
    };

    // Throws on protocol violations; the caller closes the stream.
    void workerMsgs(Worker & worker, const nix::remote::WorkerMsgs & msgs);
    void workerGone(Worker & worker);

    // For send closures, which run under the mutex: queue `fn` (e.g. the
    // transport's write kick) to run right after it is released.
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void afterUnlock(std::function<void()> func);

    // Names the mutex for annotations on send closures. Never lock through it.
    ABSL_LOCK_RETURNED(mutex) auto sendLock() const -> const absl::Mutex &
    {
        return mutex;
    }

    // Tell every connected peer the streams are about to close on purpose.
    void restarting();
    void serving();

    struct Member
    {
        std::string addr;
        bool draining = false;
    };
    using Membership = std::flat_map<std::string, std::vector<Member>>;
    [[nodiscard]] auto membership() const -> std::shared_ptr<const Membership>;

    // `wake` runs without the mutex, after every change.
    auto watchMembership(std::function<void()> wake) -> uint64_t;
    void unwatchMembership(uint64_t watcher);

private:
    // Scoped lock that runs the afterUnlock queue on release.
    class ABSL_SCOPED_LOCKABLE Lock
    {
    public:
        explicit Lock(Dispatcher & disp) ABSL_EXCLUSIVE_LOCK_FUNCTION(disp.mutex);
        ~Lock() ABSL_UNLOCK_FUNCTION();
        Lock(const Lock &) = delete;
        auto operator=(const Lock &) -> Lock & = delete;
        Lock(Lock &&) = delete;
        auto operator=(Lock &&) -> Lock & = delete;

    private:
        Dispatcher & disp;
    };

    using Clock = std::chrono::steady_clock;
    Config config;
    Metrics & metrics;
    prometheus::Counter & assignedCtr;
    const Clock::time_point epoch = Clock::now();
    absl::Mutex mutex;
    std::vector<std::function<void()>> deferred ABSL_GUARDED_BY(mutex);
    sched::Core core ABSL_GUARDED_BY(mutex);
    std::unordered_map<sched::ClientId, ClientSend> clients ABSL_GUARDED_BY(mutex);
    std::unordered_map<sched::WorkerId, WorkerSend> workers ABSL_GUARDED_BY(mutex);
    bool lettingGo ABSL_GUARDED_BY(mutex) = false;
    std::optional<double> statsAtMs ABSL_GUARDED_BY(mutex);
    std::map<sched::Core::StatsKey, std::vector<prometheus::Gauge *>> statGauges ABSL_GUARDED_BY(mutex);
    std::atomic<sched::ClientId> nextClient{1};
    std::map<uint64_t, std::function<void()>> watchers ABSL_GUARDED_BY(mutex);
    // Not atomic<shared_ptr>: libc++ lacks it.
    mutable absl::Mutex publishedMutex ABSL_ACQUIRED_AFTER(mutex);
    std::shared_ptr<const Membership> published ABSL_GUARDED_BY(publishedMutex) = std::make_shared<const Membership>();
    uint64_t nextWatcher ABSL_GUARDED_BY(mutex) = 1;

    // Side pool for present() so gRPC threads never block on niks3.
    struct LookupJob
    {
        ClientPtr client;
        nix::remote::ClientMsgs msgs;
    };

    absl::Mutex lookupMutex ABSL_ACQUIRED_AFTER(mutex);
    std::deque<LookupJob> lookupQueue ABSL_GUARDED_BY(lookupMutex);
    bool lookupStop ABSL_GUARDED_BY(lookupMutex) = false;
    std::vector<std::thread> lookupThreads;
    // exportStats skips a call within a second of the last one. The skipped
    // update would wait for the next event, minutes away while every build
    // runs, so this thread sends it.
    bool statsPending ABSL_GUARDED_BY(mutex) = false;
    bool statsStop ABSL_GUARDED_BY(mutex) = false;
    std::thread statsThread;

    [[nodiscard]] auto nowMs() const -> double;
    void debugLog(std::initializer_list<LogField> fields) const;
    [[nodiscard]] auto systemFor(std::string_view system) const -> std::string_view;
    [[nodiscard]] static auto wantsLookup(const nix::remote::Want & want) -> bool;
    // One present() call for all Wants in `msgs`; which of them are cached.
    [[nodiscard]] auto lookup(const nix::remote::ClientMsgs & msgs) const -> std::vector<bool>;
    void applyClientMsgs(Client & client, const nix::remote::ClientMsgs & msgs, const std::vector<bool> & cached);
    void lookupLoop();
    void statsLoop();
    // Run dispatch and deliver Expect (first) and Assigned.
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void dispatchLocked();
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void membershipChangedLocked();
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void wakeWatchersLocked();
    // Per-system gauges. O(entries), so at most once a second unless forced.
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void exportStats(bool force);
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void revokeOn(sched::WorkerId wid, const std::string & drvPath);
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void supersede(sched::DrvId drv, sched::WorkerId loser);
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void workerMsgLocked(Worker & worker, const nix::remote::WorkerMsg & msg);
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void onWant(Client & client, const nix::remote::Want & want, bool cached);
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void onCancel(Client & client, const nix::remote::Cancel & cancel);
};

} // namespace nixgrpc
