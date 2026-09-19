#pragma once
// The scheduler as seen by its transports: one mutex around sched::Core,
// connected clients and workers as send callbacks. No gRPC here so the
// benchmark and the in-process worker drive the same code as the RPCs.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <functional>
#include <thread>
#include <unordered_set>
#include <initializer_list>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <prometheus/counter.h>

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
        bool known = false; // under mutex: in `clients`
        bool gone = false;  // under mutex
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
    void afterUnlock(std::function<void()> func);

    // Tell every connected peer the streams are about to close on purpose.
    void restarting();
    void serving();

private:
    // scoped_lock that runs the afterUnlock queue on release.
    class Lock
    {
    public:
        explicit Lock(Dispatcher & disp);
        ~Lock();
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
    std::mutex mutex;
    std::vector<std::function<void()>> deferred; // under mutex
    sched::Core core;                                             // under mutex
    std::unordered_map<sched::ClientId, ClientSend> clients;      // under mutex
    std::unordered_map<sched::WorkerId, WorkerSend> workers;      // under mutex
    bool lettingGo = false; // under mutex
    std::atomic<sched::ClientId> nextClient{1};

    // Side pool for present() so gRPC threads never block on niks3.
    struct LookupJob
    {
        ClientPtr client;
        nix::remote::ClientMsgs msgs;
    };

    std::mutex lookupMutex;
    std::condition_variable lookupCv;
    std::deque<LookupJob> lookupQueue;
    bool lookupStop = false;
    std::vector<std::thread> lookupThreads;

    [[nodiscard]] auto nowMs() const -> double;
    void debugLog(std::initializer_list<LogField> fields) const;
    [[nodiscard]] auto systemFor(std::string_view system) const -> std::string_view;
    [[nodiscard]] static auto wantsLookup(const nix::remote::Want & want) -> bool;
    // One present() call for all Wants in `msgs`; which of them are cached.
    [[nodiscard]] auto lookup(const nix::remote::ClientMsgs & msgs) const -> std::vector<bool>;
    void applyClientMsgs(Client & client, const nix::remote::ClientMsgs & msgs, const std::vector<bool> & cached);
    void lookupLoop();
    // Under `mutex`: run dispatch and deliver Expect (first) and Assigned.
    void dispatchLocked();
    void revokeOn(sched::WorkerId wid, const std::string & drvPath);
    void workerMsgLocked(Worker & worker, const nix::remote::WorkerMsg & msg);
    void onWant(Client & client, const nix::remote::Want & want, bool cached);
    void onCancel(Client & client, const nix::remote::Cancel & cancel);
};

} // namespace nixgrpc
