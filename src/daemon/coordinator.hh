#pragma once
// Scheduler gRPC service, the builder's session to it (in-process or remote),
// and the Expect table that gates BuildDerivation.

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/store-api.hh>
#include <nix/util/sync.hh>
#include <nix/util/types.hh>

#include "auth.hh"
#include "cache.hh"
#include "dispatcher.hh"
#include "eds.hh"
#include "elector.hh"
#include "idle.hh"
#include "log-spill.hh"
#include "metrics.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "options.hh"
#include "reverse-registry.hh"
#include "reverse-request.hh"

namespace nixgrpc {
class WorkerIdentity;

using Clock = std::chrono::steady_clock;

// One assignment the builder accepted from its scheduler.
struct Expected
{
    uint64_t assignId = 0;
    Clock::time_point since;
    // A client reached us for it. The worker then holds the lease until Done,
    // a give-back or a Revoke: the scheduler revokes it if its client leaves.
    bool claimed = false;
    bool running = false;

    // Second BuildDerivation for the same drv attaches here.
    struct Shared
    {
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic<bool> revoked{false}; // scheduler moved the drv to a worker further along
        std::atomic<unsigned> attached{0};  // callers waiting in attach(); the build outlives its first caller for them
        bool finished = false;
        std::string resultWire; // BuildDerivationDone serialised
        // Written before `finished`.
        std::array<uint64_t, 3> settings{};
        LogSpill spill; // the owner's build log, replayed to whoever attaches

        // Owner only. Waking under the lock keeps an attacher that just
        // checked `written()` from sleeping through this record.
        void logged(SpillKind kind, std::string_view text)
        {
            spill.append(kind, text);
            const std::scoped_lock lock(mutex);
            cv.notify_all();
        }
    };

    std::shared_ptr<Shared> shared = std::make_shared<Shared>();
};

// Builder side: Expect table + outgoing session.
class Builder
{
public:
    // Delivers a message to the scheduler. False if no session is up.
    using SendFn = std::function<bool(const nix::remote::WorkerMsg &)>;

    Builder(const Options & options, Metrics & metrics);

    // From the scheduler.
    void onExpect(const nix::remote::Expect & exp);
    void onRevoke(const nix::remote::Revoke & rev);

    // BuildDerivation entry: nullopt => FAILED_PRECONDITION. A stale assignment
    // must not attach to a newer build of the same drv. `attach` set if the
    // matching assignment already runs.
    struct Admission
    {
        std::shared_ptr<Expected::Shared> shared;
        uint64_t assignId = 0;
        bool attach = false;
    };

    // Any BuildDerivation for the drv, also one answered NOT_FOUND.
    void claim(const std::string & drvPath);
    auto admit(const std::string & drvPath, uint64_t assignId) -> std::optional<Admission>;
    // Read-only admission for an input upload: never consume the Expect.
    [[nodiscard]] auto expects(const std::string & drvPath, uint64_t assignId) const -> bool;
    void finished(
        const std::string & drvPath,
        uint64_t assignId,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        std::string resultWire,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED,
        std::function<void()> * deferredDone = nullptr);
    [[nodiscard]] auto holding() const -> size_t;

    // Hello reflecting current state. Sent on every (re)connect.
    auto hello() const -> nix::remote::WorkerMsg;
    // False if the session is gone.
    auto greet() const -> bool;
    void setDraining(bool draining);

    [[nodiscard]] auto draining() const -> bool
    {
        return draining_;
    }

    // Give back unclaimed Expects nobody came for. Called from the serve loop.
    void tick();

    // Swapped by the session thread on every (re)connect.
    void setSend(SendFn send);
    auto send(const nix::remote::WorkerMsg & msg) const -> bool;

private:
    const Options & options;
    Metrics & metrics;

    struct State
    {
        std::unordered_map<std::string, Expected> expected;
    };

    mutable nix::Sync<State> state;
    mutable nix::Sync<SendFn> sendFn;
    std::atomic<bool> draining_{false};
    static constexpr std::chrono::seconds expectTimeout{60};

    void sendDone(
        const std::string & drvPath,
        uint64_t assignId,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    void giveBack(const std::vector<std::pair<std::string, uint64_t>> & drvs, nix::remote::Done::Reason reason);
    auto takeUnclaimed(const std::function<bool(const Expected &)> & pred) -> std::vector<std::pair<std::string, uint64_t>>;
};

// gRPC front for the Dispatcher: auth, then pump the stream into it.
class SchedulerService final : public nix::remote::Scheduler::CallbackService
{
public:
    SchedulerService(Dispatcher & dispatcher, Auth & auth, IdleTracker & idle);
    void setReverseIdentity(WorkerIdentity & identity) { reverseIdentity = &identity; }

    auto Schedule(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<nix::remote::ClientMsgs, nix::remote::SchedMsgs> * override;
    auto WorkerSession(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<nix::remote::WorkerMsgs, nix::remote::SchedCmds> * override;

    void setActive(bool active)
    {
        active_ = active;
    }
    static auto passive() -> grpc::Status
    {
        return {grpc::StatusCode::UNAVAILABLE, "scheduler passive, another node serves"};
    }

private:
    Dispatcher * dispatcher;
    Auth * auth;
    IdleTracker * idle;
    WorkerIdentity * reverseIdentity = nullptr;
    std::atomic<bool> active_{true};
};

// Owns the pieces and wires loopback vs. remote session.
class Coordinator
{
public:
    using ReverseEmit = std::function<bool(const nix::remote::BuildDerivationChunk &)>;
    using ReverseBuild = std::function<grpc::Status(const ReverseRequest::Ready &, const ReverseEmit &,
                                                     const std::function<bool()> &, std::function<void()> &)>;
    void setReverseBuild(ReverseBuild handler) { reverseBuild = std::move(handler); }
    using ReverseUpload = std::function<grpc::Status(const ReverseRequest::Upload &,
                                                      const std::function<bool()> &)>;
    void setReverseUpload(ReverseUpload handler) { reverseUpload = std::move(handler); }
    Coordinator(const Options & options, Auth & auth, Metrics & metrics, IdleTracker & idle);
    ~Coordinator();
    Coordinator(const Coordinator &) = delete;
    Coordinator(Coordinator &&) = delete;
    auto operator=(const Coordinator &) -> Coordinator & = delete;
    auto operator=(Coordinator &&) -> Coordinator & = delete;

    // NOLINTBEGIN(*-non-private-member-variables-in-classes): the RPC handlers
    // use these directly.
    Cache cache;
    std::optional<Builder> builder;
    std::optional<Dispatcher> dispatcher;
    std::unique_ptr<SchedulerService> scheduler;
    std::unique_ptr<EdsService> eds;
    // Generation-fenced data-session registry, usable only on the leader.
    ReverseRegistry reverseRegistry;
    // NOLINTEND(*-non-private-member-variables-in-classes)

    void start(grpc::Server & server);
    // From the serve loop, ~1 Hz.
    void tick();
    // Before Shutdown(): peers of the scheduler are told to reconnect rather
    // than left to find a dead stream.
    void restarting();

private:
    const Options & options;
    std::jthread sessionThread; // remote WorkerSession client
    std::jthread reverseThread; // opt-in remote ReverseSession client
    std::optional<Elector> elector;
    grpc::HealthCheckServiceInterface * health = nullptr;
    std::atomic<bool> lastHealthy{true};
    ReverseBuild reverseBuild;
    ReverseUpload reverseUpload;
    void setSchedulerActive(bool active);
    void runRemoteSession(Builder & bld, const std::stop_token & stop);
    void runReverseSession(const std::stop_token & stop);
};

} // namespace nixgrpc
