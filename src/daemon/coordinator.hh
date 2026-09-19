#pragma once
// Scheduler gRPC service, the builder's session to it (in-process or remote),
// and the Expect table that gates BuildDerivation.

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
#include "elector.hh"
#include "metrics.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "options.hh"

namespace nixgrpc {

using Clock = std::chrono::steady_clock;

// One assignment the builder accepted from its scheduler.
struct Expected
{
    uint64_t assignId = 0;
    Clock::time_point since;
    bool running = false;

    // Second BuildDerivation for the same drv attaches here.
    struct Shared
    {
        std::mutex mutex;
        std::condition_variable cv;
        bool finished = false;
        std::string resultWire; // BuildDerivationDone serialised
    };

    std::shared_ptr<Shared> shared = std::make_shared<Shared>();
};

// Builder side: Expect table + outgoing session.
class Builder
{
public:
    // Delivers a message to the scheduler; false if no session is up.
    using SendFn = std::function<bool(const nix::remote::WorkerMsg &)>;

    Builder(const Options & options, Metrics & metrics);

    // From the scheduler.
    void onExpect(const nix::remote::Expect & exp);
    void onRevoke(const nix::remote::Revoke & rev);

    // BuildDerivation entry: nullopt => FAILED_PRECONDITION. `attach` set if
    // the drv already runs; caller then waits on it instead of building.
    struct Admission
    {
        std::shared_ptr<Expected::Shared> shared;
        uint64_t assignId = 0;
        bool attach = false;
    };

    auto admit(const std::string & drvPath) -> std::optional<Admission>;
    void finished(
        const std::string & drvPath,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        std::string resultWire);

    // Hello reflecting current state; sent on every (re)connect.
    auto hello() const -> nix::remote::WorkerMsg;
    void setDraining(bool draining);

    [[nodiscard]] auto draining() const -> bool
    {
        return draining_;
    }

    // Expire unclaimed Expects; called from the serve loop.
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
        const std::vector<std::pair<std::string, uint64_t>> & outputs);
};

// gRPC front for the Dispatcher: auth, then pump the stream into it.
class SchedulerService final : public nix::remote::Scheduler::CallbackService
{
public:
    SchedulerService(Dispatcher & dispatcher, Auth & auth);

    auto Schedule(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<nix::remote::ClientMsgs, nix::remote::SchedMsgs> * override;
    auto WorkerSession(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<nix::remote::WorkerMsgs, nix::remote::SchedCmds> * override;

    // Passive: refuse new streams so the balancer retries elsewhere.
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
    std::atomic<bool> active_{true};
};

// Owns the pieces and wires loopback vs. remote session.
class Coordinator
{
public:
    Coordinator(const Options & options, Auth & auth, Metrics & metrics);
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
    std::optional<Elector> elector;
    grpc::HealthCheckServiceInterface * health = nullptr;
    std::atomic<bool> lastHealthy{true};
    void setSchedulerActive(bool active);
    void runRemoteSession(Builder & bld, const std::stop_token & stop);
};

} // namespace nixgrpc
