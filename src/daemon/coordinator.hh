#pragma once
// Scheduler gRPC service, the builder's session to it (in-process or remote),
// and the Expect table that gates BuildDerivation.

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/store-api.hh>
#include <nix/util/types.hh>

#include "auth.hh"
#include "builder.hh"
#include "cache.hh"
#include "dispatcher.hh"
#include "eds.hh"
#include "elector.hh"
#include "idle.hh"
#include "metrics.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "options.hh"

namespace nixgrpc {

// gRPC front for the Dispatcher: auth, then pump the stream into it.
class SchedulerService final : public nix::remote::Scheduler::CallbackService
{
public:
    SchedulerService(Dispatcher & dispatcher, Auth & auth, IdleTracker & idle);

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
    std::atomic<bool> active_{true};
};

// Owns the pieces and wires loopback vs. remote session.
class Coordinator
{
public:
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
    // NOLINTEND(*-non-private-member-variables-in-classes)

    void start(grpc::Server & server);
    // From the serve loop, ~1 Hz.
    void tick();
    // Before Shutdown(): peers of the scheduler are told to reconnect rather
    // than left to find a dead stream.
    void restarting();

private:
    const Options & options;
    Metrics & metrics;
    std::jthread sessionThread; // remote WorkerSession client
    std::jthread probeThread;
    // Read and written by start() and then by the probe thread only.
    bool daemonAnswered = true;
    std::optional<Elector> elector;
    grpc::HealthCheckServiceInterface * health = nullptr;
    std::atomic<bool> lastHealthy{true};
    void setSchedulerActive(bool active);
    void runRemoteSession(Builder & bld, const std::stop_token & stop);
    void askLocalDaemon(Builder & bld);
    void watchLocalDaemon(Builder & bld, const std::stop_token & stop);
};

} // namespace nixgrpc
