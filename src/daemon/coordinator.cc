#include "coordinator.hh"
#include "dispatcher.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/statvfs.h>

#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/tls_certificate_provider.h>
#include <grpcpp/security/tls_credentials_options.h>
#include <grpcpp/server.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/globals.hh>
#include <nix/util/file-system.hh>

#include "acl.hh"
#include "auth.hh"
#include "backend.hh"
#include "cache.hh"
#include "client/channel.hh"
#include "logfmt.hh"
#include "metrics.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "options.hh"
#include "sched-reactor.hh"

namespace nixgrpc {

using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedCmds;
using nix::remote::SchedMsgs;
using nix::remote::WorkerMsg;
using nix::remote::WorkerMsgs;

namespace {
constexpr int keepaliveMs = 20'000;
constexpr std::chrono::milliseconds minBackoff{200};
constexpr std::chrono::milliseconds maxBackoff{5000};
} // namespace

// ---------------------------------------------------------------- Builder

Builder::Builder(const Options & options, Metrics & metrics)
    : options(options)
    , metrics(metrics)
{
}

void Builder::onExpect(const nix::remote::Expect & exp)
{
    if (exp.drv_path().empty()) {
        return;
    }
    auto lck = state.lock();
    auto & ent = lck->expected[exp.drv_path()];
    if (ent.running) {
        return; // re-announced across a scheduler restart, keep the live one
    }
    ent.assignId = exp.assign_id();
    ent.since = Clock::now();
    metrics.event("expect");
}

void Builder::onRevoke(const nix::remote::Revoke & rev)
{
    uint64_t assignId = 0;
    {
        auto lck = state.lock();
        auto found = lck->expected.find(rev.drv_path());
        if (found == lck->expected.end()) {
            return;
        }
        if (found->second.running) {
            // The build RPC notices, kills the build and reports Done itself.
            found->second.shared->revoked = true;
            logLine(LogLevel::info, {{"event", "revoked_running"}, {"drv", rev.drv_path()}});
            return;
        }
        assignId = found->second.assignId;
        lck->expected.erase(found);
    }
    sendDone(rev.drv_path(), assignId, nix::remote::Done::CANCELLED, {});
}

auto Builder::admit(const std::string & drvPath) -> std::optional<Admission>
{
    auto lck = state.lock();
    auto found = lck->expected.find(drvPath);
    if (found == lck->expected.end()) {
        return std::nullopt;
    }
    auto & ent = found->second;
    Admission adm{.shared = ent.shared, .assignId = ent.assignId, .attach = ent.running};
    ent.running = true;
    return adm;
}

void Builder::finished(
    const std::string & drvPath,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs,
    std::string resultWire)
{
    uint64_t assignId = 0;
    std::shared_ptr<Expected::Shared> shared;
    {
        auto lck = state.lock();
        auto found = lck->expected.find(drvPath);
        if (found != lck->expected.end()) {
            assignId = found->second.assignId;
            shared = found->second.shared;
            lck->expected.erase(found);
        }
    }
    if (shared) {
        const std::scoped_lock lock(shared->mutex);
        shared->finished = true;
        shared->resultWire = std::move(resultWire);
        shared->cv.notify_all();
    }
    sendDone(drvPath, assignId, outcome, outputs);
}

void Builder::sendDone(
    const std::string & drvPath,
    uint64_t assignId,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs)
{
    WorkerMsg msg;
    auto * done = msg.mutable_done();
    done->set_drv_path(drvPath);
    done->set_assign_id(assignId);
    done->set_outcome(outcome);
    for (const auto & [path, size] : outputs) {
        auto * out = done->add_outputs();
        out->set_path(path);
        out->set_nar_size(size);
    }
    if (!send(msg)) {
        // Session down: the next Hello omits this drv, which frees the slot.
        metrics.event("done_unsent");
    }
}

auto Builder::hello() const -> WorkerMsg
{
    WorkerMsg msg;
    auto * hel = msg.mutable_hello();
    hel->set_addr(options.advertise);
    hel->add_systems(nix::settings.thisSystem.get());
    for (const auto & extra : nix::settings.extraPlatforms.get()) {
        hel->add_systems(extra);
    }
    for (const auto & feat : nix::settings.systemFeatures.get()) {
        hel->add_features(feat);
    }
    hel->set_max_jobs(draining_ ? 0 : options.maxJobs);
    auto lck = state.lock();
    for (const auto & [drv, ent] : lck->expected) {
        auto * run = hel->add_running();
        run->set_drv_path(drv);
        run->set_assign_id(ent.assignId);
    }
    return msg;
}

void Builder::setDraining(bool draining)
{
    if (draining_.exchange(draining) == draining) {
        return;
    }
    WorkerMsg msg;
    msg.mutable_load()->set_draining(draining);
    send(msg);
}

void Builder::setSend(SendFn send)
{
    *sendFn.lock() = std::move(send);
}

auto Builder::send(const WorkerMsg & msg) const -> bool
{
    const SendFn cur = *sendFn.lock(); // copy: do not hold the lock across stream I/O
    return cur && cur(msg);
}

void Builder::tick()
{
    std::vector<std::pair<std::string, uint64_t>> expired;
    {
        auto lck = state.lock();
        const auto now = Clock::now();
        for (auto iter = lck->expected.begin(); iter != lck->expected.end();) {
            if (!iter->second.running && now - iter->second.since > expectTimeout) {
                expired.emplace_back(iter->first, iter->second.assignId);
                iter = lck->expected.erase(iter);
            } else {
                ++iter;
            }
        }
    }
    for (const auto & [drv, assignId] : expired) {
        metrics.event("expect_no_show");
        sendDone(drv, assignId, nix::remote::Done::NO_SHOW, {});
    }
}

// ---------------------------------------------------------- SchedulerService

SchedulerService::SchedulerService(Dispatcher & dispatcher, Auth & auth)
    : dispatcher(&dispatcher)
    , auth(&auth)
{
}


auto SchedulerService::Schedule(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<ClientMsgs, SchedMsgs> *
{
    if (!active_) {
        return new RejectReactor<ClientMsgs, SchedMsgs>(passive()); // NOLINT(cppcoreguidelines-owning-memory): deletes itself in OnDone
    }
    auto caller = auth->identify(*context);
    if (auto status = Auth::authorize(caller, "Schedule", Role::write); !status.ok()) {
        return new RejectReactor<ClientMsgs, SchedMsgs>(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    return new ScheduleReactor(context, *dispatcher); // NOLINT(cppcoreguidelines-owning-memory)
}

auto SchedulerService::WorkerSession(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<WorkerMsgs, SchedCmds> *
{
    if (!active_) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(passive()); // NOLINT(cppcoreguidelines-owning-memory)
    }
    auto caller = auth->identify(*context);
    if (auto status = Auth::authorize(caller, "WorkerSession", Role::trusted); !status.ok()) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    logLine(LogLevel::info, {{"event", "worker_session_open"}, {"cn", caller.name}, {"peer", context->peer()}});
    return new WorkerSessionReactor(context, *dispatcher); // NOLINT(cppcoreguidelines-owning-memory)
}

// -------------------------------------------------------------- Coordinator

namespace {
// TLS unless the URL says http://. Our server cert doubles as client cert,
// --scheduler-token-file adds a bearer: either, both or neither. The address
// is often the balancer with a public certificate, so trust the system
// bundle as well as the farm CA.
auto schedulerCreds(const Options & options) -> std::shared_ptr<grpc::ChannelCredentials>
{
    if (options.schedulerAddr.starts_with(plaintextScheme)) {
        return grpc::InsecureChannelCredentials();
    }
    std::string roots;
    if (auto bundle = defaultCaCert(); !bundle.empty()) {
        roots = nix::readFile(bundle);
    }
    if (!options.clientCA.empty()) {
        roots += "\n" + nix::readFile(options.clientCA);
    }
    grpc::experimental::TlsChannelCredentialsOptions tls;
    if (!roots.empty()) {
        auto rootProvider = std::make_shared<grpc::experimental::InMemoryCertificateProvider>();
        if (auto status = rootProvider->UpdateRoot(roots); !status.ok()) {
            throw nix::Error("loading CA certificates: %s", status.ToString());
        }
        tls.set_root_certificate_provider(rootProvider);
    }
    if (!options.tlsCert.empty()) {
        tls.set_identity_certificate_provider(std::make_shared<grpc::experimental::FileWatcherCertificateProvider>(
            options.tlsKey, options.tlsCert, certRefreshSeconds));
    }
    std::shared_ptr<grpc::ChannelCredentials> creds = grpc::experimental::TlsCredentials(tls);
    if (!options.schedulerTokenFile.empty()) {
        creds = grpc::CompositeChannelCredentials(
            creds,
            grpc::MetadataCredentialsFromPlugin(std::make_unique<TokenFileCredentials>(options.schedulerTokenFile)));
    }
    return creds;
}
} // namespace


void Coordinator::setSchedulerActive(bool active)
{
    if (health != nullptr) {
        health->SetServingStatus("nix.scheduler", active);
    }
    scheduler->setActive(active);
    if (active) {
        dispatcher->serving(); // NOLINT(bugprone-unchecked-optional-access): set iff scheduler
    } else {
        dispatcher->restarting(); // NOLINT(bugprone-unchecked-optional-access)
    }
}

void Coordinator::restarting()
{
    if (!dispatcher) {
        return;
    }
    elector.reset();
    setSchedulerActive(false);
    logLine(LogLevel::info, {{"event", "scheduler_restarting"}});
    // No wait for the writes to flush: Shutdown() lets in-flight writes
    // finish, and a peer that misses the message just reconnects the old way.
}

Coordinator::Coordinator(const Options & options, Auth & auth, Metrics & metrics)
    : cache(options.niks3)
    , options(options)
{
    if (options.builder) {
        builder.emplace(options, metrics);
        metrics.buildSlots(options.maxJobs);
    }
    if (options.scheduler) {
        Dispatcher::PresentFn present;
        if (cache.hasRemote()) {
            present = [this](const std::vector<std::string> & keys) -> std::unordered_set<std::string> {
                return cache.present(keys);
            };
        }
        dispatcher.emplace(
            Dispatcher::Config{
                .defaultSystem = nix::settings.thisSystem.get(),
                .present = std::move(present),
                .logLevel = options.logLevel},
            metrics);
        scheduler = std::make_unique<SchedulerService>(*dispatcher, auth);
    }
    if (builder && dispatcher && options.schedulerAddr.empty()) {
        // In-process worker: the Dispatcher calls straight into the Builder.
        auto worker =
            std::make_shared<Dispatcher::Worker>(Dispatcher::Worker{.send = [this](const SchedCmd & cmd) -> bool {
                // Called under the dispatcher mutex; Builder takes only its own lock.
                if (cmd.has_expect()) {
                    builder->onExpect(cmd.expect());
                } else if (cmd.has_revoke()) {
                    // Revoke answers with Done, which re-enters the dispatcher: defer.
                    std::thread([this, rev = cmd.revoke()]() -> void { builder->onRevoke(rev); }).detach();
                }
                return true;
            }});
        builder->setSend([this, worker](const WorkerMsg & msg) -> bool {
            WorkerMsgs one;
            *one.add_msgs() = msg;
            dispatcher->workerMsgs(*worker, one);
            return true;
        });
    }
}

// Members would go in reverse declaration order: session thread and
// dispatcher after the Builder they call into. Cut the edges first.
Coordinator::~Coordinator()
{
    elector.reset();
    if (sessionThread.joinable()) {
        sessionThread.request_stop();
        sessionThread.join();
    }
    if (builder) {
        builder->setSend(nullptr);
    }
    scheduler.reset();
    dispatcher.reset();
}

void Coordinator::start(grpc::Server & server)
{
    health = server.GetHealthCheckService();
    if (scheduler) {
        // A node that schedules only onto itself has no peers to elect among.
        const bool elect = options.niks3.enabled() && (!builder || !options.schedulerAddr.empty());
        setSchedulerActive(!elect);
        if (elect) {
            elector.emplace(*cache.client(), [this](bool lead) -> void { setSchedulerActive(lead); });
        }
    }
    if (!builder) {
        return;
    }
    if (options.schedulerAddr.empty()) {
        builder->send(builder->hello());
    } else {
        sessionThread = std::jthread(
            [this, &bld = *builder](const std::stop_token & stop) -> void { runRemoteSession(bld, stop); });
    }
}

void Coordinator::runRemoteSession(Builder & bld, const std::stop_token & stop)
{
    auto creds = schedulerCreds(options);
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, keepaliveMs);
    auto target = options.schedulerAddr.starts_with(plaintextScheme) ? options.schedulerAddr.substr(plaintextScheme.size())
                                                                    : options.schedulerAddr;
    auto channel = grpc::CreateCustomChannel(target, creds, args);
    auto stub = nix::remote::Scheduler::NewStub(channel);
    auto backoff = minBackoff;
    while (!stop.stop_requested()) {
        grpc::ClientContext ctx;
        auto stream = stub->WorkerSession(&ctx);
        auto writeMutex = std::make_shared<std::mutex>();
        auto live = std::make_shared<std::atomic<bool>>(true);
        auto * raw = stream.get();
        // Builders send a Done every few seconds at most. No batching needed.
        bld.setSend([raw, writeMutex, live](const WorkerMsg & msg) -> bool {
            WorkerMsgs one;
            *one.add_msgs() = msg;
            const std::scoped_lock lock(*writeMutex);
            return *live && raw->Write(one);
        });
        const bool helloed = bld.send(bld.hello());
        if (helloed) {
            logLine(LogLevel::info, {{"event", "scheduler_connected"}, {"addr", options.schedulerAddr}});
            backoff = minBackoff;
        }
        const std::stop_callback onStop(stop, [&ctx]() -> void { ctx.TryCancel(); });
        SchedCmds cmds;
        bool restarting = false;
        while (helloed && stream->Read(&cmds)) {
            for (const auto & cmd : cmds.msgs()) {
                if (cmd.has_expect()) {
                    bld.onExpect(cmd.expect());
                } else if (cmd.has_revoke()) {
                    bld.onRevoke(cmd.revoke());
                } else if (cmd.has_restarting()) {
                    restarting = true;
                }
            }
            if (restarting) {
                // Leave now. A yielding scheduler keeps the connection open.
                ctx.TryCancel();
            }
        }
        *live = false;
        auto status = stream->Finish();
        if (stop.stop_requested()) {
            return;
        }
        if (restarting) {
            // Announced: go straight back, the successor is (about to be) up.
            logLine(LogLevel::info, {{"event", "scheduler_restarting"}, {"addr", options.schedulerAddr}});
            backoff = minBackoff;
        } else {
            logLine(
                LogLevel::info,
                {{"event", "scheduler_disconnected"}, {"addr", options.schedulerAddr}, {"error", status.error_message()}});
        }
        std::this_thread::sleep_for(backoff);
        backoff = std::min(backoff * 2, maxBackoff);
    }
}

void Coordinator::tick()
{
    if (!builder) {
        return;
    }
    builder->tick();
    struct statvfs vfs{};
    if (options.minFree == 0 || statvfs(options.storeDir.c_str(), &vfs) != 0) {
        return;
    }
    const bool healthy = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize >= options.minFree;
    if (lastHealthy.exchange(healthy) != healthy) {
        logLine(LogLevel::info, {{"event", healthy ? "healthy" : "unhealthy"}, {"reason", "min_free"}});
        if (stopSignal == 0) {
            builder->setDraining(!healthy);
        }
    }
}

} // namespace nixgrpc
