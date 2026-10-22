#include "coordinator.hh"
#include "dispatcher.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
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
#include "reverse-capability.hh"
#include "reverse-worker.hh"
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
constexpr int minReconnectBackoffMs = 2'000;
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
        auto const found = lck->expected.find(rev.drv_path());
        if (found == lck->expected.end() || found->second.assignId != rev.assign_id()) {
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

auto Builder::admit(const std::string & drvPath, uint64_t assignId) -> std::optional<Admission>
{
    auto lck = state.lock();
    auto const found = lck->expected.find(drvPath);
    if (found == lck->expected.end() || found->second.assignId != assignId) {
        return std::nullopt;
    }
    auto & ent = found->second;
    Admission adm{.shared = ent.shared, .assignId = ent.assignId, .attach = ent.running};
    ent.running = true;
    return adm;
}

auto Builder::expects(const std::string & drvPath, uint64_t assignId) const -> bool
{
    auto lck = state.lock();
    auto const it = lck->expected.find(drvPath);
    return assignId != 0 && it != lck->expected.end() && it->second.assignId == assignId
        && !it->second.shared->revoked;
}

void Builder::finished(
    const std::string & drvPath,
    uint64_t assignId,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs,
    std::string resultWire,
    std::function<void()> * deferredDone)
{
    std::shared_ptr<Expected::Shared> shared;
    {
        auto lck = state.lock();
        auto const found = lck->expected.find(drvPath);
        if (found != lck->expected.end() && found->second.assignId == assignId) {
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
    if (shared) {
        if (deferredDone) {
            // A reverse build must put its terminal response on the data
            // session before retiring the scheduler assignment: the relay
            // fences responses against that live assignment.
            *deferredDone = [this, drvPath, assignId, outcome, outputs] {
                sendDone(drvPath, assignId, outcome, outputs);
            };
        } else {
            sendDone(drvPath, assignId, outcome, outputs);
        }
    }
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
    if (!options.reverseWorkerId.empty()) {
        hel->set_reverse_worker_id(options.reverseWorkerId);
    }
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

// -------------------------------------------------------------- Coordinator

namespace {
// TLS unless the URL says http://. Our server cert doubles as client cert,
// --scheduler-token-file adds a bearer: either, both or neither. The address
// is often the balancer with a public certificate, so trust the system
// bundle as well as the farm CA.
auto schedulerCreds(const Options & options, const std::string & tokenFile) -> std::shared_ptr<grpc::ChannelCredentials>
{
    if (options.schedulerAddr.starts_with(plaintextScheme)) {
        return grpc::InsecureChannelCredentials();
    }
    std::string roots;
    if (auto const bundle = defaultCaCert(); !bundle.empty()) {
        roots = nix::readFile(bundle);
    }
    if (!options.clientCA.empty()) {
        roots += "\n" + nix::readFile(options.clientCA);
    }
    grpc::experimental::TlsChannelCredentialsOptions tls;
    if (!roots.empty()) {
        auto const rootProvider = std::make_shared<grpc::experimental::InMemoryCertificateProvider>();
        if (auto const status = rootProvider->UpdateRoot(roots); !status.ok()) {
            throw nix::Error("loading CA certificates: %s", status.ToString());
        }
        tls.set_root_certificate_provider(rootProvider);
    }
    if (!options.tlsCert.empty()) {
        tls.set_identity_certificate_provider(std::make_shared<grpc::experimental::FileWatcherCertificateProvider>(
            options.tlsKey, options.tlsCert, certRefreshSeconds));
    }
    std::shared_ptr<grpc::ChannelCredentials> creds = grpc::experimental::TlsCredentials(tls);
    if (!tokenFile.empty()) {
        creds = grpc::CompositeChannelCredentials(
            creds,
            grpc::MetadataCredentialsFromPlugin(std::make_unique<TokenFileCredentials>(tokenFile)));
    }
    return creds;
}
} // namespace


void Coordinator::setSchedulerActive(bool active)
{
    reverseRegistry.setActive(active);
    if (health != nullptr) {
        health->SetServingStatus("nix.scheduler", active);
    }
    scheduler->setActive(active);
    eds->setActive(active);
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
        eds = std::make_unique<EdsService>(*dispatcher, auth);
    }
    if (builder && dispatcher && options.schedulerAddr.empty()) {
        // In-process worker: the Dispatcher calls straight into the Builder.
        auto const worker =
            std::make_shared<Dispatcher::Worker>(Dispatcher::Worker{.send = [this](const SchedCmd & cmd) -> bool {
                // Called under the dispatcher mutex; Builder takes only its own lock.
                if (cmd.has_expect()) {
                    builder->onExpect(cmd.expect());
                } else if (cmd.has_revoke()) {
                    // Revoke answers with Done, which re-enters the dispatcher: defer.
                    std::thread([this, rev = cmd.revoke()] -> void { builder->onRevoke(rev); }).detach();
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
    reverseRegistry.setActive(false);
    if (reverseThread.joinable()) {
        reverseThread.request_stop();
        reverseThread.join();
    }
    if (sessionThread.joinable()) {
        sessionThread.request_stop();
        sessionThread.join();
    }
    if (builder) {
        builder->setSend(nullptr);
    }
    eds.reset();
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
        if (!options.reverseTokenFile.empty()) {
            reverseThread = std::jthread([this](const std::stop_token & stop) { runReverseSession(stop); });
        }
    }
}

void Coordinator::runReverseSession(const std::stop_token & stop)
{
    auto const creds = schedulerCreds(options, options.reverseTokenFile);
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, keepaliveMs);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, minReconnectBackoffMs);
    auto const channel = grpc::CreateCustomChannel(options.schedulerAddr, creds, args);
    auto stub = nix::remote::ReverseSession::NewStub(channel);
    auto backoff = minBackoff;
    while (!stop.stop_requested()) {
        grpc::ClientContext ctx;
        const std::stop_callback onStop(stop, [&ctx] { ctx.TryCancel(); });
        auto stream = stub->Register(&ctx);
        nix::remote::ReverseWorkerMsg registration;
        registration.set_worker_id(options.reverseWorkerId);
        nix::remote::ReverseFarmMsg ack;
        if (stream->Write(registration) && stream->Read(&ack) && ack.stream_id() == 0
            && ack.frame_case() == nix::remote::ReverseFarmMsg::FRAME_NOT_SET && ack.generation() != 0
            && ack.session_key().size() == ReverseCapability::keyBytes) {
            logLine(LogLevel::info, {{"event", "reverse_registered"}, {"addr", options.schedulerAddr}});
            backoff = minBackoff;
            ReverseRequest requests(options.reverseWorkerId, ack.generation(), ack.session_key(),
                [this](std::string_view drv, uint64_t id) {
                    return builder && builder->expects(std::string(drv), id);
                });
            if (!reverseBuild || !reverseUpload) {
                ctx.TryCancel();
                (void) stream->Finish();
                continue;
            }
            const std::stop_callback cancelUploads(stop, [&requests] { requests.cancelUploads(); });
            ReverseWorkerOutbound outgoing;
            std::mutex mutex;
            std::condition_variable_any cv;
            std::deque<std::variant<ReverseRequest::Ready, ReverseRequest::Upload>> jobs;
            std::unordered_map<uint64_t, std::shared_ptr<std::atomic<bool>>> cancellations;
            std::unordered_set<uint64_t> queuedJobs;
            bool closed = false;
            std::jthread writer([&](std::stop_token writeStop) {
                nix::remote::ReverseWorkerMsg reply;
                while (outgoing.take(reply, writeStop)) {
                    if (!stream->Write(reply)) { requests.cancelUploads(); ctx.TryCancel(); break; }
                }
            });
            // A bounded pool per registration leaves the reader and control
            // session free while several builds run. The inbound 64-stream
            // cap also bounds the number of queued, decoded requests.
            auto execute = [&](std::stop_token workStop) {
                while (!workStop.stop_requested()) {
                    std::variant<ReverseRequest::Ready, ReverseRequest::Upload> job;
                    std::shared_ptr<std::atomic<bool>> cancelled;
                    {
                        std::unique_lock lock(mutex);
                        cv.wait(lock, workStop, [&] { return closed || !jobs.empty(); });
                        if (closed || workStop.stop_requested()) break;
                        job = std::move(jobs.front());
                        jobs.pop_front();
                        cancelled = cancellations.at(std::visit([](auto const & value) { return value.id; }, job));
                    }
                    auto const id = std::visit([](auto const & value) { return value.id; }, job);
                    grpc::Status status{grpc::StatusCode::UNIMPLEMENTED, "reverse method not dispatched"};
                    std::function<void()> afterFinish;
                    if (std::holds_alternative<ReverseRequest::Ready>(job) && reverseBuild && !*cancelled) {
                        try {
                            status = reverseBuild(std::get<ReverseRequest::Ready>(job),
                                [&](const nix::remote::BuildDerivationChunk & chunk) {
                                    return !*cancelled && !workStop.stop_requested()
                                        && outgoing.message(id, chunk, workStop, [&] { return cancelled->load(); });
                                 }, [&] { return *cancelled || workStop.stop_requested() || stop.stop_requested(); },
                                 afterFinish);
                        } catch (std::exception & error) {
                            status = {grpc::StatusCode::INTERNAL, error.what()};
                        }
                    } else if (std::holds_alternative<ReverseRequest::Upload>(job) && reverseUpload && !*cancelled) {
                        try {
                            status = reverseUpload(std::get<ReverseRequest::Upload>(job),
                                [&] { return *cancelled || workStop.stop_requested() || stop.stop_requested(); });
                        } catch (std::exception & error) {
                            status = {grpc::StatusCode::INTERNAL, error.what()};
                        }
                    } else if (*cancelled) {
                        status = {grpc::StatusCode::CANCELLED, "reverse call cancelled"};
                    }
                    if (auto * upload = std::get_if<ReverseRequest::Upload>(&job)) upload->pipe->cancel();
                    auto const finished = outgoing.finish(id, status, workStop, true, std::move(afterFinish));
                    if (!finished) { ctx.TryCancel(); break; }
                    if (!requests.finished(id)) { ctx.TryCancel(); break; }
                    std::scoped_lock lock(mutex);
                    cancellations.erase(id);
                    queuedJobs.erase(id);
                }
            };
            std::vector<std::jthread> executors;
            constexpr size_t maxExecutors = 4;
            for (size_t i = 0; i < std::min<size_t>(maxExecutors, std::max(1u, options.maxJobs)); ++i)
                executors.emplace_back(execute);
            nix::remote::ReverseFarmMsg frame;
            while (!stop.stop_requested() && stream->Read(&frame)) {
                if (frame.frame_case() == nix::remote::ReverseFarmMsg::kAcknowledged) {
                    if (!frame.acknowledged() || frame.stream_id() == 0 || frame.generation() != 0
                        || !frame.session_key().empty() || !outgoing.acknowledged(frame.stream_id())) {
                        ctx.TryCancel();
                        break;
                    }
                    continue;
                }
                if (frame.has_open() && frame.open().method() != "BuildDerivation"
                    && frame.open().method() != "UploadBuildInputs") {
                    ctx.TryCancel();
                    break;
                }
                if (!requests.accept(frame)) { ctx.TryCancel(); break; }
                if (frame.has_open()) {
                    std::scoped_lock lock(mutex);
                    if (!cancellations.emplace(frame.stream_id(), std::make_shared<std::atomic<bool>>(false)).second) {
                        ctx.TryCancel();
                        break;
                    }
                }
                if (frame.cancel()) {
                    outgoing.cancel(frame.stream_id());
                    if (!outgoing.healthy()) { ctx.TryCancel(); break; }
                    bool beforeRequest = false;
                    {
                        std::scoped_lock lock(mutex);
                        if (auto it = cancellations.find(frame.stream_id()); it != cancellations.end()) {
                            *it->second = true;
                            beforeRequest = !queuedJobs.contains(frame.stream_id());
                            if (beforeRequest) cancellations.erase(it);
                        }
                    }
                    if (beforeRequest && !outgoing.finish(frame.stream_id(),
                            {grpc::StatusCode::CANCELLED, "reverse request cancelled"}, stop, false)) {
                        ctx.TryCancel();
                        break;
                    }
                    if (beforeRequest && !requests.finished(frame.stream_id())) { ctx.TryCancel(); break; }
                }
                if (auto upload = requests.takeUploadReady()) {
                    std::scoped_lock lock(mutex);
                    queuedJobs.insert(upload->id);
                    jobs.emplace_back(std::move(*upload));
                    cv.notify_one();
                }
                if (auto ready = requests.takeReady()) {
                    std::scoped_lock lock(mutex);
                    queuedJobs.insert(ready->id);
                    jobs.push_back(std::move(*ready));
                    cv.notify_one();
                }
            }
            ctx.TryCancel(); // wake a writer blocked in gRPC Write on disconnect
            {
                std::scoped_lock lock(mutex);
                closed = true;
                for (auto & [id, flag] : cancellations) *flag = true;
            }
            cv.notify_all();
            requests.close(); // unblock upload sources before joining executors
            // A build thread may be blocked in response backpressure while
            // the writer is stuck in (or has left) gRPC Write. Wake those
            // producers before joining executors, not after.
            outgoing.close();
            for (auto & executor : executors) executor.request_stop();
            for (auto & executor : executors) executor.join();
            writer.request_stop();
            writer.join();
            // On disconnect the scheduler must not retain completed builds
            // whose terminal response never received a gateway acknowledgement.
            outgoing.abandonUnacknowledged();
            stream->WritesDone();
        } else {
            // A malformed or unexpected first frame must not leave Finish()
            // waiting indefinitely for a server that expects more input.
            ctx.TryCancel();
        }
        auto const status = stream->Finish();
        if (stop.stop_requested()) {
            return;
        }
        logLine(LogLevel::info, {{"event", "reverse_disconnected"}, {"addr", options.schedulerAddr},
                                 {"error", status.error_message()}});
        // Cancellation must also interrupt reconnect backoff during daemon
        // shutdown, not only an in-flight gRPC Read.
        std::mutex waitMutex;
        std::condition_variable_any waitCv;
        std::unique_lock lock(waitMutex);
        waitCv.wait_for(lock, stop, backoff, [] { return false; });
        backoff = std::min(backoff * 2, maxBackoff);
    }
}

void Coordinator::runRemoteSession(Builder & bld, const std::stop_token & stop)
{
    auto const creds = schedulerCreds(options, options.reverseTokenFile.empty() ? options.schedulerTokenFile
                                                                                 : options.reverseTokenFile);
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, keepaliveMs);
    // A replaced balancer pod blackholes the first SYN; do not sit out gRPC's 20s.
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, minReconnectBackoffMs);
    auto const target = options.schedulerAddr.starts_with(plaintextScheme) ? options.schedulerAddr.substr(plaintextScheme.size())
                                                                    : options.schedulerAddr;
    auto const channel = grpc::CreateCustomChannel(target, creds, args);
    auto stub = nix::remote::Scheduler::NewStub(channel);
    auto backoff = minBackoff;
    while (!stop.stop_requested()) {
        grpc::ClientContext ctx;
        auto stream = stub->WorkerSession(&ctx);
        auto const writeMutex = std::make_shared<std::mutex>();
        auto const live = std::make_shared<std::atomic<bool>>(true);
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
        const std::stop_callback onStop(stop, [&ctx] -> void { ctx.TryCancel(); });
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
        auto const status = stream->Finish();
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
