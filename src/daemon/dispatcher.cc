#include "dispatcher.hh"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <initializer_list>
#include <mutex>
#include <optional>
#include <ratio>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nix/util/error.hh>
#include <nix/util/strings.hh>
#include <nix/util/types.hh>

#include "logfmt.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"
#include "scheduler.hh"

namespace nixgrpc {

using nix::remote::SchedCmd;
using nix::remote::SchedMsg;

namespace {
constexpr size_t maxSystemLen = 64;
} // namespace

Dispatcher::Dispatcher(Config config_, Metrics & metrics)
    : config(std::move(config_))
    , metrics(metrics)
    , assignedCtr(metrics.eventCounter("assigned"))
{
    if (config.present) {
        for (unsigned i = 0; i < config.lookupThreads; i++) {
            lookupThreads.emplace_back([this]() -> void { lookupLoop(); });
        }
    }
}

Dispatcher::~Dispatcher()
{
    {
        const std::scoped_lock lock(lookupMutex);
        lookupStop = true;
    }
    lookupCv.notify_all();
    for (auto & thr : lookupThreads) {
        thr.join();
    }
}

void Dispatcher::lookupLoop()
{
    for (;;) {
        LookupJob job;
        {
            std::unique_lock lock(lookupMutex);
            lookupCv.wait(lock, [this]() -> bool { return lookupStop || !lookupQueue.empty(); });
            if (lookupQueue.empty()) {
                return;
            }
            job = std::move(lookupQueue.front());
            lookupQueue.pop_front();
        }
        applyClientMsgs(*job.client, job.msgs, lookup(job.msgs));
    }
}

Dispatcher::Lock::Lock(Dispatcher & disp)
    : disp(disp)
{
    disp.mutex.lock();
}

Dispatcher::Lock::~Lock()
{
    std::vector<std::function<void()>> run;
    run.swap(disp.deferred);
    disp.mutex.unlock();
    for (auto & func : run) {
        // A destructor must not throw. Deferred work is stream I/O kick-off,
        // a failure there ends that stream and is not ours to report.
        try {
            func();
        } catch (...) { // NOLINT(bugprone-empty-catch): nothing to tell, the stream ends on its own
        }
    }
}

void Dispatcher::afterUnlock(std::function<void()> func)
{
    deferred.push_back(std::move(func));
}

auto Dispatcher::nowMs() const -> double
{
    return std::chrono::duration<double, std::milli>(Clock::now() - epoch).count();
}

void Dispatcher::debugLog(std::initializer_list<LogField> fields) const
{
    if (config.logLevel == LogLevel::debug) {
        logLine(LogLevel::debug, fields);
    }
}

// "builtin" (fetchurl) runs anywhere. Count it as the scheduler's own system.
auto Dispatcher::systemFor(std::string_view system) const -> std::string_view
{
    if (system.empty() || system == "builtin") {
        return config.defaultSystem;
    }
    return system;
}

// Only the shared cache counts; a worker-local output is answered by that
// worker's BuildDerivation as AlreadyValid.
auto Dispatcher::wantsLookup(const nix::remote::Want & want) -> bool
{
    return want.build_mode() == 0 && !want.out_keys().empty() && !want.drv_path().empty();
}

auto Dispatcher::lookup(const nix::remote::ClientMsgs & msgs) const -> std::vector<bool>
{
    std::vector<bool> cached(static_cast<size_t>(msgs.msgs_size()), false);
    std::vector<std::string> keys;
    for (const auto & msg : msgs.msgs()) {
        if (msg.has_want() && wantsLookup(msg.want())) {
            keys.insert(keys.end(), msg.want().out_keys().begin(), msg.want().out_keys().end());
        }
    }
    if (!config.present || keys.empty()) {
        return cached;
    }
    try {
        auto have = config.present(keys);
        for (int i = 0; i < msgs.msgs_size(); i++) {
            const auto & msg = msgs.msgs(i);
            cached.at(static_cast<size_t>(i)) =
                msg.has_want() && wantsLookup(msg.want())
                && std::ranges::all_of(
                    msg.want().out_keys(), [&](const std::string & key) -> bool { return have.contains(key); });
        }
    } catch (std::exception & err) {
        logLine(LogLevel::info, {{"event", "present_failed"}, {"error", err.what()}});
    }
    return cached;
}

void Dispatcher::dispatchLocked()
{
    std::vector<sched::Assign> out;
    core.dispatch(out);
    for (auto & asg : out) {
        const auto & ent = core.entry(asg.drv);
        if (!ent) {
            continue;
        }
        const auto & wkr = core.worker(asg.worker);
        SchedCmd cmd;
        cmd.mutable_expect()->set_drv_path(ent->drvPath);
        cmd.mutable_expect()->set_assign_id(asg.assignId);
        auto wit = workers.find(asg.worker);
        if (wit == workers.end() || !wit->second(cmd)) {
            // Worker stream died under us; requeue and let the next event retry.
            core.workerGone(asg.worker);
            workers.erase(asg.worker);
            metrics.event("expect_undeliverable");
            continue;
        }
        SchedMsg msg;
        msg.mutable_assigned()->set_drv_path(ent->drvPath);
        msg.mutable_assigned()->set_worker_addr(wkr.addr);
        msg.mutable_assigned()->set_assign_id(asg.assignId);
        for (auto cid : asg.clients) {
            if (auto cit = clients.find(cid); cit != clients.end()) {
                cit->second(msg);
            }
        }
        assignedCtr.Increment();
        debugLog(
            {{"event", "assigned"},
             {"drv", ent->drvPath},
             {"worker", wkr.addr},
             {"clients", std::to_string(asg.clients.size())}});
    }
    metrics.schedQueued(core.queued());
}

void Dispatcher::revokeOn(sched::WorkerId wid, const std::string & drvPath)
{
    if (auto wit = workers.find(wid); wit != workers.end()) {
        SchedCmd cmd;
        cmd.mutable_revoke()->set_drv_path(drvPath);
        wit->second(cmd);
    }
}

// ------------------------------------------------------------------ clients

auto Dispatcher::connectClient(ClientSend send) -> ClientPtr
{
    metrics.schedClients(1);
    auto client = std::make_shared<Client>();
    client->id = nextClient++;
    client->send = std::move(send);
    return client;
}

void Dispatcher::serving()
{
    const Lock lock(*this);
    lettingGo = false;
}

void Dispatcher::restarting()
{
    const Lock lock(*this);
    lettingGo = true;
    SchedMsg msg;
    msg.mutable_restarting();
    SchedCmd cmd;
    cmd.mutable_restarting();
    for (auto & [cid, send] : clients) {
        send(msg);
    }
    for (auto & [wid, send] : workers) {
        send(cmd);
    }
}

void Dispatcher::clientMsgs(const ClientPtr & client, const nix::remote::ClientMsgs & msgs)
{
    const bool needs =
        config.present && std::ranges::any_of(msgs.msgs(), [](const nix::remote::ClientMsg & msg) -> bool {
            return msg.has_want() && wantsLookup(msg.want());
        });
    if (needs && !lookupThreads.empty()) {
        {
            const std::scoped_lock lock(lookupMutex);
            lookupQueue.push_back({.client = client, .msgs = msgs});
        }
        lookupCv.notify_one();
        return;
    }
    applyClientMsgs(*client, msgs, lookup(msgs));
}

void Dispatcher::applyClientMsgs(
    Client & client, const nix::remote::ClientMsgs & msgs, const std::vector<bool> & cached)
{
    const Lock lock(*this);
    if (client.gone) {
        return;
    }
    for (int i = 0; i < msgs.msgs_size(); i++) {
        const auto & msg = msgs.msgs(i);
        if (msg.has_want()) {
            onWant(client, msg.want(), cached.at(static_cast<size_t>(i)));
        } else if (msg.has_cancel()) {
            onCancel(client, msg.cancel());
        }
    }
}

// Under the lock.
void Dispatcher::onWant(Client & client, const nix::remote::Want & want, bool cached)
{
    if (want.drv_path().empty()) {
        return;
    }
    debugLog(
        {{"event", "want"},
         {"client", std::to_string(client.id)},
         {"drv", want.drv_path()},
         {"cached", cached ? "1" : "0"}});
    SchedMsg reply;
    if (cached) {
        reply.mutable_cached()->set_drv_path(want.drv_path());
        client.send(reply);
        metrics.event("cached");
        return;
    }
    if (want.system().size() > maxSystemLen) {
        reply.mutable_unplaceable()->set_drv_path(want.drv_path());
        reply.mutable_unplaceable()->set_reason("bad system '" + want.system() + "'");
        client.send(reply);
        return;
    }
    if (!client.known) {
        client.known = true;
        clients[client.id] = client.send;
    }
    auto system = systemFor(want.system());
    auto res = core.want(
        client.id,
        {.drvPath = want.drv_path(),
         .inputs = {want.inputs().begin(), want.inputs().end()}, // string_views into `want`
         .system = system,
         .features = {want.required_features().begin(), want.required_features().end()},
         .cpHintMs = static_cast<double>(want.cp_hint_ms())},
        nowMs());
    if (res.assigned) {
        reply.mutable_assigned()->set_drv_path(want.drv_path());
        reply.mutable_assigned()->set_worker_addr(core.worker(res.assigned->first).addr);
        reply.mutable_assigned()->set_assign_id(res.assigned->second);
        client.send(reply);
        return;
    }
    dispatchLocked();
    if (!core.placeable(res.drv)) {
        // Say so instead of queueing forever.
        core.cancel(client.id, res.drv);
        reply.mutable_unplaceable()->set_drv_path(want.drv_path());
        reply.mutable_unplaceable()->set_reason(
            "no connected, non-draining worker for system '" + std::string(system) + "' offers features {"
            + nix::concatStringsSep(",", nix::Strings(want.required_features().begin(), want.required_features().end()))
            + "}");
        client.send(reply);
    }
}

// Under the lock.
void Dispatcher::onCancel(Client & client, const nix::remote::Cancel & cancel)
{
    if (auto wid = core.cancel(client.id, cancel.drv_path())) {
        revokeOn(*wid, cancel.drv_path());
    }
    metrics.schedQueued(core.queued());
}

void Dispatcher::clientGone(const ClientPtr & clientPtr)
{
    auto & client = *clientPtr;
    const Lock lock(*this);
    client.gone = true;
    clients.erase(client.id);
    std::vector<std::pair<sched::DrvId, sched::WorkerId>> revokes;
    core.clientGone(client.id, revokes);
    for (auto [drv, wid] : revokes) {
        // A client we sent away comes back for the same build. Keep it running.
        if (const auto  & ent = core.entry(drv); ent && !lettingGo) {
            revokeOn(wid, ent->drvPath);
        }
    }
    metrics.schedQueued(core.queued());
    metrics.schedClients(-1);
}

// ------------------------------------------------------------------ workers

void Dispatcher::workerMsgs(Worker & worker, const nix::remote::WorkerMsgs & msgs)
{
    if (msgs.msgs().empty()) {
        return;
    }
    const Lock lock(*this);
    if (!worker.id && !msgs.msgs(0).has_hello()) {
        throw nix::Error("WorkerSession: first message must be Hello");
    }
    for (const auto & msg : msgs.msgs()) {
        workerMsgLocked(worker, msg);
    }
    dispatchLocked();
    metrics.schedWorkers(core.workersUp());
}

void Dispatcher::workerMsgLocked(Worker & worker, const nix::remote::WorkerMsg & msg)
{
    if (msg.has_hello()) {
        const auto & hel = msg.hello();
        std::vector<std::string_view> running;
        running.reserve(static_cast<size_t>(hel.running_size()));
        for (const auto & run : hel.running()) {
            running.emplace_back(run.drv_path());
        }
        if (hel.systems().empty()) {
            throw nix::Error("worker %s offers no system", hel.addr());
        }
        std::vector<std::string> systems;
        for (const auto & name : hel.systems()) {
            if (name.size() > maxSystemLen) {
                throw nix::Error("WorkerSession: bad system '%s'", name);
            }
            systems.emplace_back(systemFor(name));
        }
        worker.id = core.hello(
            {.addr = hel.addr(),
             .systems = std::move(systems),
             .features = {hel.features().begin(), hel.features().end()},
             .maxJobs = hel.max_jobs(),
             .running = std::move(running)});
        workers[*worker.id] = worker.send;
        logLine(
            LogLevel::info,
            {{"event", "worker_hello"},
             {"addr", hel.addr()},
             {"systems", nix::concatStringsSep(",", core.worker(*worker.id).systems)},
             {"max_jobs", std::to_string(hel.max_jobs())},
             {"running", std::to_string(hel.running_size())}});
    } else if (!worker.id) {
        throw nix::Error("WorkerSession: first message must be Hello");
    } else if (msg.has_done()) {
        const auto & done = msg.done();
        std::vector<std::pair<std::string_view, uint64_t>> outputs;
        if (done.outcome() == nix::remote::Done::BUILT) {
            for (const auto & out : done.outputs()) {
                outputs.emplace_back(out.path(), out.nar_size());
            }
        }
        core.done(*worker.id, done.drv_path(), outputs);
        metrics.event("done_" + nix::remote::Done::Outcome_Name(done.outcome()));
    } else if (msg.has_load()) {
        core.setDraining(*worker.id, msg.load().draining());
    }
}

void Dispatcher::workerGone(Worker & worker)
{
    const Lock lock(*this);
    if (!worker.id) {
        return;
    }
    logLine(LogLevel::info, {{"event", "worker_gone"}, {"addr", core.worker(*worker.id).addr}});
    core.workerGone(*worker.id);
    workers.erase(*worker.id);
    dispatchLocked();
    metrics.schedWorkers(core.workersUp());
    worker.id.reset();
}

} // namespace nixgrpc
