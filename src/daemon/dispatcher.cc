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
#include <unordered_set>
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
        func();
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

// One shard per system, created on first use; std::map keeps references stable.
auto Dispatcher::shardFor(std::string_view system) -> ShardState *
{
    // "builtin" (fetchurl) runs on any system.
    if (system.empty() || system == "builtin") {
        system = config.defaultSystem;
    }
    if (system.size() > maxSystemLen) {
        return nullptr;
    }
    auto iter = shards.find(system);
    if (iter == shards.end()) {
        iter =
            shards.try_emplace(std::string(system), ShardState{.system = std::string(system), .core = sched::Shard{}})
                .first;
    }
    return &iter->second;
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

void Dispatcher::dispatchLocked(ShardState & shs)
{
    std::vector<sched::Assign> out;
    shs.core.dispatch(out);
    for (auto & asg : out) {
        const auto & ent = shs.core.entry(asg.drv);
        if (!ent) {
            continue;
        }
        const auto & wkr = shs.core.worker(asg.worker);
        SchedCmd cmd;
        cmd.mutable_expect()->set_drv_path(ent->drvPath);
        cmd.mutable_expect()->set_assign_id(asg.assignId);
        auto wit = shs.workers.find(asg.worker);
        if (wit == shs.workers.end() || !wit->second(cmd)) {
            // Worker stream died under us; requeue and let the next event retry.
            shs.core.workerGone(asg.worker);
            shs.workers.erase(asg.worker);
            metrics.event("expect_undeliverable");
            continue;
        }
        SchedMsg msg;
        msg.mutable_assigned()->set_drv_path(ent->drvPath);
        msg.mutable_assigned()->set_worker_addr(wkr.addr);
        msg.mutable_assigned()->set_assign_id(asg.assignId);
        for (auto cid : asg.clients) {
            if (auto cit = shs.clients.find(cid); cit != shs.clients.end()) {
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
    metrics.schedQueued(shs.core.queued());
}

void Dispatcher::revokeOn(ShardState & shs, sched::WorkerId wid, const std::string & drvPath)
{
    if (auto wit = shs.workers.find(wid); wit != shs.workers.end()) {
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
    const Lock lock(*this);
    if (stopping) {
        SchedMsg msg;
        msg.mutable_restarting();
        client->send(msg);
    }
    return client;
}

void Dispatcher::resume()
{
    const Lock lock(*this);
    stopping = false;
}

void Dispatcher::restarting()
{
    const Lock lock(*this);
    stopping = true;
    SchedMsg msg;
    msg.mutable_restarting();
    SchedCmd cmd;
    cmd.mutable_restarting();
    std::unordered_set<sched::ClientId> told;
    for (auto & [sys, shs] : shards) {
        for (auto & [cid, send] : shs.clients) {
            if (told.insert(cid).second) {
                send(msg);
            }
        }
        for (auto & [wid, send] : shs.workers) {
            send(cmd);
        }
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
    auto * shs = shardFor(want.system());
    if (shs == nullptr) {
        reply.mutable_unplaceable()->set_drv_path(want.drv_path());
        reply.mutable_unplaceable()->set_reason("bad system '" + want.system() + "'");
        client.send(reply);
        return;
    }
    if (std::ranges::find(client.touched, shs) == client.touched.end()) {
        client.touched.push_back(shs);
        shs->clients[client.id] = client.send;
    }
    auto res = shs->core.want(
        client.id,
        {.drvPath = want.drv_path(),
         .inputs = {want.inputs().begin(), want.inputs().end()}, // string_views into `want`
         .features = {want.required_features().begin(), want.required_features().end()},
         .cpHintMs = static_cast<double>(want.cp_hint_ms())},
        nowMs());
    if (res.assigned) {
        reply.mutable_assigned()->set_drv_path(want.drv_path());
        reply.mutable_assigned()->set_worker_addr(shs->core.worker(res.assigned->first).addr);
        reply.mutable_assigned()->set_assign_id(res.assigned->second);
        client.send(reply);
        return;
    }
    dispatchLocked(*shs);
    if (!shs->core.placeable(res.drv)) {
        // Say so instead of queueing forever.
        shs->core.cancel(client.id, res.drv);
        reply.mutable_unplaceable()->set_drv_path(want.drv_path());
        reply.mutable_unplaceable()->set_reason(
            "no connected, non-draining worker for system '" + shs->system + "' offers features {"
            + nix::concatStringsSep(",", nix::Strings(want.required_features().begin(), want.required_features().end()))
            + "}");
        client.send(reply);
    }
}

// Under the lock.
void Dispatcher::onCancel(Client & client, const nix::remote::Cancel & cancel)
{
    for (auto * shs : client.touched) {
        if (auto wid = shs->core.cancel(client.id, cancel.drv_path())) {
            revokeOn(*shs, *wid, cancel.drv_path());
        }
        metrics.schedQueued(shs->core.queued());
    }
}

void Dispatcher::clientGone(const ClientPtr & clientPtr)
{
    auto & client = *clientPtr;
    const Lock lock(*this);
    client.gone = true;
    for (auto * shs : client.touched) {
        shs->clients.erase(client.id);
        std::vector<std::pair<sched::DrvId, sched::WorkerId>> revokes;
        shs->core.clientGone(client.id, revokes);
        for (auto [drv, wid] : revokes) {
            if (const auto & ent = shs->core.entry(drv)) {
                revokeOn(*shs, wid, ent->drvPath);
            }
        }
        metrics.schedQueued(shs->core.queued());
    }
    client.touched.clear();
    metrics.schedClients(-1);
}

// ------------------------------------------------------------------ workers

void Dispatcher::workerMsgs(Worker & worker, const nix::remote::WorkerMsgs & msgs)
{
    if (msgs.msgs().empty()) {
        return;
    }
    const Lock lock(*this);
    if (worker.shard == nullptr && msgs.msgs(0).has_hello()) {
        worker.shard = shardFor(msgs.msgs(0).hello().system());
        if (worker.shard == nullptr) {
            throw nix::Error("WorkerSession: bad system '%s'", msgs.msgs(0).hello().system());
        }
        if (stopping) {
            SchedCmd cmd;
            cmd.mutable_restarting();
            worker.send(cmd);
        }
    }
    if (worker.shard == nullptr) {
        throw nix::Error("WorkerSession: first message must be Hello");
    }
    auto & shs = *worker.shard;
    for (const auto & msg : msgs.msgs()) {
        workerMsgLocked(worker, shs, msg);
    }
    dispatchLocked(shs);
    metrics.schedWorkers(shs.core.workersUp());
}

void Dispatcher::workerMsgLocked(Worker & worker, ShardState & shs, const nix::remote::WorkerMsg & msg)
{
    if (msg.has_hello()) {
        const auto & hel = msg.hello();
        std::vector<std::string_view> running;
        running.reserve(static_cast<size_t>(hel.running_size()) + static_cast<size_t>(hel.expecting_size()));
        for (const auto & run : hel.running()) {
            running.emplace_back(run.drv_path());
        }
        for (const auto & run : hel.expecting()) {
            running.emplace_back(run.drv_path());
        }
        if (shardFor(hel.system()) != &shs) {
            throw nix::Error("worker %s changed system from %s to %s", hel.addr(), shs.system, hel.system());
        }
        worker.id = shs.core.hello(
            {.addr = hel.addr(),
             .features = {hel.features().begin(), hel.features().end()},
             .maxJobs = hel.max_jobs(),
             .running = std::move(running)});
        shs.workers[*worker.id] = worker.send;
        logLine(
            LogLevel::info,
            {{"event", "worker_hello"},
             {"addr", hel.addr()},
             {"system", shs.system},
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
        shs.core.done(*worker.id, done.drv_path(), outputs);
        metrics.event("done_" + nix::remote::Done::Outcome_Name(done.outcome()));
    } else if (msg.has_load()) {
        shs.core.setDraining(*worker.id, msg.load().draining());
    }
}

void Dispatcher::workerGone(Worker & worker)
{
    const Lock lock(*this);
    if (worker.shard == nullptr || !worker.id) {
        return;
    }
    auto & shs = *worker.shard;
    logLine(
        LogLevel::info, {{"event", "worker_gone"}, {"addr", shs.core.worker(*worker.id).addr}, {"system", shs.system}});
    shs.core.workerGone(*worker.id);
    shs.workers.erase(*worker.id);
    dispatchLocked(shs);
    metrics.schedWorkers(shs.core.workersUp());
    worker.shard = nullptr;
    worker.id.reset();
}

} // namespace nixgrpc
