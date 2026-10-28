#include "dispatcher.hh"
#include "reverse-protocol.hh"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <initializer_list>
#include <optional>
#include <ratio>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
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
constexpr double statsEveryMs = 1000;
constexpr double msPerSecond = 1000;
// A client whose stream broke reconnects within its backoff and Wants its
// assigned drvs again. Until then their builds keep running.
constexpr double orphanGraceMs = 10'000;
} // namespace

Dispatcher::Dispatcher(Config config_, Metrics & metrics)
    : config(std::move(config_))
    , metrics(metrics)
    , assignedCtr(metrics.eventCounter("assigned"))
{
    if (config.present) {
        for (unsigned i = 0; i < config.lookupThreads; i++) {
            lookupThreads.emplace_back([this] -> void { lookupLoop(); });
        }
    }
    statsThread = std::thread([this] -> void { statsLoop(); });
}

Dispatcher::~Dispatcher()
{
    {
        const absl::MutexLock lock(lookupMutex);
        lookupStop = true;
    }
    for (auto & thr : lookupThreads) {
        thr.join();
    }
    {
        const absl::MutexLock lock(mutex);
        statsStop = true;
    }
    statsThread.join();
}

void Dispatcher::statsLoop()
{
    for (;;) {
        // A Lock per round, so what it defers runs.
        const Lock lock(*this);
        mutex.AwaitWithTimeout(absl::Condition(&statsStop), absl::Milliseconds(statsEveryMs));
        if (statsStop) {
            return;
        }
        revokeOrphans();
        if (statsPending) {
            exportStats(true);
        }
    }
}

void Dispatcher::lookupLoop()
{
    for (;;) {
        LookupJob job;
        {
            const absl::MutexLock lock(lookupMutex);
            auto const ready = [this] ABSL_EXCLUSIVE_LOCKS_REQUIRED(lookupMutex) -> bool {
                return lookupStop || !lookupQueue.empty();
            };
            lookupMutex.Await(absl::Condition(&ready));
            if (lookupQueue.empty()) {
                return;
            }
            job = std::move(lookupQueue.front());
            lookupQueue.pop_front();
        }
        // Inline, the reactor closes the stream on a throw. Here only this client fails.
        try {
            applyClientMsgs(*job.client, job.msgs, lookup(job.msgs));
        } catch (std::exception & err) {
            logLine(LogLevel::info, {{"event", "client_failed"}, {"client", std::to_string(job.client->id)}, {"error", err.what()}});
                    const Lock lock(*this);
            if (!job.client->gone) {
                job.client->abort();
            }
        }
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
    for (auto const & func : run) {
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

auto Dispatcher::membership() const -> std::shared_ptr<const Membership>
{
    const absl::MutexLock lock(publishedMutex);
    return published;
}

auto Dispatcher::watchMembership(std::function<void()> wake) -> uint64_t
{
    const Lock lock(*this);
    auto const watcher = nextWatcher++;
    watchers.emplace(watcher, std::move(wake));
    return watcher;
}

void Dispatcher::unwatchMembership(uint64_t watcher)
{
    const Lock lock(*this);
    watchers.erase(watcher);
}

void Dispatcher::membershipChangedLocked()
{
    auto next = std::make_shared<Membership>();
    for (size_t wid = 0; wid < core.workerCount(); wid++) {
        const auto & wkr = core.worker(static_cast<sched::WorkerId>(wid));
        if (!wkr.up) {
            continue;
        }
        // The balancer cannot dial a reverse worker; it is reached only
        // through its data session. Never publish it as an EDS endpoint.
        if (auto const it = workers.find(static_cast<sched::WorkerId>(wid));
            it != workers.end() && !it->second->verifiedPodId.empty()) {
            continue;
        }
        for (const auto & system : wkr.systems) {
            (*next)[system].push_back({.addr = wkr.addr, .draining = wkr.draining || wkr.maxJobs == 0});
        }
    }
    {
        const absl::MutexLock lock(publishedMutex);
        published = std::move(next);
    }
    wakeWatchersLocked();
}

void Dispatcher::wakeWatchersLocked()
{
    for (const auto & [watcher, wake] : watchers) {
        afterUnlock(wake);
    }
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
    for (auto const & asg : out) {
        const auto & ent = core.entry(asg.drv);
        if (!ent) {
            continue;
        }
        const auto & wkr = core.worker(asg.worker);
        SchedCmd cmd;
        cmd.mutable_expect()->set_drv_path(ent->drvPath);
        cmd.mutable_expect()->set_assign_id(asg.assignId);
        auto const wit = workers.find(asg.worker);
        if (wit == workers.end() || !wit->second->send(cmd)) {
            // Worker stream died under us; requeue and let the next event retry.
            core.workerGone(asg.worker);
            membershipChangedLocked();
            workers.erase(asg.worker);
            metrics.event("expect_undeliverable");
            placeabilityChangedLocked();
            continue;
        }
        sendToFollowers(asg.drv, assignedMsg(asg.drv));
        assignedCtr.Increment();
        metrics.queueWait(ent->system, (nowMs() - ent->enqMs) / msPerSecond);
        debugLog(
            {{"event", "assigned"},
             {"drv", ent->drvPath},
             {"worker", wkr.addr},
             {"clients", std::to_string(asg.clients.size())}});
    }
    exportStats(false);
}

void Dispatcher::exportStats(bool force)
{
    auto now = nowMs();
    if (!force && statsAtMs && now - *statsAtMs < statsEveryMs) {
        statsPending = true;
        return;
    }
    statsPending = false;
    statsAtMs = now;
    auto previous = std::exchange(statGauges, {});
    size_t queued = 0;
    for (const auto & [key, sst] : core.stats()) {
        queued += sst.queued;
        auto & live = statGauges[key];
        live = {
            metrics.schedSystem(key.system, key.features, "queued", sst.queued),
            metrics.schedSystem(key.system, key.features, "unplaceable", sst.unplaceable),
            metrics.schedSystem(key.system, key.features, "running", sst.running),
            metrics.schedSystem(key.system, key.features, "slots", sst.slots),
            metrics.schedSystem(key.system, key.features, "free", sst.free),
        };
        previous.erase(key);
    }
    metrics.schedQueued(queued);
    // A feature set nobody asks for or offers any more: drop the series
    // instead of leaving a stale value.
    for (auto & [key, gauges] : previous) {
        for (auto * gauge : gauges) {
            metrics.schedSystemRemove(gauge);
        }
    }
}

// The entry moved to a reconnecting worker that was already building it.
// Stop the copy we started and send its clients after the original.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void Dispatcher::supersede(sched::DrvId drv, sched::WorkerId loser, uint64_t loserAssignId)
{
    const auto & ent = core.entry(drv);
    if (!ent) {
        return;
    }
    sendToFollowers(drv, assignedMsg(drv));
    if (loser == sched::noWorker) {
        return; // was queued: only the followers need telling
    }
    revokeOn(loser, ent->drvPath, loserAssignId);
    metrics.event("superseded");
    logLine(
        LogLevel::info,
        {{"event", "superseded"},
         {"drv", ent->drvPath},
         {"keep", core.worker(ent->worker).addr},
         {"revoke", core.worker(loser).addr}});
}

void Dispatcher::revokeOn(sched::WorkerId wid, const std::string & drvPath, uint64_t assignId)
{
    if (auto const wit = workers.find(wid); wit != workers.end()) {
        SchedCmd cmd;
        cmd.mutable_revoke()->set_drv_path(drvPath);
        cmd.mutable_revoke()->set_assign_id(assignId);
        wit->second->send(cmd);
    }
}

void Dispatcher::revokeOrphans()
{
    auto const now = nowMs();
    std::erase_if(orphans, [&](const Orphan & orphan) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) -> bool {
        if (orphan.dueMs > now) {
            return false;
        }
        const auto & ent = core.entry(orphan.drv);
        if (ent && ent->assignId == orphan.assignId && ent->worker != sched::noWorker && ent->followers.empty()) {
            revokeOn(ent->worker, ent->drvPath, ent->assignId);
        }
        return true;
    });
}

auto Dispatcher::assignedMsg(sched::DrvId drv) const -> SchedMsg
{
    SchedMsg msg;
    const auto & ent = core.entry(drv);
    if (!ent || ent->worker == sched::noWorker) {
        return msg;
    }
    const auto & wkr = core.worker(ent->worker);
    auto * asg = msg.mutable_assigned();
    asg->set_drv_path(ent->drvPath);
    asg->set_worker_addr(wkr.addr);
    asg->set_assign_id(ent->assignId);
    // The balancer routes on it: the drv's own system may be one only some of the worker's peers offer.
    asg->set_worker_system(wkr.systems.empty() ? std::string() : wkr.systems.front());
    // Reached through its data session at the scheduler, not by x-nix-worker.
    if (auto const owner = workers.find(ent->worker); owner != workers.end()) {
        asg->set_reverse(!owner->second->verifiedPodId.empty());
    }
    return msg;
}

auto Dispatcher::unplaceableMsg(const sched::Entry & ent) const -> SchedMsg
{
    auto const join = [](const auto & feats) -> std::string {
        return nix::concatStringsSep(",", nix::Strings(feats.begin(), feats.end()));
    };
    SchedMsg msg;
    msg.mutable_unplaceable()->set_drv_path(ent.drvPath);
    msg.mutable_unplaceable()->set_reason(
        "no connected, non-draining worker for system '" + ent.system + "' offers features {" + join(ent.features)
        + "}, the workers offer {" + join(core.offeredFeatures(ent.system)) + "}");
    return msg;
}

void Dispatcher::sendToFollowers(sched::DrvId drv, const SchedMsg & msg)
{
    const auto & ent = core.entry(drv);
    if (!ent) {
        return;
    }
    for (auto const cid : ent->followers) {
        if (auto const cit = clients.find(cid); cit != clients.end()) {
            cit->second(msg);
        }
    }
}

// After an event that changes which workers are usable: tell the followers
// of queued entries whose answer flipped.
void Dispatcher::placeabilityChangedLocked()
{
    std::vector<sched::DrvId> changed;
    core.placeabilityChanges(changed);
    for (auto const drv : changed) {
        const auto & ent = core.entry(drv);
        if (!ent) {
            continue;
        }
        SchedMsg msg;
        if (ent->unplaceable) {
            msg = unplaceableMsg(*ent);
        } else {
            msg.mutable_placeable()->set_drv_path(ent->drvPath);
        }
        sendToFollowers(drv, msg);
    }
}

// ------------------------------------------------------------------ clients

auto Dispatcher::connectClient(ClientSend send, std::function<void()> abort) -> ClientPtr
{
    metrics.schedClients(1);
    auto client = std::make_shared<Client>();
    client->id = nextClient++;
    client->send = std::move(send);
    client->abort = std::move(abort);
    // Known from the start, so restarting() reaches clients that only got Cached so far.
    const Lock lock(*this);
    clients.emplace(client->id, client->send);
    return client;
}

void Dispatcher::serving()
{
    const Lock lock(*this);
    lettingGo = false;
    metrics.schedLeader(true);
}

void Dispatcher::restarting()
{
    const Lock lock(*this);
    lettingGo = true;
    metrics.schedLeader(false);
    wakeWatchersLocked();
    SchedMsg msg;
    msg.mutable_restarting();
    SchedCmd cmd;
    cmd.mutable_restarting();
    for (auto & [cid, send] : clients) {
        send(msg);
    }
    for (auto & [wid, wkr] : workers) {
        wkr->send(cmd);
    }
}

void Dispatcher::clientMsgs(const ClientPtr & client, const nix::remote::ClientMsgs & msgs)
{
    const bool needs =
        config.present && std::ranges::any_of(msgs.msgs(), [](const nix::remote::ClientMsg & msg) -> bool {
            return msg.has_want() && wantsLookup(msg.want());
        });
    if (needs && !lookupThreads.empty()) {
        const absl::MutexLock lock(lookupMutex);
        lookupQueue.push_back({.client = client, .msgs = msgs});
        return;
    }
    applyClientMsgs(*client, msgs, lookup(msgs));
}

void Dispatcher::applyClientMsgs(
    Client & client, const nix::remote::ClientMsgs & msgs, const std::vector<bool> & cached)
{
    // The lock is released between chunks so a large batch does not stall everyone else.
    static constexpr int chunk = 256;
    for (int start = 0; start < msgs.msgs_size(); start += chunk) {
        const Lock lock(*this);
        if (client.gone) {
            return;
        }
        for (int i = start; i < std::min(start + chunk, msgs.msgs_size()); i++) {
            const auto & msg = msgs.msgs(i);
            if (msg.has_want()) {
                onWant(client, msg.want(), cached.at(static_cast<size_t>(i)));
            } else if (msg.has_cancel()) {
                onCancel(client, msg.cancel());
            }
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
    if (lettingGo) {
        // Yielded while this batch waited for its lookup: go to the new leader.
        reply.mutable_restarting();
        client.send(reply);
        return;
    }
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
    auto const system = systemFor(want.system());
    if (!core.admitsSystem(std::string(system))) {
        reply.mutable_unplaceable()->set_drv_path(want.drv_path());
        reply.mutable_unplaceable()->set_reason("too many systems no worker offers, not keeping '" + std::string(system) + "'");
        client.send(reply);
        return;
    }
    auto res = core.want(
        client.id,
        {.drvPath = want.drv_path(),
         .inputs = {want.inputs().begin(), want.inputs().end()}, // string_views into `want`
         .system = system,
         .features = {want.required_features().begin(), want.required_features().end()},
         .cpHintMs = static_cast<double>(want.cp_hint_ms())},
        nowMs());
    if (res.assigned) {
        client.send(assignedMsg(res.drv));
        return;
    }
    dispatchLocked();
    const auto & ent = core.entry(res.drv);
    if (ent && ent->worker == sched::noWorker && !core.placeable(res.drv)) {
        // Keep the Want: right after a (re)start no worker has said Hello yet.
        // placeabilityChangedLocked() tells the followers once one has.
        core.markUnplaceable(res.drv, true);
        client.send(unplaceableMsg(*ent));
        exportStats(false);
    }
}

// Under the lock.
void Dispatcher::onCancel(Client & client, const nix::remote::Cancel & cancel)
{
    if (auto wid = core.cancel(client.id, cancel.drv_path())) {
        if (auto drv = core.findDrv(cancel.drv_path()); drv && core.entry(*drv)) {
            revokeOn(*wid, cancel.drv_path(), core.entry(*drv)->assignId);
        }
    }
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
        if (const auto & ent = core.entry(drv)) {
            orphans.push_back({.drv = drv, .assignId = ent->assignId, .dueMs = nowMs() + orphanGraceMs});
        }
    }
    metrics.schedClients(-1);
    exportStats(false);
}

// ------------------------------------------------------------------ workers

auto Dispatcher::ownsAssignment(std::string_view workerAddr, std::string_view drvPath, uint64_t assignId) const -> bool
{
    auto selected = assignmentWorker(drvPath, assignId);
    return selected && *selected == workerAddr;
}

auto Dispatcher::assignmentWorker(std::string_view drvPath, uint64_t assignId) const -> std::optional<std::string>
{
    if (drvPath.empty() || assignId == 0) {
        return std::nullopt;
    }
    const absl::MutexLock lock(mutex);
    auto const drv = core.findDrv(drvPath);
    if (!drv) {
        return std::nullopt;
    }
    const auto & slot = core.entry(*drv);
    if (!slot || slot->followers.empty() || slot->worker == sched::noWorker || slot->assignId != assignId
        || slot->drvPath != drvPath) {
        return std::nullopt;
    }
    const auto & worker = core.worker(slot->worker);
    if (!worker.up || !workers.contains(slot->worker)) {
        return std::nullopt;
    }
    return worker.addr;
}

auto Dispatcher::assignmentPod(std::string_view drvPath, uint64_t assignId) const -> std::optional<std::string>
{
    if (drvPath.empty() || assignId == 0) {
        return std::nullopt;
    }
    const absl::MutexLock lock(mutex);
    auto const it = core.findDrv(drvPath);
    if (!it) {
        return std::nullopt;
    }
    auto const & slot = core.entry(*it);
    if (!slot || slot->followers.empty() || slot->worker == sched::noWorker || slot->assignId != assignId
        || slot->drvPath != drvPath || !core.worker(slot->worker).up) {
        return std::nullopt;
    }
    auto const connected = workers.find(slot->worker);
    if (connected == workers.end() || connected->second->verifiedPodId.empty()
        || connected->second->credentialExpiresAt <= std::chrono::system_clock::now()) {
        return std::nullopt;
    }
    return connected->second->verifiedPodId;
}

auto Dispatcher::podConnected(std::string_view podId) const -> bool
{
    if (podId.empty()) {
        return false;
    }
    const absl::MutexLock lock(mutex);
    auto const now = std::chrono::system_clock::now();
    for (const auto & [wid, worker] : workers) {
        if (worker->verifiedPodId == podId && worker->credentialExpiresAt > now && core.worker(wid).up) {
            return true;
        }
    }
    return false;
}

auto Dispatcher::podOwnsAssignment(std::string_view podId, std::string_view drvPath, uint64_t assignId) const -> bool
{
    auto selected = assignmentPod(drvPath, assignId);
    return selected && *selected == podId;
}

void Dispatcher::workerMsgs(Worker & worker, const nix::remote::WorkerMsgs & msgs)
{
    if (msgs.msgs().empty()) {
        return;
    }
    const Lock lock(*this);
    for (const auto & msg : msgs.msgs()) {
        if (worker.id) {
            const auto current = workers.find(*worker.id);
            if (current == workers.end() || current->second != &worker) {
                throw nix::Error("WorkerSession: replaced by a newer session");
            }
        } else if (!msg.has_hello()) {
            throw nix::Error("WorkerSession: first message must be Hello");
        }
        workerMsgLocked(worker, msg);
    }
    dispatchLocked();
    metrics.schedWorkers(core.workersUp());
    exportStats(false);
}

void Dispatcher::onHello(Worker & worker, const nix::remote::Hello & hel)
{
    if (worker.verifiedPodId != hel.reverse_worker_id()) {
        throw nix::Error("WorkerSession: reverse identity does not match verified pod credential");
    }
    if (!worker.verifiedPodId.empty() && hel.reverse_protocol() != reverseProtocolVersion) {
        throw ReverseProtocolMismatch("WorkerSession: %s", reverseProtocolMismatch(hel.reverse_protocol()));
    }
    if (!worker.id) {
        // A replacement control stream must not adopt another pod's
        // address, even if it knows that address and has a valid token.
        // Neither a cert-only stream nor a different verified pod may
        // replace a live owner. An unverified direct worker is protected
        // from takeover by a reverse-capable worker as well.
        if (auto const previous = core.findWorker(hel.addr())) {
            auto const current = workers.find(*previous);
            if (current != workers.end() && current->second->verifiedPodId != worker.verifiedPodId
                && (!current->second->verifiedPodId.empty() || !worker.verifiedPodId.empty())) {
                throw nix::Error("WorkerSession: advertised address belongs to another verified pod");
            }
        }
    }
    if (!worker.verifiedPodId.empty() && worker.credentialExpiresAt <= std::chrono::system_clock::now()) {
        throw nix::Error("WorkerSession: reverse control credential expired");
    }
    if (worker.id && core.worker(*worker.id).addr != hel.addr()) {
        throw nix::Error("WorkerSession: cannot change address on an open session");
    }
    std::vector<std::pair<std::string_view, uint64_t>> running;
    running.reserve(static_cast<size_t>(hel.running_size()));
    for (const auto & run : hel.running()) {
        running.emplace_back(run.drv_path(), run.assign_id());
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
    auto const wid = core.workerId(hel.addr());
    if (auto const old = workers.find(wid); old != workers.end() && old->second != &worker) {
        // A half-open older session must not take the new one down when it finally closes.
        old->second->id.reset();
        if (old->second->abort) {
            afterUnlock(old->second->abort);
        }
    }
    std::vector<sched::Core::Superseded> superseded;
    worker.id = core.hello(
        {.addr = hel.addr(),
         .systems = std::move(systems),
         .features = {hel.features().begin(), hel.features().end()},
         .maxJobs = hel.max_jobs(),
         .running = std::move(running)},
        superseded);
    workers[*worker.id] = &worker;
    membershipChangedLocked();
    for (auto const sup : superseded) {
        supersede(sup.drv, sup.loser, sup.loserAssignId);
    }
    placeabilityChangedLocked();
    logLine(
        LogLevel::info,
        {{"event", "worker_hello"},
         {"addr", hel.addr()},
         {"systems", nix::concatStringsSep(",", core.worker(*worker.id).systems)},
         {"max_jobs", std::to_string(hel.max_jobs())},
         {"running", std::to_string(hel.running_size())}});
}

void Dispatcher::onDone(sched::WorkerId wid, const nix::remote::Done & done)
{
    metrics.event("done_" + nix::remote::Done::Outcome_Name(done.outcome()));
    if (done.outcome() == nix::remote::Done::GIVEN_BACK || done.outcome() == nix::remote::Done::NO_SHOW) {
        core.giveBack(wid, done.drv_path());
        metrics.event("given_back_" + nix::remote::Done::Reason_Name(done.reason()));
        placeabilityChangedLocked(); // its followers may have no worker left
        return;
    }
    std::vector<std::pair<std::string_view, uint64_t>> outputs;
    if (done.outcome() == nix::remote::Done::BUILT) {
        for (const auto & out : done.outputs()) {
            outputs.emplace_back(out.path(), out.nar_size());
        }
    }
    if (!core.done(wid, done.drv_path(), done.assign_id(), outputs)) {
        metrics.event("done_stale");
    }
}

void Dispatcher::workerMsgLocked(Worker & worker, const nix::remote::WorkerMsg & msg)
{
    if (msg.has_hello()) {
        onHello(worker, msg.hello());
    } else if (!worker.id) {
        throw nix::Error("WorkerSession: first message must be Hello");
    } else if (msg.has_done()) {
        onDone(*worker.id, msg.done());
    } else if (msg.has_load()) {
        core.setDraining(*worker.id, msg.load().draining());
        membershipChangedLocked();
        placeabilityChangedLocked();
    } else if (msg.has_reauth()) {
        // Verified and applied by the session before this batch; a
        // cert-only session cannot carry one.
        if (worker.verifiedPodId.empty()) {
            throw nix::Error("WorkerSession: reauthentication on a session without a pod credential");
        }
    }
}

void Dispatcher::renewCredential(Worker & worker, std::chrono::system_clock::time_point expiresAt)
{
    const Lock lock(*this);
    // An expired session is already being torn down; it cannot be revived.
    if (worker.verifiedPodId.empty() || expiresAt <= worker.credentialExpiresAt
        || worker.credentialExpiresAt <= std::chrono::system_clock::now()) {
        return;
    }
    worker.credentialExpiresAt = expiresAt;
}

void Dispatcher::expireCredentials()
{
    const Lock lock(*this);
    auto const now = std::chrono::system_clock::now();
    bool changed = false;
    for (auto it = workers.begin(); it != workers.end();) {
        if (it->second->verifiedPodId.empty() || it->second->credentialExpiresAt > now) {
            ++it;
            continue;
        }
        logLine(LogLevel::info, {{"event", "worker_credential_expired"}, {"addr", core.worker(it->first).addr}});
        metrics.event("worker_credential_expired");
        core.workerGone(it->first);
        it->second->id.reset();
        if (it->second->abort) {
            afterUnlock(it->second->abort);
        }
        it = workers.erase(it);
        changed = true;
    }
    if (changed) {
        membershipChangedLocked();
        dispatchLocked();
        placeabilityChangedLocked();
        metrics.schedWorkers(core.workersUp());
        exportStats(false);
    }
}

void Dispatcher::workerGone(Worker & worker)
{
    const Lock lock(*this);
    if (!worker.id) {
        return;
    }
    const auto current = workers.find(*worker.id);
    if (current == workers.end() || current->second != &worker) {
        worker.id.reset();
        return;
    }
    logLine(LogLevel::info, {{"event", "worker_gone"}, {"addr", core.worker(*worker.id).addr}});
    core.workerGone(*worker.id);
    workers.erase(*worker.id);
    membershipChangedLocked();
    dispatchLocked();
    placeabilityChangedLocked();
    metrics.schedWorkers(core.workersUp());
    exportStats(false);
    worker.id.reset();
}

} // namespace nixgrpc
