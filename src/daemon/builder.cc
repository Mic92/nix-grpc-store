#include "builder.hh"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "logfmt.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

using nix::remote::WorkerMsg;

Builder::Builder(Config config, Metrics & metrics)
    : config(std::move(config))
    , metrics(metrics)
{
}

void Builder::onExpect(const nix::remote::Expect & exp)
{
    if (exp.drv_path().empty()) {
        return;
    }
    if (draining_) {
        giveBack({{exp.drv_path(), exp.assign_id()}}, nix::remote::Done::DRAINING);
        return;
    }
    auto lck = state.lock();
    auto & ent = lck->expected[exp.drv_path()];
    if (ent.claimed) {
        return; // re-announced across a scheduler restart, keep the live one
    }
    ent.assignId = exp.assign_id();
    ent.since = config.now();
    metrics.event("expect");
}

void Builder::claim(const std::string & drvPath)
{
    auto lck = state.lock();
    if (auto const found = lck->expected.find(drvPath); found != lck->expected.end()) {
        found->second.claimed = true;
    }
}

auto Builder::holding() const -> size_t
{
    return state.lock()->expected.size();
}

void Builder::giveBack(const std::vector<std::pair<std::string, uint64_t>> & drvs, nix::remote::Done::Reason reason)
{
    for (const auto & [drv, assignId] : drvs) {
        metrics.event("give_back");
        sendDone(drv, assignId, nix::remote::Done::GIVEN_BACK, {}, reason);
    }
}

void Builder::onRevoke(const nix::remote::Revoke & rev)
{
    uint64_t assignId = 0;
    {
        auto lck = state.lock();
        auto const found = lck->expected.find(rev.drv_path());
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
    auto const found = lck->expected.find(drvPath);
    if (found == lck->expected.end()) {
        return std::nullopt;
    }
    auto & ent = found->second;
    Admission adm{.shared = ent.shared, .assignId = ent.assignId, .attach = ent.running};
    ent.claimed = true;
    ent.running = true;
    return adm;
}

void Builder::finished(
    const std::string & drvPath,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs,
    std::string resultWire,
    nix::remote::Done::Reason reason)
{
    uint64_t assignId = 0;
    std::shared_ptr<Expected::Shared> shared;
    {
        auto lck = state.lock();
        auto const found = lck->expected.find(drvPath);
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
    sendDone(drvPath, assignId, outcome, outputs, reason);
}

void Builder::sendDone(
    const std::string & drvPath,
    uint64_t assignId,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs,
    nix::remote::Done::Reason reason)
{
    WorkerMsg msg;
    auto * done = msg.mutable_done();
    done->set_drv_path(drvPath);
    done->set_assign_id(assignId);
    done->set_outcome(outcome);
    done->set_reason(reason);
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

auto Builder::hello() -> WorkerMsg
{
    WorkerMsg msg;
    auto * hel = msg.mutable_hello();
    hel->set_addr(config.advertise);
    for (const auto & system : config.systems) {
        hel->add_systems(system);
    }
    for (const auto & feat : config.features) {
        hel->add_features(feat);
    }
    hel->set_max_jobs(config.maxJobs);
    auto lck = state.lock();
    // A slot nobody claimed is dropped, not reported: the scheduler would
    // take it for a build and revoke the one its client started elsewhere.
    std::erase_if(lck->expected, [](const auto & item) -> bool { return !item.second.claimed; });
    for (const auto & [drv, ent] : lck->expected) {
        auto * run = hel->add_running();
        run->set_drv_path(drv);
        run->set_assign_id(ent.assignId);
    }
    return msg;
}

auto Builder::greet() -> bool
{
    if (!send(hello())) {
        return false;
    }
    if (draining_) {
        WorkerMsg msg;
        msg.mutable_load()->set_draining(true);
        return send(msg);
    }
    return true;
}

void Builder::setDraining(bool draining)
{
    if (draining_.exchange(draining) == draining) {
        return;
    }
    WorkerMsg msg;
    msg.mutable_load()->set_draining(draining);
    send(msg);
    if (draining) {
        giveBack(takeUnclaimed([](const Expected &) -> bool { return true; }), nix::remote::Done::DRAINING);
    }
}

auto Builder::takeUnclaimed(const std::function<bool(const Expected &)> & pred)
    -> std::vector<std::pair<std::string, uint64_t>>
{
    std::vector<std::pair<std::string, uint64_t>> taken;
    auto lck = state.lock();
    for (auto iter = lck->expected.begin(); iter != lck->expected.end();) {
        if (!iter->second.claimed && pred(iter->second)) {
            taken.emplace_back(iter->first, iter->second.assignId);
            iter = lck->expected.erase(iter);
        } else {
            ++iter;
        }
    }
    return taken;
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
    const auto now = config.now();
    // While draining, also what an Expect racing setDraining() left behind.
    bool const draining = draining_;
    giveBack(
        takeUnclaimed([&](const Expected & ent) -> bool { return draining || now - ent.since > expectTimeout; }),
        draining ? nix::remote::Done::DRAINING : nix::remote::Done::UNREACHABLE);
}

} // namespace nixgrpc
