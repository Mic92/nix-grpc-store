// The Dispatcher driven through the calls its transports make, without gRPC.

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

namespace {

using nixgrpc::Dispatcher;

struct Farm
{
    nixgrpc::Metrics metrics{""};
    double nowMs = 0;
    Dispatcher disp{
        Dispatcher::Config{
            .defaultSystem = "x", .present = {}, .lookupThreads = 0, .nowMs = [this] -> double { return nowMs; }},
        metrics};
    std::vector<std::string> sentTo;
    bool unplaceable = false;
    Dispatcher::ClientPtr client = disp.connectClient(
        [this](const nix::remote::SchedMsg & msg) -> bool {
            if (msg.has_assigned()) {
                sentTo.push_back(msg.assigned().worker_addr());
            }
            unplaceable = unplaceable || msg.has_unplaceable();
            return true;
        },
        {});

    void hello(Dispatcher::Worker & session, const std::string & addr, uint32_t slots = 2)
    {
        nix::remote::WorkerMsgs msgs;
        auto * hel = msgs.add_msgs()->mutable_hello();
        hel->set_addr(addr);
        hel->add_systems("x");
        hel->set_max_jobs(slots);
        disp.workerMsgs(session, msgs);
    }

    void want(const std::string & drv, uint64_t refused = 0)
    {
        nix::remote::ClientMsgs msgs;
        auto * wnt = msgs.add_msgs()->mutable_want();
        wnt->set_drv_path(drv);
        wnt->set_system("x");
        wnt->set_refused(refused);
        disp.clientMsgs(client, msgs);
    }
};

// What a worker session was told to expect.
struct Session
{
    bool alive = true;
    bool ended = false;
    std::vector<std::string> expected;
    std::vector<std::string> revoked;
    uint64_t lastAssignId = 0;
    Dispatcher::Worker conn{
        .send = [this](const nix::remote::SchedCmd & cmd) -> bool {
            if (alive && cmd.has_revoke()) {
                revoked.push_back(cmd.revoke().drv_path());
            }
            if (alive && cmd.has_expect()) {
                expected.push_back(cmd.expect().drv_path());
                lastAssignId = cmd.expect().assign_id();
            }
            return alive;
        },
        .abort = [this] -> void { ended = true; },
        .id = {}};
};

// A session an Expect cannot be sent on is dropped and ended. Its transport
// reports the end later, after the worker has reconnected: that must not
// take the new session down.
void endOfADroppedSessionKeepsTheNewOne()
{
    Farm farm;
    Session dead;
    farm.hello(dead.conn, "w:1");
    dead.alive = false;
    farm.want("a.drv");

    Session fresh;
    farm.hello(fresh.conn, "w:1");
    assert(dead.ended);
    assert(fresh.expected == std::vector<std::string>{"a.drv"});

    farm.disp.workerGone(dead.conn);
    farm.want("b.drv");
    assert((fresh.expected == std::vector<std::string>{"a.drv", "b.drv"}));
    farm.disp.workerGone(fresh.conn);
}

// The derivation a dropped session was meant to get goes to another worker
// right away, not with the next event.
void dropPlacesItsWorkElsewhere()
{
    Farm farm;
    Session dead;
    Session other;
    constexpr uint32_t more = 4; // so the dead one is picked first
    farm.hello(dead.conn, "w:1", more);
    farm.hello(other.conn, "w:2");
    dead.alive = false;
    farm.want("a.drv");
    assert(other.expected == std::vector<std::string>{"a.drv"});
    farm.disp.workerGone(dead.conn);
    farm.disp.workerGone(other.conn);
}

// A worker in the scheduler's own process answers from inside the delivery
// of a command, here by giving an Expect back as a draining builder does.
void loopbackWorkerMayAnswerACommand()
{
    Farm farm;
    std::vector<std::string> expected;
    std::shared_ptr<Dispatcher::Worker> worker;
    worker = farm.disp.loopbackWorker([&](const nix::remote::SchedCmd & cmd) -> void {
        if (!cmd.has_expect()) {
            return;
        }
        expected.push_back(cmd.expect().drv_path());
        if (expected.size() == 1) {
            nix::remote::WorkerMsgs msgs;
            auto * done = msgs.add_msgs()->mutable_done();
            done->set_drv_path(cmd.expect().drv_path());
            done->set_assign_id(cmd.expect().assign_id());
            done->set_outcome(nix::remote::Done::GIVEN_BACK);
            farm.disp.workerMsgs(*worker, msgs);
        }
    });
    farm.hello(*worker, "w:1");
    farm.want("a.drv");
    // Given back once, then placed again on the only worker there is.
    assert((expected == std::vector<std::string>{"a.drv", "a.drv"}));
    farm.disp.workerGone(*worker);
}

// A worker that stops taking builds gives back what it was told to expect.
// That says nothing about the derivation, which may return there.
void aWorkerThatRestedGetsItsDerivationBack()
{
    Farm farm;
    Session rested;
    Session busy;
    farm.hello(rested.conn, "w:1", 1);
    farm.want("a.drv");
    farm.hello(busy.conn, "w:2", 1);
    farm.want("b.drv");
    assert(busy.expected == std::vector<std::string>{"b.drv"});

    nix::remote::WorkerMsgs msgs;
    msgs.add_msgs()->mutable_load()->set_draining(true);
    auto * done = msgs.add_msgs()->mutable_done();
    done->set_drv_path("a.drv");
    done->set_assign_id(rested.lastAssignId);
    done->set_outcome(nix::remote::Done::GIVEN_BACK);
    done->set_reason(nix::remote::Done::DRAINING);
    msgs.add_msgs()->mutable_load()->set_draining(false);
    farm.disp.workerMsgs(rested.conn, msgs);

    assert((rested.expected == std::vector<std::string>{"a.drv", "a.drv"}));
    farm.disp.workerGone(rested.conn);
    farm.disp.workerGone(busy.conn);
}

// A worker that gives back build after build for a failure of its own gets
// none for a while. Its client waits and is not told the build has no worker.
void aFailingWorkerIsRested()
{
    constexpr double rest = 30e3;
    Farm farm;
    Session sick;
    farm.hello(sick.conn, "w:1", 1);
    farm.want("a.drv");
    for (std::size_t round = 1; round <= 3; round++) {
        assert(sick.expected.size() == round);
        nix::remote::WorkerMsgs msgs;
        auto * done = msgs.add_msgs()->mutable_done();
        done->set_drv_path("a.drv");
        done->set_assign_id(sick.lastAssignId);
        done->set_outcome(nix::remote::Done::GIVEN_BACK);
        done->set_reason(nix::remote::Done::TRANSIENT_FAILURE);
        farm.disp.workerMsgs(sick.conn, msgs);
    }
    assert(sick.expected.size() == 3 && !farm.unplaceable);
    farm.nowMs = rest;
    farm.disp.tick();
    assert(sick.expected.size() == 4);
    farm.disp.workerGone(sick.conn);
}

// A client that cannot reach the worker it was sent to is sent to another.
void aClientThatCannotReachItsWorkerIsSentElsewhere()
{
    Farm farm;
    Session unreachable;
    Session other;
    constexpr uint32_t more = 4; // so it is picked first
    farm.hello(unreachable.conn, "w:1", more);
    farm.hello(other.conn, "w:2");
    farm.want("a.drv");
    assert(farm.sentTo == std::vector<std::string>{"w:1"});

    farm.want("a.drv", unreachable.lastAssignId);
    assert((farm.sentTo == std::vector<std::string>{"w:1", "w:2"}));
    assert(unreachable.revoked == std::vector<std::string>{"a.drv"});
    assert(other.expected == std::vector<std::string>{"a.drv"});
    farm.disp.workerGone(unreachable.conn);
    farm.disp.workerGone(other.conn);
}

} // namespace

auto main() -> int
try {
    aClientThatCannotReachItsWorkerIsSentElsewhere();
    aWorkerThatRestedGetsItsDerivationBack();
    aFailingWorkerIsRested();
    loopbackWorkerMayAnswerACommand();
    endOfADroppedSessionKeepsTheNewOne();
    dropPlacesItsWorkElsewhere();
    return 0;
} catch (std::exception &) {
    return 1;
}
