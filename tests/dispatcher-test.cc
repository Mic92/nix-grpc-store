// Dispatcher::onDone and the membership against an in-process Dispatcher,
// without gRPC.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

using namespace nixgrpc;

namespace {

// What happens once w2 is busy with b.drv and w0 has a.drv.
enum class Step : uint8_t
{
    drain,            // w0: Load{draining}
    undrain,          // w0: Load{not draining}
    giveBack,         // w0: Done for a.drv, as `outcome` for `reason`
    giveBackDraining, // w0: Done for a.drv, GIVEN_BACK for DRAINING
    hello,            // w0: Hello again
    drainW2,          // w2: Load{draining}
    undrainW2,        // w2: Load{not draining}
    wantC,            // a client wants c.drv
};

struct After
{
    std::vector<std::string> expects; // the drvs w0 was told to expect
    bool draining = false;            // w0, as the membership says
};

// w2 is busy and w0 has a.drv; then w0 takes `steps`, giving a.drv back as
// `outcome`, for `reason`.
auto afterSteps(
    const std::vector<Step> & steps,
    nix::remote::Done::Reason reason,
    nix::remote::Done::Outcome outcome = nix::remote::Done::GIVEN_BACK) -> After
{
    Metrics metrics{""};
    Dispatcher disp({.defaultSystem = "x", .lookupThreads = 0}, metrics);
    std::map<std::string, std::vector<std::string>> expects;
    std::map<std::string, Dispatcher::Worker> conns;
    auto const send = [&](const std::string & addr, auto build) -> void {
        nix::remote::WorkerMsgs msgs;
        build(*msgs.add_msgs());
        disp.workerMsgs(conns[addr], msgs);
    };
    auto const hello = [&](const std::string & addr) -> void {
        // Init-captured: a plain [addr] is a const member, which the closure's move constructor copies.
        conns[addr].send = [&expects, addr = std::string(addr)](const nix::remote::SchedCmd & cmd) -> bool {
            expects[addr].push_back(cmd.expect().drv_path());
            return true;
        };
        send(addr, [&](nix::remote::WorkerMsg & msg) -> void {
            msg.mutable_hello()->set_addr(addr);
            msg.mutable_hello()->add_systems("x");
            msg.mutable_hello()->set_max_jobs(1);
        });
    };
    auto const client = disp.connectClient([](const nix::remote::SchedMsg &) -> bool { return true; }, [] -> void {});
    auto const want = [&](const std::string & drv) -> void {
        nix::remote::ClientMsgs msgs;
        msgs.add_msgs()->mutable_want()->set_drv_path(drv);
        disp.clientMsgs(client, msgs);
    };
    hello("w2");
    want("b.drv");
    hello("w0");
    want("a.drv");
    assert(expects["w2"] == std::vector<std::string>{"b.drv"} && expects["w0"] == std::vector<std::string>{"a.drv"});
    for (auto const step : steps) {
        switch (step) {
        case Step::drain:
        case Step::undrain:
            send("w0", [&](nix::remote::WorkerMsg & msg) -> void { msg.mutable_load()->set_draining(step == Step::drain); });
            break;
        case Step::giveBack:
            send("w0", [&](nix::remote::WorkerMsg & msg) -> void {
                msg.mutable_done()->set_drv_path("a.drv");
                msg.mutable_done()->set_outcome(outcome);
                msg.mutable_done()->set_reason(reason);
            });
            break;
        case Step::giveBackDraining:
            send("w0", [&](nix::remote::WorkerMsg & msg) -> void {
                msg.mutable_done()->set_drv_path("a.drv");
                msg.mutable_done()->set_outcome(nix::remote::Done::GIVEN_BACK);
                msg.mutable_done()->set_reason(nix::remote::Done::DRAINING);
            });
            break;
        case Step::hello:
            hello("w0");
            break;
        case Step::drainW2:
        case Step::undrainW2:
            send("w2", [&](nix::remote::WorkerMsg & msg) -> void { msg.mutable_load()->set_draining(step == Step::drainW2); });
            break;
        case Step::wantC:
            want("c.drv");
            break;
        }
    }
    auto const members = disp.membership()->at("x");
    auto const wzero = std::ranges::find(members, std::string("w0"), &Dispatcher::Member::addr);
    assert(wzero != members.end());
    return {.expects = expects["w0"], .draining = wzero->draining};
}

// w2 is busy; w0 gives a.drv back (`outcome`, for `reason`), drains first if
// the reason is DRAINING, and says Hello again. Returns the drvs w0 was told
// to expect.
auto expectsAfterGiveBack(nix::remote::Done::Reason reason, nix::remote::Done::Outcome outcome = nix::remote::Done::GIVEN_BACK) -> std::vector<std::string>
{
    std::vector<Step> steps{Step::giveBack, Step::hello};
    if (reason == nix::remote::Done::DRAINING) {
        steps.insert(steps.begin(), Step::drain);
    }
    return afterSteps(steps, reason, outcome).expects;
}

// Whether the membership reports a worker whose Hello says `jobs` as draining.
auto drainingInMembership(uint32_t jobs) -> bool
{
    Metrics metrics{""};
    Dispatcher disp({.defaultSystem = "x", .lookupThreads = 0}, metrics);
    Dispatcher::Worker conn;
    conn.send = [](const nix::remote::SchedCmd &) -> bool { return true; };
    nix::remote::WorkerMsgs msgs;
    auto & hel = *msgs.add_msgs()->mutable_hello();
    hel.set_addr("w0");
    hel.add_systems("x");
    hel.set_max_jobs(jobs);
    disp.workerMsgs(conn, msgs);
    auto const members = disp.membership()->at("x");
    assert(members.size() == 1);
    return members.front().draining;
}

} // namespace

auto main() -> int
try {
    // Back from draining, w0 is like any other: it gets a.drv again.
    assert((expectsAfterGiveBack(nix::remote::Done::DRAINING) == std::vector<std::string>{"a.drv", "a.drv"}));
    // Any other give-back: a.drv waits for w2.
    for (auto const reason : {nix::remote::Done::REASON_UNSPECIFIED, nix::remote::Done::PUBLISH_UNAVAILABLE,
                              nix::remote::Done::INPUT_UNAVAILABLE, nix::remote::Done::UNREACHABLE,
                              nix::remote::Done::TRANSIENT_FAILURE}) {
        assert((expectsAfterGiveBack(reason) == std::vector<std::string>{"a.drv"}));
    }
    assert((expectsAfterGiveBack(nix::remote::Done::REASON_UNSPECIFIED, nix::remote::Done::NO_SHOW) == std::vector<std::string>{"a.drv"}));
    {
        using enum Step;
        auto const draining = nix::remote::Done::DRAINING;
        // Given back for DRAINING before w0's Load says it drains: a.drv waits
        // for w2 rather than go straight back to w0.
        assert((afterSteps({giveBack}, draining).expects == std::vector<std::string>{"a.drv"}));
        // A give-back for DRAINING that arrives after w0 is back does not
        // make it drain: it would stay draining until its next Hello or
        // drain cycle. It is remembered: w0 takes c.drv, not a.drv. onDone
        // does not republish the membership, so a mark in the core alone
        // shows in `expects`, not in `draining`.
        auto const late = afterSteps({drain, undrain, giveBack, wantC}, draining);
        assert(!late.draining);
        assert((late.expects == std::vector<std::string>{"a.drv", "c.drv"}));
        // Any other give-back from a draining worker is remembered.
        assert((afterSteps({drain, giveBack, undrain}, nix::remote::Done::TRANSIENT_FAILURE).expects
                == std::vector<std::string>{"a.drv"}));
        // Back via Load{not draining} rather than Hello, w0 gets a.drv again.
        assert((afterSteps({drain, giveBack, undrain}, draining).expects == std::vector<std::string>{"a.drv", "a.drv"}));
        // A later give-back that is not remembered keeps the earlier one.
        assert((afterSteps({giveBack, drainW2, drain, giveBackDraining, undrainW2, undrain},
                           nix::remote::Done::TRANSIENT_FAILURE)
                    .expects
                == std::vector<std::string>{"a.drv", "a.drv"}));
        // A draining w2 is not another worker that can run it.
        assert((afterSteps({giveBack, drainW2}, nix::remote::Done::TRANSIENT_FAILURE).expects
                == std::vector<std::string>{"a.drv", "a.drv"}));
        // NO_SHOW for another reason from a draining worker is remembered.
        assert((afterSteps({drain, giveBack, undrain}, nix::remote::Done::TRANSIENT_FAILURE, nix::remote::Done::NO_SHOW)
                    .expects
                == std::vector<std::string>{"a.drv"}));
        // Once back, w0 takes c.drv; a.drv is still passed over while w2 can run it.
        auto const back = afterSteps({giveBack, drain, undrain, wantC}, draining);
        assert(!back.draining);
        assert((back.expects == std::vector<std::string>{"a.drv", "c.drv"}));
    }
    // A worker without slots is draining as far as the membership goes.
    assert(drainingInMembership(0) && !drainingInMembership(1));
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
