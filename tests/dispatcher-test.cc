// Dispatcher::onDone against an in-process Dispatcher, without gRPC.

#include <cassert>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

using namespace nixgrpc;
using Done = nix::remote::Done;

namespace {

using Placements = std::vector<std::string>;

// w2 (1 slot) is busy. w0 (3 slots) has a.drv; whoever has it gives it
// back as `outcome` for `reason`. If w1 (2 slots) is free: twice, then w2 is
// done, then once more. If w1 is busy too: once, then w0 says Load. The
// workers a.drv was sent to, in order, and "load" for the Load.
auto placements(Done::Outcome outcome, Done::Reason reason, bool w1Busy) -> Placements
{
    Metrics metrics{""};
    Dispatcher disp({.defaultSystem = "x", .lookupThreads = 0}, metrics);
    Placements sent;
    std::map<std::string, Dispatcher::Worker> conns;
    auto const send = [&](const std::string & addr, const nix::remote::WorkerMsg & msg) -> void {
        nix::remote::WorkerMsgs msgs;
        *msgs.add_msgs() = msg;
        disp.workerMsgs(conns[addr], msgs);
    };
    auto const client = disp.connectClient([](const nix::remote::SchedMsg &) -> bool { return true; }, [] -> void {});
    auto const join = [&](const std::string & addr, uint32_t jobs, const std::string & drv) -> void {
        auto const * const name = &conns.try_emplace(addr).first->first;
        auto * const seen = &sent;
        conns[addr].send = [name, seen](const nix::remote::SchedCmd & cmd) -> bool {
            if (cmd.expect().drv_path() == "a.drv") {
                seen->push_back(*name);
            }
            return true;
        };
        nix::remote::WorkerMsg hello;
        hello.mutable_hello()->set_addr(addr);
        hello.mutable_hello()->add_systems("x");
        hello.mutable_hello()->set_max_jobs(jobs);
        send(addr, hello);
        if (!drv.empty()) {
            nix::remote::ClientMsgs want;
            want.add_msgs()->mutable_want()->set_drv_path(drv);
            disp.clientMsgs(client, want);
        }
    };
    auto const giveBack = [&] -> void {
        nix::remote::WorkerMsg msg;
        msg.mutable_done()->set_drv_path("a.drv");
        msg.mutable_done()->set_outcome(outcome);
        msg.mutable_done()->set_reason(reason);
        send(sent.back(), msg);
    };
    join("w2", 1, "b.drv");
    join("w1", w1Busy ? 1 : 2, w1Busy ? "c.drv" : "");
    join("w0", 3, "a.drv");
    giveBack();
    if (w1Busy) {
        sent.emplace_back("load");
        nix::remote::WorkerMsg load;
        load.mutable_load()->set_draining(false);
        send("w0", load);
        return sent;
    }
    giveBack();
    nix::remote::WorkerMsg built;
    built.mutable_done()->set_drv_path("b.drv");
    built.mutable_done()->set_outcome(Done::BUILT);
    send("w2", built);
    giveBack();
    return sent;
}

} // namespace

// Each reason passes its giver over as long as it holds. About the drv:
// every giver while a free worker never had it (w2 last), else the last one.
// About the clients, or w0 draining: until the next give-back (w1 again
// last), and for draining until w0's next Load. About the cache: nobody.
auto main() -> int
try {
    using P = Placements;
    for (auto const & [outcome, reason, free, busy] : std::vector<std::tuple<Done::Outcome, Done::Reason, P, P>>{
             {Done::GIVEN_BACK, Done::TRANSIENT_FAILURE, {"w0", "w1", "w0", "w2"}, {"w0", "load"}},
             {Done::GIVEN_BACK, Done::REASON_UNSPECIFIED, {"w0", "w1", "w0", "w2"}, {"w0", "load"}},
             {Done::GIVEN_BACK, Done::UNREACHABLE, {"w0", "w1", "w0", "w1"}, {"w0", "load"}},
             {Done::NO_SHOW, Done::REASON_UNSPECIFIED, {"w0", "w1", "w0", "w1"}, {"w0", "load"}},
             {Done::GIVEN_BACK, Done::DRAINING, {"w0", "w1", "w0", "w1"}, {"w0", "load", "w0"}},
             {Done::GIVEN_BACK, Done::INPUT_UNAVAILABLE, {"w0", "w0", "w0", "w0"}, {"w0", "w0", "load"}},
             {Done::GIVEN_BACK, Done::PUBLISH_UNAVAILABLE, {"w0", "w0", "w0", "w0"}, {"w0", "w0", "load"}}}) {
        assert(placements(outcome, reason, false) == free);
        assert(placements(outcome, reason, true) == busy);
    }
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
