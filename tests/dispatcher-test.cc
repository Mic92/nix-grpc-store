// The Dispatcher driven through the calls its transports make, without gRPC.

#include <cassert>
#include <cstdint>
#include <exception>
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
    Dispatcher disp{Dispatcher::Config{.defaultSystem = "x", .present = {}, .lookupThreads = 0}, metrics};
    Dispatcher::ClientPtr client = disp.connectClient([](const nix::remote::SchedMsg &) -> bool { return true; }, {});

    void hello(Dispatcher::Worker & session, const std::string & addr, uint32_t slots = 2)
    {
        nix::remote::WorkerMsgs msgs;
        auto * hel = msgs.add_msgs()->mutable_hello();
        hel->set_addr(addr);
        hel->add_systems("x");
        hel->set_max_jobs(slots);
        disp.workerMsgs(session, msgs);
    }

    void want(const std::string & drv)
    {
        nix::remote::ClientMsgs msgs;
        auto * wnt = msgs.add_msgs()->mutable_want();
        wnt->set_drv_path(drv);
        wnt->set_system("x");
        disp.clientMsgs(client, msgs);
    }
};

// What a worker session was told to expect.
struct Session
{
    bool alive = true;
    bool ended = false;
    std::vector<std::string> expected;
    Dispatcher::Worker conn{
        .send = [this](const nix::remote::SchedCmd & cmd) -> bool {
            if (alive && cmd.has_expect()) {
                expected.push_back(cmd.expect().drv_path());
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

} // namespace

auto main() -> int
try {
    endOfADroppedSessionKeepsTheNewOne();
    dropPlacesItsWorkElsewhere();
    return 0;
} catch (std::exception &) {
    return 1;
}
