#include <cassert>
#include <string>

#include "reverse-capability.hh"
#include "reverse-frames.hh"
#include "reverse-inbound.hh"
#include "reverse-registry.hh"
#include "reverse-responses.hh"

int main()
{
    using nixgrpc::Caller;
    using nixgrpc::ReverseCapability;
    using nixgrpc::ReverseFrames;
    using nixgrpc::ReverseInbound;
    using nixgrpc::ReverseRegistry;
    using nixgrpc::ReverseResponses;
    using nixgrpc::Role;

    ReverseRegistry registry;
    registry.setActive(true);
    auto key = ReverseCapability::newKey();
    assert(key);
    std::string const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    auto handle = registry.registerWorker(worker, worker, [] {}, *key);
    assert(handle && registry.current(*handle));
    ReverseFrames outgoing;
    ReverseInbound incoming(worker, (*handle)->generation, (*handle)->sessionKey);
    ReverseResponses responses;

    Caller const writer{.name = "client", .role = Role::write, .kind = Caller::Kind::named};
    auto capability = ReverseCapability::mint((*handle)->sessionKey, worker, (*handle)->generation, 1,
                                               "BuildDerivation", writer, "assigned.drv", 42, 0);
    assert(capability && responses.open(1));
    nix::remote::ReverseOpen start;
    start.set_method("BuildDerivation");
    start.set_capability(*capability);
    assert(outgoing.open(1, std::move(start)) && outgoing.data(1, "request bytes") && outgoing.halfClose(1));

    nix::remote::ReverseFarmMsg toWorker;
    assert(outgoing.take(toWorker));
    auto opened = incoming.accept(toWorker);
    assert(opened && opened->kind == ReverseInbound::Event::Kind::open && opened->caller);
    nix::remote::BuildDerivationRequest request;
    request.set_drv_path("assigned.drv");
    request.set_assign_id(42);
    assert(ReverseCapability::matchesBuild(*opened->caller, request));
    request.set_assign_id(43);
    assert(!ReverseCapability::matchesBuild(*opened->caller, request));
    assert(outgoing.take(toWorker) && incoming.accept(toWorker)->kind == ReverseInbound::Event::Kind::data);
    assert(outgoing.take(toWorker) && incoming.accept(toWorker)->kind == ReverseInbound::Event::Kind::halfClose);
    assert(!outgoing.take(toWorker));

    nix::remote::ReverseWorkerMsg fromWorker;
    fromWorker.set_stream_id(1);
    fromWorker.mutable_data()->set_data("build response");
    assert(responses.accept(fromWorker));
    fromWorker.mutable_finish()->set_code(static_cast<uint32_t>(grpc::StatusCode::OK));
    assert(responses.accept(fromWorker));
    assert(!responses.accept(fromWorker));
    nix::remote::ReverseWorkerMsg received;
    assert(responses.take(received) && received.data().data() == "build response");
    assert(responses.take(received) && received.has_finish() && received.finish().code() == 0);
    assert(responses.finished(1) && incoming.finished(1) && outgoing.finished(1));

    auto nextKey = ReverseCapability::newKey();
    assert(nextKey);
    auto replacement = registry.registerWorker(worker, worker, [] {}, *nextKey);
    assert(replacement && !registry.current(*handle));
    ReverseInbound next(worker, (*replacement)->generation, (*replacement)->sessionKey);
    nix::remote::ReverseFarmMsg stale;
    stale.set_stream_id(1);
    stale.mutable_open()->set_method("BuildDerivation");
    stale.mutable_open()->set_capability(*capability);
    assert(!next.accept(stale)); // no replay into replacement registration
    outgoing.close();
    incoming.close();
    responses.close();
}
