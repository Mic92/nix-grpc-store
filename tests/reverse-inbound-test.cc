#include <cassert>
#include <string>

#include "reverse-inbound.hh"

int main()
{
    using nixgrpc::Caller;
    using nixgrpc::ReverseCapability;
    using nixgrpc::ReverseInbound;
    using nixgrpc::Role;

    auto key = ReverseCapability::newKey();
    assert(key);
    auto const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    ReverseInbound inbound(worker, 4, *key);
    Caller reader{.name = "client", .role = Role::readOnly, .kind = Caller::Kind::named};
    auto const capability = ReverseCapability::mint(*key, worker, 4, 1, "StoreInfo", reader);
    assert(capability);
    nix::remote::ReverseFarmMsg open;
    open.set_stream_id(1);
    open.mutable_open()->set_method("StoreInfo");
    open.mutable_open()->set_capability(*capability);
    auto event = inbound.accept(open);
    assert(event && event->kind == ReverseInbound::Event::Kind::open && event->caller &&
           event->caller->caller.role == Role::readOnly);
    assert(!inbound.accept(open));
    auto wrong = open;
    wrong.set_stream_id(2);
    assert(!inbound.accept(wrong)); // capability scoped to ID 1
    wrong = open;
    wrong.mutable_open()->set_method("Connect");
    assert(!inbound.accept(wrong));
    wrong = open;
    wrong.set_session_key(*key); // ack key cannot appear on data frames
    assert(!inbound.accept(wrong));
    wrong = open;
    wrong.set_generation(4);
    assert(!inbound.accept(wrong));

    nix::remote::ReverseFarmMsg data;
    data.set_stream_id(1);
    data.mutable_data()->set_data("payload");
    event = inbound.accept(data);
    assert(event && event->kind == ReverseInbound::Event::Kind::data);
    data.set_stream_id(2);
    assert(!inbound.accept(data));
    data.set_stream_id(1);
    data.mutable_data()->set_data(std::string(nixgrpc::ReverseFrames::maxFrameBytes + 1, 'x'));
    assert(!inbound.accept(data));
    nix::remote::ReverseFarmMsg half;
    half.set_stream_id(1);
    half.set_half_close(true);
    event = inbound.accept(half);
    assert(event && event->kind == ReverseInbound::Event::Kind::halfClose);
    assert(!inbound.accept(half));
    data.mutable_data()->set_data("late");
    assert(!inbound.accept(data));
    assert(inbound.finished(1));
    assert(!inbound.accept(open)); // never reuse a finished ID
    nix::remote::ReverseFarmMsg lateCancel;
    lateCancel.set_stream_id(1);
    lateCancel.set_cancel(true);
    assert(inbound.accept(lateCancel)); // late cancel races with the final response
    assert(!inbound.accept(lateCancel));
    nix::remote::ReverseFarmMsg cancel;
    cancel.set_stream_id(2);
    cancel.set_cancel(true);
    assert(!inbound.accept(cancel));
    auto cap2 = ReverseCapability::mint(*key, worker, 4, 2, "StoreInfo", reader);
    assert(cap2);
    open.set_stream_id(2);
    open.mutable_open()->set_capability(*cap2);
    assert(inbound.accept(open));
    assert(inbound.accept(cancel));
    assert(!inbound.accept(cancel));
    assert(inbound.finished(2)); // completion after a running handler was cancelled
    assert(!inbound.finished(2));
    inbound.close();
    assert(!inbound.accept(open));
}
