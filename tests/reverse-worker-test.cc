#include <cassert>
#include <stop_token>
#include <string>
#include <thread>

#include "reverse-worker.hh"

int main()
{
    nixgrpc::ReverseWorkerOutbound replies;
    std::stop_source stop;
    nix::remote::BuildDerivationChunk chunk;
    chunk.mutable_done()->set_result(std::string(130000, 'x'));
    assert(replies.message(1, chunk, stop.get_token()));
    assert(replies.finish(1, grpc::Status::OK, stop.get_token()));
    nix::remote::ReverseWorkerMsg frame;
    std::string wire;
    while (replies.take(frame, stop.get_token())) {
        assert(frame.stream_id() == 1);
        if (frame.has_finish()) {
            assert(frame.finish().code() == 0);
            break;
        }
        assert(frame.data().data().size() <= nixgrpc::ReverseFrames::maxFrameBytes);
        wire += frame.data().data();
    }
    assert(wire.size() >= 4);
    uint32_t length = 0;
    for (unsigned i = 0; i != 4; ++i)
        length |= static_cast<uint32_t>(static_cast<unsigned char>(wire[i])) << (8 * i);
    nix::remote::BuildDerivationChunk decoded;
    assert(wire.size() == length + 4 && decoded.ParseFromArray(wire.data() + 4, static_cast<int>(length)));
    assert(decoded.done().result() == chunk.done().result());

    bool retired = false;
    assert(replies.finish(10, grpc::Status::OK, stop.get_token(), true, [&] { retired = true; }));
    assert(replies.take(frame, stop.get_token()) && frame.stream_id() == 10 && frame.has_finish());
    assert(!retired); // writing a finish does not retire the assignment
    assert(replies.acknowledged(10) && retired);
    assert(!replies.acknowledged(10)); // duplicate acknowledgements fail closed
    nixgrpc::ReverseWorkerOutbound lost;
    bool abandoned = false;
    assert(lost.finish(11, grpc::Status::OK, stop.get_token(), true, [&] { abandoned = true; }));
    lost.close();
    assert(!abandoned);
    lost.abandonUnacknowledged();
    assert(abandoned);

    assert(replies.message(2, chunk, stop.get_token()));
    replies.cancel(2);
    assert(!replies.message(2, chunk, stop.get_token()));
    assert(replies.finish(2, {grpc::StatusCode::CANCELLED, "cancelled"}, stop.get_token()));
    assert(replies.take(frame, stop.get_token()) && frame.stream_id() == 2 && frame.has_finish());

    nix::remote::BuildDerivationChunk huge;
    huge.mutable_done()->set_result(std::string(nixgrpc::ReverseRelay::maxMessageBytes + 1, 'x'));
    assert(!replies.message(3, huge, stop.get_token()));
    replies.close();
    assert(!replies.finish(3, grpc::Status::OK, stop.get_token()));

    nixgrpc::ReverseWorkerOutbound saturated;
    bool full = false;
    for (uint64_t id = 1; id <= 10000; ++id) {
        if (!saturated.finish(id, {grpc::StatusCode::CANCELLED, std::string(1024, 'x')},
                              stop.get_token(), false)) {
            full = true;
            break;
        }
    }
    assert(full); // cancellation from the reader never waits for queue space
    saturated.close();

    nixgrpc::ReverseWorkerOutbound blocked;
    std::string const statusText(1024, 'x');
    bool queued = true;
    for (uint64_t id = 1; id <= 10000; ++id) {
        if (!blocked.finish(id, {grpc::StatusCode::CANCELLED, statusText}, stop.get_token(), false)) {
            queued = false;
            break;
        }
    }
    assert(!queued);
    std::jthread producer([&](std::stop_token workStop) {
        queued = blocked.finish(10001, {grpc::StatusCode::CANCELLED, statusText}, workStop);
    });
    blocked.close();
    producer.join();
    assert(!queued); // disconnect wakes a response producer waiting for space
}
