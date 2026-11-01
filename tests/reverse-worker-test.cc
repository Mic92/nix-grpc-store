#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include <grpcpp/support/status.h>
#include <grpcpp/support/status_code_enum.h>

#include "nix_remote.pb.h"
#include "reverse-frames.hh"
#include "reverse-relay.hh"
#include "reverse-worker.hh"

namespace {

constexpr size_t lengthPrefixBytes = 4;
constexpr unsigned bitsPerByte = 8;
constexpr size_t multiFrameResultBytes = 130000;
constexpr uint64_t saturationAttempts = 10000;
constexpr size_t statusTextBytes = 1024;
constexpr uint64_t retiredStream = 10;
constexpr uint64_t lostStream = 11;

} // namespace

auto main() -> int
{
    nixgrpc::ReverseWorkerOutbound replies;
    std::stop_source const stop;
    nix::remote::BuildDerivationChunk chunk;
    chunk.mutable_done()->set_result(std::string(multiFrameResultBytes, 'x'));
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
    assert(wire.size() >= lengthPrefixBytes);
    uint32_t length = 0;
    for (unsigned index = 0; index != lengthPrefixBytes; ++index) {
        length |= static_cast<uint32_t>(static_cast<unsigned char>(wire.at(index))) << (bitsPerByte * index);
    }
    nix::remote::BuildDerivationChunk decoded;
    auto const body = std::string_view(wire).substr(lengthPrefixBytes);
    assert(wire.size() == length + lengthPrefixBytes && decoded.ParseFromArray(body.data(), static_cast<int>(length)));
    assert(decoded.done().result() == chunk.done().result());

    bool retired = false;
    bool const finishQueued = replies.finish(retiredStream, grpc::Status::OK, stop.get_token(), true, [&] -> void { retired = true; });
    assert(finishQueued);
    assert(replies.take(frame, stop.get_token()) && frame.stream_id() == retiredStream && frame.has_finish());
    assert(!retired); // writing a finish does not retire the assignment
    assert(replies.acknowledged(retiredStream) && retired);
    assert(!replies.acknowledged(retiredStream)); // duplicate acknowledgements fail closed
    nixgrpc::ReverseWorkerOutbound lost;
    bool abandoned = false;
    bool const lostQueued = lost.finish(lostStream, grpc::Status::OK, stop.get_token(), true, [&] -> void { abandoned = true; });
    assert(lostQueued);
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
    for (uint64_t streamId = 1; streamId <= saturationAttempts; ++streamId) {
        if (!saturated.finish(streamId, {grpc::StatusCode::CANCELLED, std::string(statusTextBytes, 'x')},
                              stop.get_token(), false)) {
            full = true;
            break;
        }
    }
    assert(full); // cancellation from the reader never waits for queue space
    saturated.close();

    nixgrpc::ReverseWorkerOutbound blocked;
    std::string const statusText(statusTextBytes, 'x');
    bool queued = true;
    for (uint64_t streamId = 1; streamId <= saturationAttempts; ++streamId) {
        if (!blocked.finish(streamId, {grpc::StatusCode::CANCELLED, statusText}, stop.get_token(), false)) {
            queued = false;
            break;
        }
    }
    assert(!queued);
    std::jthread producer([&](const std::stop_token & workStop) -> void {
        queued = blocked.finish(saturationAttempts + 1, {grpc::StatusCode::CANCELLED, statusText}, workStop);
    });
    blocked.close();
    producer.join();
    assert(!queued); // disconnect wakes a response producer waiting for space
}
