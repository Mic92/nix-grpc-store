#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <stop_token>
#include <thread>

#include <grpcpp/support/status_code_enum.h>

#include "nix_remote.pb.h"
#include "reverse-frames.hh"
#include "reverse-responses.hh"

namespace {

constexpr uint32_t invalidStatusCode = 123;
constexpr uint64_t otherStream = 5;
constexpr uint64_t isolatedStream = 6;

} // namespace

auto main() -> int
{
    nixgrpc::ReverseResponses replies;
    assert(!replies.open(0));
    assert(replies.open(1) && !replies.open(1));
    nix::remote::ReverseWorkerMsg response;
    response.set_worker_id("client-chosen-worker");
    response.set_stream_id(1);
    response.mutable_data()->set_data("x");
    assert(!replies.accept(response));
    response.clear_worker_id();
    assert(replies.accept(response));
    response.set_stream_id(2);
    assert(!replies.accept(response));
    response.set_stream_id(1);
    response.mutable_data()->set_data(std::string(nixgrpc::ReverseFrames::maxFrameBytes + 1, 'x'));
    assert(!replies.accept(response));
    response.mutable_data()->set_data("tail");
    assert(replies.accept(response));
    response.mutable_finish()->set_code(static_cast<uint32_t>(grpc::StatusCode::OK));
    assert(replies.accept(response));
    assert(!replies.accept(response));
    assert(replies.finished(1));
    assert(!replies.open(1) && !replies.accept(response));
    nix::remote::ReverseWorkerMsg taken;
    assert(replies.take(taken) && taken.data().data() == "x");
    assert(replies.take(taken) && taken.data().data() == "tail");
    assert(replies.take(taken) && taken.has_finish());
    assert(!replies.take(taken));

    assert(replies.open(2));
    response.set_stream_id(2);
    response.mutable_finish()->set_code(invalidStatusCode);
    assert(!replies.accept(response));
    response.mutable_finish()->set_code(static_cast<uint32_t>(grpc::StatusCode::UNAVAILABLE));
    response.mutable_finish()->set_message(std::string(nixgrpc::ReverseResponses::maxStatusBytes + 1, 'x'));
    assert(!replies.accept(response));
    response.mutable_finish()->set_message("worker unavailable");
    assert(replies.accept(response));
    assert(replies.cancel(2));
    assert(!replies.take(taken) && !replies.finished(2));
    response.mutable_data()->set_data("late cancelled data");
    assert(!replies.accept(response)); // finish was already queued at cancel
    assert(replies.open(3));
    response.set_stream_id(3);
    assert(replies.accept(response));
    assert(replies.cancel(3));
    assert(replies.accept(response)); // late frame after cancel, before finish
    response.mutable_finish()->set_code(0);
    assert(replies.accept(response) && !replies.accept(response));
    assert(!replies.take(taken));

    assert(replies.open(4));
    response.set_stream_id(4);
    response.mutable_data()->set_data(std::string(nixgrpc::ReverseFrames::maxFrameBytes, 'x'));
    size_t queued = 0;
    while (replies.accept(response)) {
        ++queued;
    }
    assert(queued > 0 && queued < nixgrpc::ReverseFrames::maxQueuedBytes / nixgrpc::ReverseFrames::maxFrameBytes);
    response.mutable_finish()->set_code(static_cast<uint32_t>(grpc::StatusCode::UNAVAILABLE));
    response.mutable_finish()->set_message("worker stopped");
    assert(replies.accept(response)); // data saturation cannot starve final status
    assert(replies.cancel(4));
    assert(!replies.take(taken));
    assert(replies.open(otherStream));
    assert(replies.open(isolatedStream));
    std::stop_source stop;
    bool received = false;
    std::jthread waiter([&] -> void {
        nix::remote::ReverseWorkerMsg isolated;
        received = replies.waitTake(isolatedStream, isolated, stop.get_token())
            && isolated.stream_id() == isolatedStream && isolated.data().data() == "only six";
    });
    response.set_stream_id(otherStream);
    response.mutable_data()->set_data("only five");
    assert(replies.accept(response));
    response.set_stream_id(isolatedStream);
    response.mutable_data()->set_data("only six");
    assert(replies.accept(response));
    waiter.join();
    assert(received);
    assert(replies.take(taken) && taken.stream_id() == otherStream);
    assert(replies.cancel(otherStream));
    replies.close();
    assert(!replies.open(7) && !replies.accept(response) && !replies.take(taken));
}
