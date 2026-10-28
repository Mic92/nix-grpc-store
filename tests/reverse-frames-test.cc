#include <cassert>
#include <string>
#include <stop_token>
#include <thread>

#include "reverse-frames.hh"

int main()
{
    nixgrpc::ReverseFrames frames;
    nix::remote::ReverseOpen start;
    start.set_method("BuildDerivation");
    start.set_capability("scoped-test-capability");
    assert(!frames.open(0, start));
    assert(!frames.open(1, nix::remote::ReverseOpen{}));
    auto unknown = start;
    unknown.set_method("UnknownRpc");
    assert(!frames.open(1, unknown));
    assert(frames.open(1, start));
    assert(!frames.open(1, start));
    assert(!frames.data(2, "not opened"));
    assert(!frames.data(1, std::string(nixgrpc::ReverseFrames::maxFrameBytes + 1, 'x')));
    assert(frames.data(1, "hello"));
    assert(frames.halfClose(1));
    assert(!frames.halfClose(1) && !frames.data(1, "after half close"));
    nix::remote::ReverseFarmMsg msg;
    assert(frames.take(msg) && msg.stream_id() == 1 && msg.has_open());
    assert(frames.take(msg) && msg.stream_id() == 1 && msg.data().data() == "hello");
    assert(frames.take(msg) && msg.stream_id() == 1 && msg.half_close());
    assert(frames.acknowledge(1));
    assert(!frames.acknowledge(1)); // exactly one ack per result
    // The response finishing before the writer sends the ack keeps the
    // reservation; only the sent ack retires it.
    assert(frames.finished(1));
    assert(frames.take(msg) && msg.stream_id() == 1 && msg.acknowledged());
    assert(!frames.take(msg));
    frames.acknowledgementSent(1);
    assert(!frames.acknowledge(1));
    assert(!frames.finished(1));

    assert(!frames.open(1, start)); // cannot reuse a finished ID
    auto upload = start;
    upload.set_method("UploadBuildInputs");
    assert(frames.open(2, upload));
    assert(frames.data(2, "cancelled before dispatch"));
    assert(frames.cancel(2));
    assert(!frames.data(2, "stale"));
    assert(frames.take(msg) && msg.has_open());
    assert(frames.take(msg) && msg.cancel());
    assert(!frames.take(msg));

    for (uint64_t id = 3; id < 3 + nixgrpc::ReverseFrames::maxStreams; ++id) {
        assert(frames.open(id, start));
    }
    assert(!frames.open(1000, start));
    size_t queued = 0;
    while (frames.data(3, std::string(nixgrpc::ReverseFrames::maxFrameBytes, 'z'))) {
        ++queued;
    }
    assert(queued < nixgrpc::ReverseFrames::maxQueuedBytes / nixgrpc::ReverseFrames::maxFrameBytes);
    assert(frames.halfClose(3)); // data saturation must not starve control
    assert(frames.cancel(4));
    frames.close();
    assert(!frames.take(msg));
    assert(!frames.open(1001, start));
    assert(!frames.data(1, "closed"));
    nixgrpc::ReverseFrames blocking;
    std::stop_source stop;
    bool woke = false;
    std::jthread waiter([&] {
        nix::remote::ReverseFarmMsg next;
        woke = blocking.waitTake(next, stop.get_token()) && next.stream_id() == 1 && next.has_open();
    });
    assert(blocking.open(1, start));
    waiter.join();
    assert(woke);
    blocking.close();
    assert(!blocking.waitTake(msg, stop.get_token()));

    nixgrpc::ReverseFrames streamed;
    assert(streamed.open(1, upload));
    assert(streamed.take(msg) && msg.has_open());
    std::string const payload(nixgrpc::ReverseFrames::maxFrameBytes, 'x');
    while (streamed.data(1, payload)) { }
    std::stop_source uploadStop;
    bool enqueued = false;
    std::jthread producer([&] { enqueued = streamed.waitData(1, payload, uploadStop.get_token()); });
    assert(streamed.take(msg) && msg.has_data());
    producer.join();
    assert(enqueued); // freeing queue space wakes the producer

    while (streamed.data(1, payload)) { }
    std::jthread interrupted([&] { enqueued = streamed.waitData(1, payload, uploadStop.get_token()); });
    uploadStop.request_stop();
    interrupted.join();
    assert(!enqueued); // cancellation wakes a backpressured upload
    streamed.close();

    nixgrpc::ReverseFrames cancelled;
    assert(cancelled.open(1, upload));
    while (cancelled.data(1, payload)) { }
    std::stop_source stillRunning;
    std::jthread discarded([&] { enqueued = cancelled.waitData(1, payload, stillRunning.get_token()); });
    assert(cancelled.cancel(1));
    discarded.join();
    assert(!enqueued); // removal of the stream wakes a blocked producer
    cancelled.close();
}
