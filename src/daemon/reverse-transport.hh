#pragma once
// Session-local queues. Neither side grants authority by itself: the gateway
// must authorize a real client and pin the exact active registry handle.

#include <atomic>
#include <cstdint>

#include "reverse-frames.hh"
#include "reverse-responses.hh"

namespace nixgrpc {

struct ReverseTransport
{
    // Stream IDs must never be reused within one registration, even after a
    // canceled or failed reservation. Other registrations have new keys.
    std::atomic<uint64_t> nextStreamId{1};
    ReverseFrames outgoing;
    ReverseResponses responses;

    void close()
    {
        outgoing.close();
        responses.close();
    }
};

} // namespace nixgrpc
