// Which failures the client retries, and the backoff under a deadline.

#include <cassert>

#include <grpcpp/support/status.h>

#include "retry.hh"

namespace {

auto unavailable(const char * msg) -> grpc::Status { return {grpc::StatusCode::UNAVAILABLE, msg}; }

// Messages as seen in nixbot build logs.
void classifiesFarmErrors()
{
    assert(nixgrpc::goneAway(unavailable(
        "failed to connect to all addresses; last error: UNKNOWN: ipv4:65.21.215.17:50051: handshaker shutdown")));
    assert(nixgrpc::goneAway(unavailable(
        "failed to connect to all addresses; last error: UNKNOWN: ipv4:65.21.215.17:50051: tcp handshaker shutdown")));
    assert(nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNKNOWN: "
                                         "ipv4:65.21.215.17:50051: connection attempt timed out before "
                                         "receiving SETTINGS frame")));
    assert(!nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNKNOWN: "
                                          "ipv4:65.21.215.17:50051: Ssl handshake failed: certificate verify failed")));
    assert(!nixgrpc::goneAway({grpc::StatusCode::PERMISSION_DENIED, "Connection reset"}));
    // A call that reached a daemon as it exited idle, before any handler took it.
    assert(nixgrpc::goneAway({grpc::StatusCode::CANCELLED, ""}));
    assert(!nixgrpc::goneAway({grpc::StatusCode::CANCELLED, "Cancelled by build hook"}));
    assert(nixgrpc::streamBroken({grpc::StatusCode::UNKNOWN, "Stream removed (Socket closed)"}));
}

} // namespace

auto main() -> int
try {
    classifiesFarmErrors();
    return 0;
} catch (...) {
    return 1;
}
