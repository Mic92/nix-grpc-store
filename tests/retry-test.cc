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
    // The peer took the connection and closed it before it spoke TLS.
    assert(nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNAVAILABLE: "
                                         "ipv4:127.0.0.1:41288: Handshake read failed (Socket closed)")));
    // The peer does not speak TLS, or refuses us with a TLS alert.
    assert(!nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNKNOWN: "
                                          "ipv4:127.0.0.1:41916: Ssl handshake failed (TSI_PROTOCOL_FAILURE): "
                                          "SSL_ERROR_SSL: error:0A00010B:SSL routines::wrong version number")));
    assert(!nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNKNOWN: "
                                          "ipv4:127.0.0.1:42678: Ssl handshake failed (TSI_PROTOCOL_FAILURE): "
                                          "SSL_ERROR_SSL: error:0A000410:SSL routines::ssl/tls alert handshake failure")));
    assert(!nixgrpc::goneAway({grpc::StatusCode::PERMISSION_DENIED, "Connection reset"}));
    // Whatever the daemon says with UNAVAILABLE is worth a retry.
    assert(nixgrpc::goneAway(unavailable("publish failed: niks3 push exited with 1")));
    assert(nixgrpc::goneAway(unavailable("no healthy upstream")));
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
