#pragma once
// HTTP/2 keepalive for streams that may be silent for hours (builds).
// Server-side GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS must stay below the time.
// Set GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA to 0 too: by default pings stop after two on a silent stream.

namespace nixgrpc {

constexpr int kKeepaliveTimeMs = 20'000;
constexpr int kKeepaliveTimeoutMs = 20'000;

} // namespace nixgrpc
