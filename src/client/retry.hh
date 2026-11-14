#pragma once
// Which failed calls the client retries and for how long, shared by the
// unary store calls, the Schedule stream and the NAR fetch streams.

#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <string_view>

#include <grpcpp/support/status.h>

#include "retry-pause.hh"

namespace nixgrpc {

// TCP/DNS-level failures. TLS rejections are not here: retrying those only
// delays the error. gRPC words a connect timeout as "handshaker shutdown",
// with or without a "tcp " prefix, and that is no TLS problem either.
inline auto transportError(std::string_view msg) -> bool
{
    static constexpr auto needles = std::to_array<std::string_view>({
        "Connection refused",
        "Connection reset",
        "No route",
        "unreachable",
        "timed out",
        "handshaker shutdown",
        "DNS",
        "GOAWAY"});
    return std::ranges::any_of(needles, [&](std::string_view needle) -> bool { return msg.contains(needle); });
}

// TLS said no: a certificate was refused or the peer does not speak TLS.
// A peer that closes during the handshake without saying so ("Handshake
// read failed") is restarting, and not listed.
inline auto tlsRejected(std::string_view msg) -> bool
{
    static constexpr auto needles = std::to_array<std::string_view>({"andshake failed", "certificate"});
    return std::ranges::any_of(needles, [&](std::string_view needle) -> bool { return msg.contains(needle); });
}

// A worker or the balancer restarting under us, or not reachable right now.
// The daemon sends UNAVAILABLE only for that; TLS setup failures are config errors.
inline auto goneAway(const grpc::Status & status) -> bool
{
    auto const & msg = status.error_message();
    switch (status.error_code()) {
    case grpc::StatusCode::UNAVAILABLE:
        return transportError(msg) || !tlsRejected(msg);
    case grpc::StatusCode::CANCELLED:
        // Empty: the daemon shut down before a handler took the call.
        return msg.empty() || msg.contains("Cancelling all calls");
    default:
        return false;
    }
}

// The connection died under an open stream. Only callers that know what the
// stream already delivered can retry this safely.
inline auto streamBroken(const grpc::Status & status) -> bool
{
    return status.error_message().contains("Stream removed");
}

// The pauses of one retry loop, under a deadline.
class Backoff
{
public:

    explicit Backoff(std::chrono::milliseconds budget)
        : deadline(std::chrono::steady_clock::now() + budget)
    {
    }

    // Whether one more pause still ends before the deadline.
    [[nodiscard]] auto exhausted() const -> bool
    {
        return std::chrono::steady_clock::now() + retryPause(retry) > deadline;
    }

    // The pause to sleep now.
    [[nodiscard]] auto next() -> std::chrono::milliseconds { return retryPause(retry++); }

private:
    std::chrono::steady_clock::time_point deadline;
    unsigned retry = 1;
};

struct RetryPolicy
{
    // Empty: never retry.
    std::function<bool(const grpc::Status &)> retryable;
    // Asked per call: the store allows more once the server has answered.
    std::function<std::chrono::milliseconds()> budget = [] -> std::chrono::milliseconds { return {}; };

    [[nodiscard]] auto shouldRetry(const grpc::Status & status, const Backoff & backoff) const -> bool
    {
        return !status.ok() && retryable && retryable(status) && !backoff.exhausted();
    }
};

} // namespace nixgrpc
