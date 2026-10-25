#pragma once
// Which failed calls the client retries and for how long, shared by the
// unary store calls, the Schedule stream and the NAR fetch streams.

#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <string_view>

#include <grpcpp/support/status.h>

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

// A worker or the balancer restarting under us, or not reachable right now.
inline auto goneAway(const grpc::Status & status) -> bool
{
    auto const & msg = status.error_message();
    switch (status.error_code()) {
    case grpc::StatusCode::UNAVAILABLE:
        return transportError(msg) || msg.contains("upstream") || msg.contains("shutting down")
               || msg.contains("local nix-daemon connection lost");
    case grpc::StatusCode::CANCELLED:
        return msg.contains("Cancelling all calls");
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

// Doubling pause under a deadline.
class Backoff
{
public:
    static constexpr std::chrono::milliseconds firstPause{500};
    static constexpr std::chrono::milliseconds maxPause{4000};

    explicit Backoff(std::chrono::milliseconds budget)
        : deadline(std::chrono::steady_clock::now() + budget)
    {
    }

    // Whether one more pause still ends before the deadline.
    [[nodiscard]] auto exhausted() const -> bool { return std::chrono::steady_clock::now() + pause > deadline; }

    // The pause to sleep now; the next one is twice as long.
    [[nodiscard]] auto next() -> std::chrono::milliseconds
    {
        auto const current = pause;
        pause = std::min(pause * 2, maxPause);
        return current;
    }

private:
    std::chrono::steady_clock::time_point deadline;
    std::chrono::milliseconds pause = firstPause;
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
