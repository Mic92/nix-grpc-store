#pragma once
// Scheduler and reverse worker must run the same reverse protocol: the
// result acknowledgement, assignment-scoped Revoke and in-band token
// rotation have no fallback. Bump on any incompatible change. Zero is what
// a peer from before this field sends.

#include <cstdint>
#include <string>

#include <nix/util/error.hh>

namespace nixgrpc {

constexpr uint32_t reverseProtocolVersion = 1;

// Ends a WorkerSession with FAILED_PRECONDITION instead of INVALID_ARGUMENT.
struct ReverseProtocolMismatch : nix::Error
{
    using nix::Error::Error;
};

inline auto reverseProtocolMismatch(uint32_t peer) -> std::string
{
    return "reverse protocol " + std::to_string(peer) + " does not match " + std::to_string(reverseProtocolVersion)
        + "; roll out scheduler and reverse workers together";
}

} // namespace nixgrpc
