#pragma once

// A builder that cannot be dialled opens connections to the balancer, which
// then sends its requests over them. See tunnel-protocol.hh for the wire side.

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include <nix/util/file-descriptor.hh>

#include "tunnel-protocol.hh"

namespace nixgrpc::tunnel {

struct Target
{
    std::string host;    // what the balancer's certificate must be valid for
    std::string address; // one IP address of `host`
    uint16_t port = 0;
    std::string protocol; // ALPN name: tells the balancer which set of tunnels this one joins
};

struct Tls
{
    std::string cert;
    std::string key;
    std::string ca; // trusted besides the system's roots, may be empty
};

// Interrupts a wait from another thread until cleared.
class Wake
{
public:
    Wake();
    void set() const;
    void clear() const;
    // True when set, false when `timeout` passed first.
    [[nodiscard]] auto wait(std::optional<std::chrono::milliseconds> timeout) const -> bool;
    [[nodiscard]] auto fd() const -> int
    {
        return readSide.get();
    }

private:
    nix::AutoCloseFD readSide;
    nix::AutoCloseFD writeSide;
};

// Opens a tunnel and answers the balancer's pings until it starts a request,
// then returns the socket with that request unread. Calls `accepted` once the
// balancer took the tunnel. Returns no socket when `stop` interrupted. Throws when the tunnel cannot be opened or the balancer
// closes it.
auto open(const Target & target, const Tls & tls, const Identity & self, const Wake & stop, const std::function<void()> & accepted = {})
    -> std::optional<nix::AutoCloseFD>;

enum class End : uint8_t {
    closed,
    stopped,
};

// Waits until the connection behind `sock` ended, while another descriptor of
// the same socket is in use elsewhere.
auto waitClosed(int sock, const Wake & stop) -> End;

// The name the balancer knows the builder by: the common name of the
// certificate in `file`, or without one the last segment of its SPIFFE ID.
auto certName(const std::string & file) -> std::string;

} // namespace nixgrpc::tunnel
