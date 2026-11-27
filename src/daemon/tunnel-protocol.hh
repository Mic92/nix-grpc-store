#pragma once

// Builder side of Envoy's reverse tunnel (1.40). The builder connects to the
// balancer with TLS and sends one HTTP request. After the 200 the balancer
// stops using that TLS session: it sends `RPING` on the bare socket, which
// the builder echoes, until it starts a TLS session of its own for requests.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>

namespace nixgrpc::tunnel {

// The balancer finds a tunnel by `node`, or any tunnel of `group`. It takes
// the certificate's name from the TLS session and `instance` from the request
// and refuses a `node` that is not made of the two. It only takes tunnels
// from certificates whose name it is configured to accept, and `name` is the
// one field it can refuse alone when it reads no such name.
struct Identity
{
    std::string node;
    std::string group;
    std::string instance;
    std::string name;
};

inline constexpr std::string_view instanceHeader = "x-nix-instance";

// `host` is a name or an IP address.
[[nodiscard]] inline auto request(const Identity & self, std::string_view host) -> std::string
{
    const auto bracketed = host.contains(':') ? std::format("[{}]", host) : std::string(host);
    return std::format(
        "GET /reverse_connections/request HTTP/1.1\r\n"
        "host: {}\r\n"
        "x-envoy-reverse-tunnel-node-id: {}\r\n"
        "x-envoy-reverse-tunnel-cluster-id: {}\r\n"
        "x-envoy-reverse-tunnel-tenant-id: {}\r\n"
        "x-envoy-reverse-tunnel-upstream-cluster-name: {}\r\n"
        "{}: {}\r\n"
        "content-length: 0\r\n"
        "\r\n",
        bracketed,
        self.node,
        self.group,
        self.name,
        bracketed,
        instanceHeader,
        self.instance);
}

// What the scheduler hands to clients and the balancer looks a tunnel up by.
[[nodiscard]] inline auto nodeName(std::string_view certName, std::string_view instance) -> std::string
{
    return std::format("{}/{}", certName, instance);
}

// Header values end at a line break, so one inside a name would add headers.
[[nodiscard]] constexpr auto validName(std::string_view name) noexcept -> bool
{
    constexpr size_t maxLen = 253;
    if (name.empty() || name.size() > maxLen) {
        return false;
    }
    return std::ranges::all_of(name, [](char chr) -> bool {
        return (chr >= 'a' && chr <= 'z') || (chr >= 'A' && chr <= 'Z') || (chr >= '0' && chr <= '9') || chr == '-'
            || chr == '.' || chr == '_' || chr == '/';
    });
}

static_assert(validName("any/b1.example"));
static_assert(!validName(""));
static_assert(!validName("b1\r\nx-evil: 1"));
static_assert(!validName("b1:50051"));

// The characters of an IPv6 address; whether it is one is for inet_pton().
[[nodiscard]] constexpr auto validAddress6(std::string_view text) noexcept -> bool
{
    constexpr size_t maxLen = 45;
    return text.contains(':') && text.size() <= maxLen && std::ranges::all_of(text, [](char chr) -> bool {
        return (chr >= '0' && chr <= '9') || (chr >= 'a' && chr <= 'f') || (chr >= 'A' && chr <= 'F') || chr == ':' || chr == '.';
    });
}

static_assert(validAddress6("::1"));
static_assert(validAddress6("::ffff:10.0.0.2"));
static_assert(!validAddress6("10.0.0.2"));
static_assert(!validAddress6("fe80::1%eth0"));
static_assert(!validAddress6("::1\r\nx-evil: 1"));

enum class Reply : uint8_t {
    incomplete,
    accepted,
    refused,
};

// `head` is what arrived so far. Nothing follows the blank line: the balancer
// waits for the tunnel to be registered before it writes anything else.
[[nodiscard]] constexpr auto reply(std::string_view head) noexcept -> Reply
{
    constexpr std::string_view accepted = "HTTP/1.1 200 ";
    if (!head.contains("\r\n\r\n")) {
        return Reply::incomplete;
    }
    return head.starts_with(accepted) ? Reply::accepted : Reply::refused;
}

static_assert(reply("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n") == Reply::incomplete);
static_assert(reply("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n") == Reply::accepted);
static_assert(reply("HTTP/1.1 403 Forbidden\r\n\r\n") == Reply::refused);
static_assert(reply("HTTP/1.1 2000 OK\r\n\r\n") == Reply::refused);

// ALPN names. One port takes both sets of tunnels and checks each differently.
inline constexpr std::string_view namedProtocol = "nix-tunnel";  // requests for this builder
inline constexpr std::string_view anyProtocol = "nix-tunnel-any"; // requests any builder of its system may answer

inline constexpr std::string_view ping = "RPING";

enum class Front : uint8_t {
    ping,    // a whole ping: echo it
    partial, // the start of one: wait for the rest
    data,    // the balancer's request traffic begins here
};

[[nodiscard]] constexpr auto front(std::string_view bytes) noexcept -> Front
{
    if (bytes.starts_with(ping)) {
        return Front::ping;
    }
    return ping.starts_with(bytes) ? Front::partial : Front::data;
}

static_assert(front("RPING") == Front::ping);
static_assert(front("RPING\x16\x03") == Front::ping);
static_assert(front("") == Front::partial);
static_assert(front("RPI") == Front::partial);
static_assert(front("\x16\x03\x01") == Front::data);
static_assert(front("RPIX") == Front::data);

} // namespace nixgrpc::tunnel
