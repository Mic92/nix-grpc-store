#pragma once
// Client identity forwarded by a TLS-terminating proxy (envoy's
// x-forwarded-client-cert). Only believed from a peer whose own certificate
// matches --trusted-proxy, so the proxy must use SANITIZE_SET semantics. The
// proxy verified the client's chain: a forwarded SPIFFE ID is only as good
// as the proxy's check that the chain belongs to that ID's trust domain.

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/server_context.h>

#include "peer.hh"

namespace nixgrpc::xfcc {

constexpr std::string_view header = "x-forwarded-client-cert";
constexpr size_t maxHeaderBytes = 16384;
constexpr size_t maxCommonNameBytes = 256;

namespace detail {

class Cursor
{
    std::string_view rest;

public:
    explicit Cursor(std::string_view input)
        : rest(input)
    {
    }

    [[nodiscard]] auto done() const -> bool
    {
        return rest.empty();
    }

    [[nodiscard]] auto peek() const -> char
    {
        return rest.empty() ? '\0' : rest.front();
    }

    auto take() -> char
    {
        if (rest.empty()) {
            return '\0';
        }
        char const chr = rest.front();
        rest.remove_prefix(1);
        return chr;
    }

    // Up to (not including) any char in `stops`. nullopt if none found.
    auto until(std::string_view stops) -> std::optional<std::string_view>
    {
        auto const end = rest.find_first_of(stops);
        if (end == std::string_view::npos) {
            return std::nullopt;
        }
        auto out = rest.substr(0, end);
        rest.remove_prefix(end);
        return out;
    }

    auto remaining() -> std::string_view
    {
        auto const out = rest;
        rest = {};
        return out;
    }
};

// Value of one `Key=value` pair. Quoted values keep their backslash escapes
// because the DN inside uses the same escaping. nullopt on an unterminated
// quote.
inline auto readPairValue(Cursor & cur) -> std::optional<std::string>
{
    std::string out;
    if (cur.peek() != '"') {
        auto plain = cur.until(";,");
        out = plain ? std::string(*plain) : std::string(cur.remaining());
        return out;
    }
    cur.take();
    while (true) {
        if (cur.done()) {
            return std::nullopt;
        }
        char const chr = cur.take();
        if (chr == '"') {
            return out;
        }
        out += chr;
        if (chr == '\\') {
            if (cur.done()) {
                return std::nullopt;
            }
            out += cur.take();
        }
    }
}

// CN attribute of an RFC 4514 DN ("CN=a\,b,O=x" -> "a,b"). Also accepts
// the legacy "/CN=x/O=y" form. nullopt if there is no CN.
inline auto commonNameOfDn(std::string_view name) -> std::optional<std::string>
{
    Cursor cur(name);
    if (cur.peek() == '/') {
        cur.take();
    }
    while (!cur.done()) {
        std::string attr;
        while (!cur.done() && cur.peek() != ',' && cur.peek() != '/') {
            char const chr = cur.take();
            if (chr == '\\' && !cur.done()) {
                attr += cur.take();
            } else {
                attr += chr;
            }
        }
        cur.take(); // separator or end
        std::string_view const view = attr;
        if (view.starts_with("CN=") || view.starts_with("cn=")) {
            return std::string(view.substr(3));
        }
    }
    return std::nullopt;
}

inline auto saneCommonName(const std::string & name) -> bool
{
    if (name.empty() || name.size() > maxCommonNameBytes) {
        return false;
    }
    constexpr unsigned char space = 0x20;
    constexpr unsigned char del = 0x7f;
    return std::ranges::all_of(name, [](unsigned char chr) -> bool { return chr >= space && chr != del; });
}

} // namespace detail

// Identity in the first (client-adjacent) element, e.g.
//   Hash=..;Subject="CN=ci-1,O=nt";URI=spiffe://nt/ci,By=..
//     ->  CN "ci-1", SPIFFE ID "spiffe://nt/ci"
// Envoy sends URI only with set_current_client_cert_details.uri, and one
// pair per URI SAN: as for a direct peer, the SPIFFE ID counts only when it
// is the single URI. By is the proxy's own URI and never the client's.
// No certificate (an empty Peer) when the element is malformed, oversized,
// has neither a CN nor a SPIFFE ID, or its CN holds control characters.
inline auto elementPeer(std::string_view value) -> Peer
{
    if (value.empty() || value.size() > maxHeaderBytes) {
        return {};
    }
    std::optional<std::string> subject;
    std::vector<std::string> uris;
    detail::Cursor cur(value);
    while (!cur.done()) {
        auto key = cur.until("=");
        if (!key) {
            return {};
        }
        cur.take();
        auto val = detail::readPairValue(cur);
        if (!val) {
            return {};
        }
        if (*key == "Subject") {
            if (subject) {
                return {};
            }
            subject = std::move(*val);
        } else if (*key == "URI") {
            uris.push_back(std::move(*val));
        }
        if (cur.peek() == ',') {
            break; // the next hop's element
        }
        if (!cur.done() && cur.take() != ';') {
            return {};
        }
    }
    Peer peer;
    if (uris.size() == 1 && saneSpiffeId(uris.front())) {
        peer.spiffeId = std::move(uris.front());
    }
    auto name = subject ? detail::commonNameOfDn(*subject) : std::nullopt;
    if (name && !detail::saneCommonName(*name)) {
        return {};
    }
    if (!name && !peer.spiffeId) {
        return {};
    }
    peer.commonName = name.value_or("");
    return peer;
}

// No certificate unless exactly one header is present and parses.
inline auto forwardedPeer(const grpc::ServerContextBase & context) -> Peer
{
    auto const & metadata = context.client_metadata();
    auto [first, last] = metadata.equal_range(std::string(header));
    if (first == last || std::next(first) != last) {
        return {};
    }
    return elementPeer({first->second.data(), first->second.size()});
}

class TrustedProxies
{
    std::vector<std::string> patterns;

public:
    void add(std::string_view pattern)
    {
        if (pattern.empty()) {
            throw std::invalid_argument("--trusted-proxy: empty pattern");
        }
        patterns.emplace_back(pattern);
    }

    [[nodiscard]] auto empty() const -> bool
    {
        return patterns.empty();
    }

    [[nodiscard]] auto matches(const Peer & peer) const -> bool
    {
        return std::ranges::any_of(patterns, [&](const std::string & pattern) -> bool {
            if (!isSpiffePattern(pattern) && !(peer.commonName && detail::saneCommonName(*peer.commonName))) {
                return false;
            }
            return matchesPeer(pattern, peer);
        });
    }
};

} // namespace nixgrpc::xfcc
