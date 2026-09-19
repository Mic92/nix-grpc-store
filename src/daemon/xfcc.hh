#pragma once
// Client identity forwarded by a TLS-terminating proxy (envoy's
// x-forwarded-client-cert). Only believed from a peer whose own certificate
// matches --trusted-proxy, so the proxy must use SANITIZE_SET semantics.

#include <fnmatch.h>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/server_context.h>

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
        auto out = rest;
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

// CN of the first (client-adjacent) element's Subject, e.g.
//   Hash=..;Subject="CN=ci-1,O=nt";URI=..,By=..   ->  "ci-1"
// nullopt when malformed, oversized, without Subject/CN, or when the CN is
// empty or holds control characters.
inline auto subjectCommonName(std::string_view value) -> std::optional<std::string>
{
    if (value.empty() || value.size() > maxHeaderBytes) {
        return std::nullopt;
    }
    detail::Cursor cur(value);
    while (!cur.done()) {
        auto key = cur.until("=");
        if (!key) {
            return std::nullopt;
        }
        cur.take();
        auto val = detail::readPairValue(cur);
        if (!val) {
            return std::nullopt;
        }
        if (*key == "Subject") {
            auto name = detail::commonNameOfDn(*val);
            if (!name || !detail::saneCommonName(*name)) {
                return std::nullopt;
            }
            return name;
        }
        if (cur.peek() != ';') {
            return std::nullopt; // ',' starts the next hop's element, or junk
        }
        cur.take();
    }
    return std::nullopt;
}

// nullopt unless exactly one header is present and parses.
inline auto forwardedCommonName(const grpc::ServerContextBase & context) -> std::optional<std::string>
{
    auto const & metadata = context.client_metadata();
    auto [first, last] = metadata.equal_range(std::string(header));
    if (first == last || std::next(first) != last) {
        return std::nullopt;
    }
    return subjectCommonName({first->second.data(), first->second.size()});
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

    [[nodiscard]] auto matches(const std::optional<std::string> & peerCommonName) const -> bool
    {
        if (!peerCommonName || !detail::saneCommonName(*peerCommonName)) {
            return false;
        }
        return std::ranges::any_of(patterns, [&](const std::string & pattern) -> bool {
            return fnmatch(pattern.c_str(), peerCommonName->c_str(), 0) == 0;
        });
    }
};

} // namespace nixgrpc::xfcc
