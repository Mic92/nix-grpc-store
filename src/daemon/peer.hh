#pragma once
// What a TLS peer's certificate says about it, and the glob match the access
// rules and trusted proxies share.

#include <fnmatch.h>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace nixgrpc {

constexpr std::string_view spiffeScheme = "spiffe://";
constexpr size_t maxSpiffeIdBytes = 2048; // SPIFFE-ID spec, section 2.3

struct Peer
{
    // nullopt: no certificate. Empty: a certificate without a CN, as SPIRE
    // issues them.
    std::optional<std::string> commonName;
    // The certificate's only URI SAN, when it is a SPIFFE ID.
    std::optional<std::string> spiffeId;

    [[nodiscard]] auto present() const -> bool
    {
        return commonName.has_value();
    }

    // For logs and metrics when no rule decided which name counts.
    [[nodiscard]] auto label() const -> std::string
    {
        if (!commonName) {
            return "-";
        }
        if (commonName->empty() && spiffeId) {
            return *spiffeId;
        }
        return *commonName;
    }
};

// Trust domain and path charset of the SPIFFE-ID spec, without its finer
// rules (no empty or dot segments): enough that an ID is safe to log and
// cannot smuggle a glob or a NUL into fnmatch.
inline auto saneSpiffeId(std::string_view spiffeId) -> bool
{
    if (!spiffeId.starts_with(spiffeScheme) || spiffeId.size() > maxSpiffeIdBytes) {
        return false;
    }
    constexpr std::string_view domainChars = "abcdefghijklmnopqrstuvwxyz0123456789.-_";
    constexpr std::string_view pathChars = "abcdefghijklmnopqrstuvwxyz0123456789.-_ABCDEFGHIJKLMNOPQRSTUVWXYZ/";
    auto const rest = spiffeId.substr(spiffeScheme.size());
    auto const domain = rest.substr(0, rest.find('/'));
    auto const path = rest.substr(domain.size());
    return !domain.empty() && domain.find_first_not_of(domainChars) == std::string_view::npos
           && path.find_first_not_of(pathChars) == std::string_view::npos;
}

inline auto isSpiffePattern(std::string_view pattern) -> bool
{
    return pattern.starts_with(spiffeScheme);
}

// A pattern that starts with spiffe:// is matched against the SPIFFE ID and
// nothing else, any other pattern against the CN and nothing else: a CN that
// spells a SPIFFE ID gains nothing. In a SPIFFE pattern '*' stays inside one
// path segment, so it can cross neither into another trust domain nor into a
// deeper path.
inline auto patternField(std::string_view pattern, const Peer & peer) -> const std::optional<std::string> &
{
    return isSpiffePattern(pattern) ? peer.spiffeId : peer.commonName;
}

inline auto matchesName(const std::string & pattern, const std::string & name) -> bool
{
    // fnmatch truncates at NUL; a CN like "ci-1\0evil" must not match
    // "ci-*".
    if (name.contains('\0')) {
        return false;
    }
    return fnmatch(pattern.c_str(), name.c_str(), isSpiffePattern(pattern) ? FNM_PATHNAME : 0) == 0;
}

inline auto matchesPeer(const std::string & pattern, const Peer & peer) -> bool
{
    auto const & name = patternField(pattern, peer);
    return name && matchesName(pattern, *name);
}

} // namespace nixgrpc
