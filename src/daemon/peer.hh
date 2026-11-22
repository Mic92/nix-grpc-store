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

// Whether the SPIFFE-ID spec allows it. An ID that passes holds no glob
// character and no NUL, and is safe to log.
inline auto saneSpiffeId(std::string_view spiffeId) -> bool
{
    constexpr size_t maxTrustDomainBytes = 255;
    constexpr std::string_view domainChars = "abcdefghijklmnopqrstuvwxyz0123456789.-_";
    constexpr std::string_view segmentChars = "abcdefghijklmnopqrstuvwxyz0123456789.-_ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    if (!spiffeId.starts_with(spiffeScheme) || spiffeId.size() > maxSpiffeIdBytes) {
        return false;
    }
    auto rest = spiffeId.substr(spiffeScheme.size());
    auto const domain = rest.substr(0, rest.find('/'));
    if (domain.empty() || domain.size() > maxTrustDomainBytes
        || domain.find_first_not_of(domainChars) != std::string_view::npos) {
        return false;
    }
    rest.remove_prefix(domain.size());
    while (!rest.empty()) {
        rest.remove_prefix(1); // the '/'
        auto const segment = rest.substr(0, rest.find('/'));
        if (segment.empty() || segment == "." || segment == ".."
            || segment.find_first_not_of(segmentChars) != std::string_view::npos) {
            return false;
        }
        rest.remove_prefix(segment.size());
    }
    return true;
}

// The last path segment of a SPIFFE ID the spec allows, if it has a path.
inline auto spiffeLastSegment(std::string_view spiffeId) -> std::optional<std::string_view>
{
    auto const slash = spiffeId.rfind('/');
    if (!saneSpiffeId(spiffeId) || slash < spiffeScheme.size()) {
        return std::nullopt;
    }
    return spiffeId.substr(slash + 1);
}

// A glob from --allow or --trusted-proxy. One that starts with spiffe:// is
// matched against the SPIFFE ID and nothing else, any other against the CN
// and nothing else: a CN that spells a SPIFFE ID gains nothing. In a SPIFFE
// pattern '*' stays inside one path segment, so it can cross neither into
// another trust domain nor into a deeper path.
class NamePattern
{
    std::string glob;
    bool spiffe;

public:
    explicit NamePattern(std::string_view pattern)
        : glob(pattern)
        , spiffe(pattern.starts_with(spiffeScheme))
    {
    }

    [[nodiscard]] auto readsSpiffeId() const -> bool
    {
        return spiffe;
    }

    // The name that matched, or nullptr.
    [[nodiscard]] auto match(const Peer & peer) const -> const std::string *
    {
        auto const & name = spiffe ? peer.spiffeId : peer.commonName;
        // fnmatch truncates at NUL: a CN like "ci-1\0evil" must not match
        // "ci-*".
        if (!name || name->contains('\0') || fnmatch(glob.c_str(), name->c_str(), spiffe ? FNM_PATHNAME : 0) != 0) {
            return nullptr;
        }
        return &*name;
    }
};

} // namespace nixgrpc
