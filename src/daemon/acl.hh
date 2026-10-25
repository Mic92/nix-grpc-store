#pragma once
// Maps TLS client certificates to roles via ordered glob rules on the CN or
// the SPIFFE ID (first match wins, no match denies). Without rules every
// client keeps full access, preserving the pre-ACL behavior.

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nix/util/error.hh>

#include "peer.hh"

namespace nixgrpc {

// Capabilities are strictly nested, so the order is significant: an RPC is
// allowed iff the client's role >= the RPC's minimum role.
enum class Role : std::uint8_t {
    readOnly, // path queries and NAR downloads
    write,    // + signed imports and builds
    trusted,  // + worker-protocol tunnel and unsigned imports
};

constexpr std::array<std::pair<std::string_view, Role>, 3> roleNames{
    {{"read-only", Role::readOnly}, {"write", Role::write}, {"trusted", Role::trusted}}};

inline auto roleName(Role role) -> std::string_view
{
    for (auto [name, value] : roleNames) {
        if (value == role) {
            return name;
        }
    }
    return "unknown";
}

inline auto parseRole(std::string_view name) -> Role
{
    for (auto [candidate, role] : roleNames) {
        if (name == candidate) {
            return role;
        }
    }
    throw nix::Error("unknown role '%s' (expected read-only, write or trusted)", std::string(name));
}

class Acl
{
    struct Rule
    {
        std::string pattern;
        Role role;
    };

    std::vector<Rule> rules;
    // Separate from the glob rules so '*' cannot grant anonymous access.
    std::optional<Role> anonRole;
    bool certRequired = false;

public:
    [[nodiscard]] auto active() const -> bool
    {
        return !rules.empty();
    }

    void addRule(std::string_view spec)
    {
        auto const sep = spec.rfind('=');
        if (sep == std::string_view::npos || sep == 0) {
            throw nix::Error("--allow must be 'pattern=role', got '%s'", std::string(spec));
        }
        rules.push_back({.pattern = std::string(spec.substr(0, sep)), .role = parseRole(spec.substr(sep + 1))});
    }

    void allowAnonymous(Role role)
    {
        anonRole = role;
    }

    [[nodiscard]] auto anonymousRole() const -> std::optional<Role>
    {
        return anonRole;
    }

    void requireCertificate()
    {
        certRequired = true;
    }

    struct Access
    {
        // nullopt means access denied.
        std::optional<Role> role;
        // The name the matching rule read, so a log line names what was
        // authorized and not another field of the same certificate.
        std::string name;
    };

    [[nodiscard]] auto accessFor(const Peer & peer) const -> Access
    {
        if (!peer.present()) {
            if (anonRole) {
                return {.role = anonRole, .name = peer.label()};
            }
            return {
                .role = (certRequired || !rules.empty()) ? std::nullopt : std::optional(Role::trusted),
                .name = peer.label()};
        }
        if (rules.empty()) {
            return {.role = Role::trusted, .name = peer.label()};
        }
        for (const auto & rule : rules) {
            auto const & name = patternField(rule.pattern, peer);
            if (name && matchesName(rule.pattern, *name)) {
                return {.role = rule.role, .name = *name};
            }
        }
        return {.role = std::nullopt, .name = peer.label()};
    }

    [[nodiscard]] auto roleFor(const Peer & peer) const -> std::optional<Role>
    {
        return accessFor(peer).role;
    }
};

} // namespace nixgrpc
