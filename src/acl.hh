#pragma once
// Maps TLS client certificate CNs to roles via ordered glob rules (first
// match wins, no match denies). Without rules every client keeps full
// access, preserving the pre-ACL behavior.

#include <fnmatch.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nix/util/error.hh>

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

public:
    [[nodiscard]] auto active() const -> bool
    {
        return !rules.empty();
    }

    void addRule(std::string_view spec)
    {
        auto const sep = spec.rfind('=');
        if (sep == std::string_view::npos || sep == 0) {
            throw nix::Error("--allow must be 'cn-pattern=role', got '%s'", std::string(spec));
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

    // nullopt means access denied.
    [[nodiscard]] auto roleFor(const std::optional<std::string> & commonName) const -> std::optional<Role>
    {
        if (!commonName) {
            if (anonRole) {
                return anonRole;
            }
            return rules.empty() ? std::optional(Role::trusted) : std::nullopt;
        }
        if (rules.empty()) {
            return Role::trusted;
        }
        // fnmatch truncates at NUL; a CN like "ci-1\0evil" must not match
        // "ci-*".
        if (commonName->contains('\0')) {
            return std::nullopt;
        }
        for (const auto & rule : rules) {
            if (fnmatch(rule.pattern.c_str(), commonName->c_str(), 0) == 0) {
                return rule.role;
            }
        }
        return std::nullopt;
    }
};

} // namespace nixgrpc
