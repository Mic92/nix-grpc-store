// Acl::roleFor over every way the daemon can be configured.

#include <array>
#include <cassert>
#include <exception>
#include <iostream>
#include <optional>
#include <string>

#include "acl.hh"

namespace {

using nixgrpc::Acl;
using nixgrpc::Role;

struct Config
{
    bool authenticates = false; // --client-ca or --oidc-config
    bool trustClients = false;
    std::optional<Role> anonymous;
    const char * rule = nullptr;
};

auto acl(const Config & cfg) -> Acl
{
    Acl acl;
    if (cfg.authenticates) {
        acl.requireIdentity();
    }
    if (cfg.trustClients) {
        acl.trustClients();
    }
    if (cfg.anonymous) {
        acl.allowAnonymous(*cfg.anonymous);
    }
    if (cfg.rule != nullptr) {
        acl.addRule(cfg.rule);
    }
    return acl;
}

struct Row
{
    Config cfg;
    std::optional<std::string> cn;
    std::optional<Role> role;
};

void run()
{
    auto const rows = std::to_array<Row>({
    // No authentication configured: everyone is one anonymous caller.
    {.cfg = {}, .cn = std::nullopt, .role = Role::write},
    {.cfg = {.trustClients = true}, .cn = std::nullopt, .role = Role::trusted},
    // An authenticator is configured: callers without an identity are denied.
    {.cfg = {.authenticates = true}, .cn = std::nullopt, .role = std::nullopt},
    {.cfg = {.authenticates = true, .anonymous = Role::readOnly}, .cn = std::nullopt, .role = Role::readOnly},
    // Without rules, an identity is capped unless clients are trusted.
    {.cfg = {.authenticates = true}, .cn = "ci", .role = Role::write},
    {.cfg = {.authenticates = true, .trustClients = true}, .cn = "ci", .role = Role::trusted},
    // A rule naming trusted grants it explicitly.
    {.cfg = {.authenticates = true, .rule = "ci-*=trusted"}, .cn = "ci-1", .role = Role::trusted},
    {.cfg = {.authenticates = true, .rule = "ci-*=trusted"}, .cn = "dev", .role = std::nullopt},
    {.cfg = {.authenticates = true, .rule = "*=read-only"}, .cn = std::nullopt, .role = std::nullopt},
    });
    for (const auto & row : rows) {
        assert(acl(row.cfg).roleFor(row.cn) == row.role);
    }
}

} // namespace

auto main() -> int
{
    try {
        run();
    } catch (std::exception & err) {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
    return 0;
}
