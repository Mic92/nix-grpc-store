// Acl::roleFor over every way the daemon can be configured, and rules on
// the CN and the SPIFFE ID.

#include <array>
#include <cassert>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

#include "acl.hh"
#include "peer.hh"

namespace {

using nixgrpc::Acl;
using nixgrpc::Peer;
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

void configurations()
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
        assert(acl(row.cfg).roleFor(Peer{.commonName = row.cn}) == row.role);
    }
}

auto svid(std::string spiffeId) -> Peer
{
    return {.commonName = "", .spiffeId = std::move(spiffeId)};
}

void spiffeRules()
{
    Acl acl;
    assert(acl.roleFor(Peer{}) == Role::write);
    acl.addRule("spiffe://nt.example/farm/client/*=trusted");
    acl.addRule("spiffe://nt.example/mirror=read-only");
    acl.addRule("ci-*=write");

    assert(acl.roleFor(Peer{}) == std::nullopt);
    assert(acl.roleFor(Peer{.commonName = "ci-1"}) == Role::write);
    assert(acl.roleFor(svid("spiffe://nt.example/farm/client/bld1")) == Role::trusted);
    assert(acl.roleFor(svid("spiffe://nt.example/mirror")) == Role::readOnly);
    // '*' is one path segment: neither deeper nor another trust domain.
    assert(acl.roleFor(svid("spiffe://nt.example/farm/client/a/b")) == std::nullopt);
    assert(acl.roleFor(svid("spiffe://other.example/farm/client/bld1")) == std::nullopt);
    assert(acl.roleFor(svid("spiffe://nt.example.evil.example/farm/client/bld1")) == std::nullopt);
    // Each kind of rule reads its own field only.
    assert(acl.roleFor(Peer{.commonName = "spiffe://nt.example/farm/client/bld1"}) == std::nullopt);
    assert(acl.roleFor(Peer{.commonName = "x", .spiffeId = "ci-1"}) == std::nullopt);
    assert(acl.roleFor(svid(std::string("spiffe://nt.example/mirror") + '\0' + "x")) == std::nullopt);

    // The log name is the field the rule read.
    auto const both = Peer{.commonName = "ci-1", .spiffeId = "spiffe://nt.example/farm/client/bld1"};
    assert(acl.accessFor(both).name == "spiffe://nt.example/farm/client/bld1");
    assert(acl.accessFor(Peer{.commonName = "ci-1", .spiffeId = "spiffe://nt.example/x"}).name == "ci-1");
    assert(acl.accessFor(svid("spiffe://nt.example/x")).name == "spiffe://nt.example/x");

    {
        Acl domain;
        domain.addRule("spiffe://*/farm=write");
        assert(domain.roleFor(svid("spiffe://a.example/farm")) == Role::write);
        assert(domain.roleFor(svid("spiffe://a.example/x/farm")) == std::nullopt);
    }
}

} // namespace

auto main() -> int
{
    try {
        configurations();
        spiffeRules();
    } catch (std::exception & err) {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
    return 0;
}
