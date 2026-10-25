#include <cassert>
#include <optional>
#include <string>
#include <utility>

#include "acl.hh"
#include "peer.hh"

using nixgrpc::Acl;
using nixgrpc::Peer;
using nixgrpc::Role;

namespace {

auto svid(std::string spiffeId) -> Peer
{
    return {.commonName = "", .spiffeId = std::move(spiffeId)};
}

} // namespace

auto main() -> int
try {
    Acl acl;
    assert(acl.roleFor(Peer{}) == Role::trusted);
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
    return 0;
} catch (...) {
    return 1;
}
