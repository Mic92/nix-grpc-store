#pragma once
// Who is calling (client certificate, forwarded certificate or OIDC bearer
// token) and whether the ACL lets them call a given method.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "acl.hh"
#include "oidc.hh"
#include "xfcc.hh"

namespace nixgrpc {

class Metrics;

struct Caller
{
    enum class Kind : std::uint8_t { anonymous, named, badToken };
    std::string name = "-"; // cert CN, SPIFFE ID or oidc:<provider>:<sub>, for logs and metrics
    std::optional<Role> role;
    Kind kind = Kind::anonymous;
};

struct Auth
{
    Acl acl;
    xfcc::TrustedProxies proxies;
    std::optional<oidc::Verifier> * oidc = nullptr;
    Metrics * metrics = nullptr;

    [[nodiscard]] auto identify(const grpc::ServerContextBase & context) const -> Caller;
    // Identity from the peer's own certificate, for calls the balancer makes
    // for itself. Client requests go through identify().
    [[nodiscard]] auto identifyDirect(const grpc::ServerContextBase & context) const -> Caller;
    [[nodiscard]] auto authorize(const Caller & caller, std::string_view method, Role minRole) const -> grpc::Status;
    // Repair rewrites existing store paths.
    [[nodiscard]] auto authorize(const Caller & caller, std::string_view method, Role minRole, uint32_t buildMode) const
        -> grpc::Status;
};

} // namespace nixgrpc
