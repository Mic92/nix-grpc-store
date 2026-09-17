#pragma once
// Who is calling (client certificate or forwarded certificate) and whether
// the ACL lets them call a given method.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "acl.hh"
#include "xfcc.hh"

namespace nixgrpc {

struct Caller
{
    enum class Kind : std::uint8_t { anonymous, named };
    std::string name = "-"; // cert CN, for logs and metrics
    std::optional<Role> role;
    Kind kind = Kind::anonymous;
};

struct Auth
{
    Acl acl;
    xfcc::TrustedProxies proxies;

    [[nodiscard]] auto identify(const grpc::ServerContext & context) const -> Caller;
    static auto authorize(const Caller & caller, std::string_view method, Role minRole) -> grpc::Status;
    // Repair rewrites existing store paths.
    static auto authorize(const Caller & caller, std::string_view method, Role minRole, uint32_t buildMode)
        -> grpc::Status;

};

} // namespace nixgrpc
