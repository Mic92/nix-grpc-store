// Who is calling and whether the ACL lets them.

#include "auth.hh"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <nix/store/store-api.hh>

#include "acl.hh"
#include "logfmt.hh"

namespace nixgrpc {

auto Auth::identify(const grpc::ServerContext & context) const -> Caller
{
    auto cert = clientCommonName(context);
    return {
        .name = cert.value_or("-"), .role = acl.roleFor(cert), .kind = cert ? Caller::Kind::named : Caller::Kind::anonymous};
}


auto Auth::authorize(const Caller & caller, std::string_view method, Role minRole) -> grpc::Status
{
    auto const & role = caller.role;
    if (role && *role >= minRole) {
        return grpc::Status::OK;
    }
    logLine(
        LogLevel::info,
        {{"event", "denied"},
         {"method", std::string(method)},
         {"cn", caller.name},
         {"role", role ? std::string(roleName(*role)) : "none"}});
    if (role) {
        return {
            grpc::StatusCode::PERMISSION_DENIED,
            "role '" + std::string(roleName(*role)) + "' may not call " + std::string(method)};
    }
    if (caller.kind == Caller::Kind::anonymous) {
        return {grpc::StatusCode::UNAUTHENTICATED, "server requires a TLS client certificate"};
    }
    return {grpc::StatusCode::PERMISSION_DENIED, "no access rule matches certificate CN '" + caller.name + "'"};
}

auto Auth::authorize(const Caller & caller, std::string_view method, Role minRole, uint32_t buildMode) -> grpc::Status
{
    auto status = authorize(caller, method, minRole);
    if (status.ok() && buildMode == static_cast<uint32_t>(nix::bmRepair)) {
        status = authorize(caller, std::string(method) + "(repair)", Role::trusted);
    }
    return status;
}

} // namespace nixgrpc
