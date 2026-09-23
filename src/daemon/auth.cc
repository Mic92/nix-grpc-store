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
#include "oidc.hh"
#include "xfcc.hh"

namespace nixgrpc {

// A client certificate with an access rule first, then bearer token, then
// the certificate anyway (for the error), then anonymous. A trusted proxy's
// own certificate stands in for whatever client it forwards.
auto Auth::identify(const grpc::ServerContextBase & context) const -> Caller
{
    auto cert = clientCommonName(context);
    if (proxies.matches(cert)) {
        cert = xfcc::forwardedCommonName(context);
    }
    auto const certKnown = cert && acl.roleFor(cert);
    auto token = certKnown || !*oidc ? std::nullopt : oidc::bearerToken(context);
    if (!token) {
        return {
            .name = cert.value_or("-"),
            .role = acl.roleFor(cert),
            .kind = cert ? Caller::Kind::named : Caller::Kind::anonymous};
    }
    auto res = (*oidc)->verify(*token);
    if (!res.identity) {
        // Details stay in our log. The client learns nothing about providers or keys.
        logLine(
            LogLevel::info, {{"event", "oidc_rejected"}, {"error", res.error}, {"peer", context.peer()}});
        return {.kind = Caller::Kind::badToken};
    }
    return {.name = res.identity->subject, .role = res.identity->role, .kind = Caller::Kind::named};
}


auto Auth::identifyDirect(const grpc::ServerContextBase & context) const -> Caller
{
    auto const cert = clientCommonName(context);
    return {
        .name = cert.value_or("-"),
        .role = acl.roleFor(cert),
        .kind = cert ? Caller::Kind::named : Caller::Kind::anonymous};
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
    switch (caller.kind) {
    case Caller::Kind::anonymous:
        return {grpc::StatusCode::UNAUTHENTICATED, "server requires a TLS client certificate or bearer token"};
    case Caller::Kind::badToken:
        return {grpc::StatusCode::UNAUTHENTICATED, "bearer token rejected"};
    case Caller::Kind::named:
        break;
    }
    return {grpc::StatusCode::PERMISSION_DENIED, "no access rule matches '" + caller.name + "'"};
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
