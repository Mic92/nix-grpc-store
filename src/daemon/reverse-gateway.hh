#pragma once
// Gateway authorization boundary for client-facing reverse RPCs.
// Worker selection must come from a verified control/data-session binding,
// and build assignment from the active dispatcher, never x-nix-worker.

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "auth.hh"
#include "reverse-capability.hh"
#include "reverse-registry.hh"

namespace nixgrpc
{

class ReverseGateway
{
  public:
    // Must consult the active scheduler's current assignment, not simply
    // parse a request or trust the client's routing header. False fails shut.
    // workerId MUST be a pod UID authenticated on WorkerSession, bound to its
    // advertised address by the scheduler; an address or claimed UID alone is
    // not a safe key for ReverseRegistry.
    using Assignment = std::function<bool(std::string_view workerId, std::string_view drvPath, uint64_t assignId)>;

    struct Prepared
    {
        ReverseRegistry::Handle session;
        nix::remote::ReverseOpen open;
        std::string drvPath;
        uint64_t assignId = 0;
    };

    struct Result
    {
        grpc::Status status;
        std::optional<Prepared> prepared;
    };

    ReverseGateway(Auth &auth, ReverseRegistry &registry, Assignment assignment)
        : auth(auth), registry(registry), assignment(std::move(assignment))
    {
    }

    [[nodiscard]] auto prepare(const grpc::ServerContextBase &context, std::string_view selectedWorker,
                               uint64_t streamId, std::string_view method, std::string_view drvPath = {},
                               uint64_t assignId = 0, uint32_t buildMode = 0) const -> Result
    {
        auto const caller = auth.identify(context);
        // Do not consult the registry or issue a capability to an unauthorized
        // client. In particular, no ambient proxy or worker privilege applies.
        // The direct service historically permits anonymous calls with an
        // empty ACL; reverse delegation deliberately requires an identity.
        if (caller.kind != Caller::Kind::named)
        {
            return {{grpc::StatusCode::UNAUTHENTICATED, "reverse calls require an authenticated client"}, {}};
        }
        Role minRole;
        if (method == "Connect")
        {
            minRole = Role::trusted;
        }
        else if (method == "BuildDerivation" || method == "UploadBuildInputs" || method == "AddMultipleToStore")
        {
            minRole = Role::write;
        }
        else if (method == "QueryValidPaths" || method == "QueryPathInfos" || method == "FetchNars" ||
                 method == "QueryMissing" || method == "StoreInfo")
        {
            minRole = Role::readOnly;
        }
        else
        {
            return {{grpc::StatusCode::UNIMPLEMENTED, "unknown reverse method"}, {}};
        }
        if (auto const status = Auth::authorize(caller, method, minRole, buildMode); !status.ok())
        {
            return {status, {}};
        }
        if (method == "BuildDerivation" || method == "UploadBuildInputs")
        {
            if (method == "UploadBuildInputs" && buildMode != 0)
            {
                return {{grpc::StatusCode::INVALID_ARGUMENT, "upload does not accept a build mode"}, {}};
            }
            if (buildMode > static_cast<uint32_t>(nix::bmCheck))
            {
                return {{grpc::StatusCode::INVALID_ARGUMENT, "unsupported build mode"}, {}};
            }
            if (!assignment || drvPath.empty() || assignId == 0 || !assignment(selectedWorker, drvPath, assignId))
            {
                return {{grpc::StatusCode::FAILED_PRECONDITION, "no matching live assignment"}, {}};
            }
        }
        else if (!drvPath.empty() || assignId != 0 || buildMode != 0)
        {
            return {{grpc::StatusCode::INVALID_ARGUMENT, "unexpected build assignment"}, {}};
        }
        auto session = registry.find(selectedWorker);
        if (!registry.current(session) || !session->transport || session->sessionKey.size() != ReverseCapability::keyBytes)
        {
            return {{grpc::StatusCode::UNAVAILABLE, "selected worker session unavailable"}, {}};
        }
        auto token = ReverseCapability::mint(session->sessionKey, session->workerId, session->generation, streamId,
                                             method, caller, drvPath, assignId, buildMode);
        if (!token || !registry.current(session))
        {
            return {{grpc::StatusCode::UNAVAILABLE, "selected worker session changed"}, {}};
        }
        nix::remote::ReverseOpen open;
        open.set_method(method);
        open.set_capability(std::move(*token));
        // Still a snapshot. Before enqueueing an eventual RPC, check
        // registry.current(session) again and fence the transport on revoke.
        return {grpc::Status::OK, Prepared{std::move(session), std::move(open),
                                            std::string(drvPath), assignId}};
    }

    // Reserve the response before sending the open. The caller must choose a
    // monotonically increasing stream ID on this exact session; it must cancel
    // both reservations if request serialization or enqueue later fails.
    [[nodiscard]] auto enqueue(Prepared prepared, uint64_t streamId) const -> grpc::Status
    {
        auto const & session = prepared.session;
        if (!registry.current(session) || !session->transport)
        {
            return {grpc::StatusCode::UNAVAILABLE, "selected worker session changed"};
        }
        if ((prepared.open.method() == "BuildDerivation" || prepared.open.method() == "UploadBuildInputs") &&
            (!assignment || !assignment(session->workerId, prepared.drvPath, prepared.assignId)))
        {
            return {grpc::StatusCode::FAILED_PRECONDITION, "build assignment no longer live"};
        }
        // The token is re-verified here to prevent a caller from enqueuing a
        // Prepared value for a different stream, worker or registration.
        auto const verified = ReverseCapability::authorizeOpen(session->sessionKey, prepared.open, session->workerId,
                                                                session->generation, streamId);
        if (!verified || verified->drvPath != prepared.drvPath || verified->assignId != prepared.assignId)
        {
            return {grpc::StatusCode::PERMISSION_DENIED, "invalid prepared reverse open"};
        }
        auto & transport = *session->transport;
        if (!transport.responses.open(streamId))
        {
            return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse response stream unavailable"};
        }
        if (!registry.current(session) || !transport.outgoing.open(streamId, std::move(prepared.open)))
        {
            (void) transport.responses.cancel(streamId, false);
            return {grpc::StatusCode::UNAVAILABLE, "selected worker session changed or queue full"};
        }
        // Replacement closes the outgoing queue and cancels responses; the
        // writer also rechecks current(handle) immediately before Write.
        if (!registry.current(session))
        {
            (void) transport.outgoing.cancel(streamId);
            (void) transport.responses.cancel(streamId, false);
            return {grpc::StatusCode::UNAVAILABLE, "selected worker session displaced"};
        }
        return grpc::Status::OK;
    }

    // For builds, obtain the exact destination from the live scheduler. A
    // client-supplied worker header must never enter this lookup; returning no
    // pod fails closed rather than selecting some other registered worker.
    using ResolveBuild = std::function<std::optional<std::string>(std::string_view drvPath, uint64_t assignId)>;

    [[nodiscard]] auto prepareBuild(const grpc::ServerContextBase &context, const ResolveBuild &resolve,
                                    uint64_t streamId, std::string_view drvPath, uint64_t assignId,
                                    uint32_t buildMode) const -> Result
    {
        // Do not expose assignment existence to an unauthenticated client.
        // prepare() repeats authorization after resolving the pod, so the
        // selected worker never gains authority from a client-owned field.
        auto const caller = auth.identify(context);
        if (caller.kind != Caller::Kind::named)
        {
            return {{grpc::StatusCode::UNAUTHENTICATED, "reverse calls require an authenticated client"}, {}};
        }
        if (auto const status = Auth::authorize(caller, "BuildDerivation", Role::write, buildMode); !status.ok())
        {
            return {status, {}};
        }
        if (buildMode > static_cast<uint32_t>(nix::bmCheck))
        {
            return {{grpc::StatusCode::INVALID_ARGUMENT, "unsupported build mode"}, {}};
        }
        if (!resolve || drvPath.empty() || assignId == 0)
        {
            return {{grpc::StatusCode::FAILED_PRECONDITION, "no scheduler-owned build route"}, {}};
        }
        auto pod = resolve(drvPath, assignId);
        if (!pod || pod->empty())
        {
            return {{grpc::StatusCode::FAILED_PRECONDITION, "no live pod-bound build assignment"}, {}};
        }
        return prepare(context, *pod, streamId, "BuildDerivation", drvPath, assignId, buildMode);
    }

    // Upload selection never trusts client routing headers.
    [[nodiscard]] auto prepareBuildInputs(const grpc::ServerContextBase & context, const ResolveBuild & resolve,
                                          uint64_t streamId, std::string_view drvPath, uint64_t assignId) const -> Result
    {
        auto const caller = auth.identify(context);
        if (caller.kind != Caller::Kind::named)
        {
            return {{grpc::StatusCode::UNAUTHENTICATED, "reverse calls require an authenticated client"}, {}};
        }
        if (auto const status = Auth::authorize(caller, "UploadBuildInputs", Role::write); !status.ok())
        {
            return {status, {}};
        }
        if (!resolve || drvPath.empty() || assignId == 0)
        {
            return {{grpc::StatusCode::FAILED_PRECONDITION, "no scheduler-owned upload route"}, {}};
        }
        auto pod = resolve(drvPath, assignId);
        if (!pod || pod->empty())
        {
            return {{grpc::StatusCode::FAILED_PRECONDITION, "no live pod-bound upload assignment"}, {}};
        }
        return prepare(context, *pod, streamId, "UploadBuildInputs", drvPath, assignId);
    }

  private:
    Auth &auth;
    ReverseRegistry &registry;
    Assignment assignment;
};

} // namespace nixgrpc
