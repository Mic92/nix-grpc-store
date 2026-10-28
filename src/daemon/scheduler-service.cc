#include "coordinator.hh"

#include <chrono>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/server_context.h>

#include "auth.hh"
#include "dispatcher.hh"
#include "logfmt.hh"
#include "sched-reactor.hh"
#include "worker-identity.hh"

namespace nixgrpc {

SchedulerService::SchedulerService(Dispatcher & dispatcher, Auth & auth, IdleTracker & idle)
    : dispatcher(&dispatcher)
    , auth(&auth)
    , idle(&idle)
{
}

auto SchedulerService::Schedule(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<nix::remote::ClientMsgs, nix::remote::SchedMsgs> *
{
    if (!active_) {
        return new RejectReactor<nix::remote::ClientMsgs, nix::remote::SchedMsgs>(passive()); // NOLINT(cppcoreguidelines-owning-memory)
    }
    auto const caller = auth->identify(*context);
    if (auto const status = Auth::authorize(caller, "Schedule", Role::write); !status.ok()) {
        return new RejectReactor<nix::remote::ClientMsgs, nix::remote::SchedMsgs>(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    return new ScheduleReactor(context, *dispatcher, *idle); // NOLINT(cppcoreguidelines-owning-memory)
}

auto SchedulerService::WorkerSession(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<nix::remote::WorkerMsgs, nix::remote::SchedCmds> *
{
    using nix::remote::WorkerMsgs;
    using nix::remote::SchedCmds;
    if (!active_) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(passive()); // NOLINT(cppcoreguidelines-owning-memory)
    }
    std::string verifiedPodId;
    std::chrono::system_clock::time_point credentialExpiresAt{};
    if (reverseIdentity) {
        auto const tls = context->auth_context();
        if (tls) {
            auto const transports = tls->FindPropertyValues(GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME);
            auto const & metadata = context->client_metadata();
            auto [first, last] = metadata.equal_range("authorization");
            constexpr std::string_view prefix = "Bearer ";
            if (transports.size() == 1 &&
                (std::string_view(transports.front().data(), transports.front().size()) == "ssl" ||
                 std::string_view(transports.front().data(), transports.front().size()) == "tls")
                && first != last && std::next(first) == last) {
                std::string_view token(first->second.data(), first->second.size());
                if (token.starts_with(prefix)) {
                    if (auto bound = reverseIdentity->verify(std::string(token.substr(prefix.size())))) {
                        verifiedPodId = std::move(bound->workerId);
                        credentialExpiresAt = bound->expiresAt;
                    }
                }
            }
        }
        // Existing direct workers may use a different, ordinary scheduler
        // bearer. Only an exclusively verified tunnel token can bind a pod;
        // the normal Auth path still decides whether a direct worker may
        // connect, and its Hello cannot claim a reverse_worker_id.
    }
    // The tunnel audience is exclusive and not in client Auth. For reverse
    // control sessions, independently authorize the TLS certificate; a token
    // must not itself grant WorkerSession permission.
    auto caller = verifiedPodId.empty() ? auth->identify(*context) : auth->identifyCertificate(*context);
    if (!verifiedPodId.empty() && caller.kind != Caller::Kind::named) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(
            {grpc::StatusCode::UNAUTHENTICATED, "reverse control session requires a TLS client certificate"});
    }
    if (auto const status = Auth::authorize(caller, "WorkerSession", Role::trusted); !status.ok()) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    logLine(LogLevel::info, {{"event", "worker_session_open"}, {"cn", caller.name}, {"peer", context->peer()}});
    WorkerSessionReactor::Renew renew;
    if (!verifiedPodId.empty()) {
        renew = [identity = reverseIdentity, podId = verifiedPodId](const std::string & token) {
            return identity->renew(token, podId);
        };
    }
    return new WorkerSessionReactor( // NOLINT(cppcoreguidelines-owning-memory)
        context, *dispatcher, *idle, std::move(verifiedPodId), credentialExpiresAt, std::move(renew));
}

} // namespace nixgrpc
