#pragma once
// A worker data session cannot identify itself by Hello, a shared TLS CN, or
// the Kubernetes SA subject: replicas share those. Bind it to the pod UID in
// a signature-verified, pod-bound projected service-account JWT instead.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "oidc.hh"

namespace nixgrpc {

class WorkerIdentity
{
    oidc::Verifier & verifier; // configured with a tunnel-only audience
    std::string provider;
    std::string nameSpace;
    std::string serviceAccount;
    std::string audience;
    std::chrono::seconds maxLifetime;

    static auto safeName(std::string_view value, size_t limit) -> bool
    {
        return !value.empty() && value.size() <= limit && std::ranges::all_of(value, [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.';
        });
    }

    static auto validUid(std::string_view uid) -> bool
    {
        // Kubernetes pod metadata.uid is a UUID; reject ambiguous or
        // unbounded identifiers before they can become routing keys.
        constexpr size_t uuidLength = 36;
        if (uid.size() != uuidLength) {
            return false;
        }
        for (size_t i = 0; i < uid.size(); ++i) {
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (uid[i] != '-') {
                    return false;
                }
            } else if (!((uid[i] >= '0' && uid[i] <= '9') || (uid[i] >= 'a' && uid[i] <= 'f'))) {
                return false;
            }
        }
        return true;
    }

public:
    struct Bound
    {
        std::string workerId;
        std::chrono::system_clock::time_point expiresAt;
    };

    // Kubernetes' minimum projected-token lifetime. Offline verification
    // cannot see pod deletion or token revocation, so this bounds how long a
    // deleted pod's stolen token can keep (or open) a session.
    static constexpr std::chrono::seconds defaultMaxLifetime{600};

    WorkerIdentity(
        oidc::Verifier & verifier, std::string provider, std::string nameSpace, std::string serviceAccount,
        std::string audience, std::chrono::seconds maxLifetime = defaultMaxLifetime)
        : verifier(verifier)
        , provider(std::move(provider))
        , nameSpace(std::move(nameSpace))
        , serviceAccount(std::move(serviceAccount))
        , audience(std::move(audience))
        , maxLifetime(maxLifetime)
    {
    }

    // The token must come from the registration RPC's Authorization header;
    // never accept the claimed worker ID as proof. A separate verifier with
    // an exclusive tunnel audience prevents a normal client token from
    // registering a worker, even if that client has the trusted ACL role.
    // Verify again on reconnect and before the token expires; a live stream
    // must not outlast the credential that authenticated it.
    auto verify(const std::string & token) -> std::optional<Bound>
    {
        constexpr size_t maxNamespaceLength = 63;
        constexpr size_t maxDnsNameLength = 253;
        if (!safeName(nameSpace, maxNamespaceLength) || !safeName(serviceAccount, maxDnsNameLength)
            || audience.empty()) {
            return std::nullopt;
        }
        auto result = verifier.verify(token);
        if (!result.identity || result.identity->role != Role::trusted) {
            return std::nullopt;
        }
        auto const & id = *result.identity;
        if (id.singleAudience != audience
            || id.subject != "oidc:" + provider + ":system:serviceaccount:" + nameSpace + ":" + serviceAccount
            || id.podNamespace != nameSpace || id.serviceAccountName != serviceAccount
            || !id.podName || !safeName(*id.podName, maxDnsNameLength) || !id.podUid || !validUid(*id.podUid)
            || !id.expiresAt || *id.expiresAt <= std::chrono::system_clock::now()) {
            return std::nullopt;
        }
        // Both claims are signed. Bound the issued lifetime and, in case the
        // signer back-dates iat, the remaining lifetime as seen locally.
        auto const skew = std::chrono::seconds(oidc::clockSkewSecs);
        if (maxLifetime.count() <= 0 || !id.issuedAt || *id.expiresAt - *id.issuedAt > maxLifetime
            || *id.expiresAt > std::chrono::system_clock::now() + maxLifetime + skew) {
            return std::nullopt;
        }
        auto const boundId = "pod:" + nameSpace + ":" + *id.podUid;
        return Bound{.workerId = boundId, .expiresAt = *id.expiresAt};
    }

    auto bind(const std::string & token, std::string_view claimedId) -> std::optional<Bound>
    {
        auto bound = verify(token);
        return bound && bound->workerId == claimedId ? bound : std::nullopt;
    }

    // A rotated token may only extend a session for the pod it was opened
    // for; it never changes identity.
    auto renew(const std::string & token, std::string_view boundId) -> std::optional<std::chrono::system_clock::time_point>
    {
        auto bound = verify(token);
        return bound && bound->workerId == boundId ? std::optional(bound->expiresAt) : std::nullopt;
    }
};

} // namespace nixgrpc
