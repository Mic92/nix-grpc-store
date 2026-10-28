#pragma once
// Per-session method-scoped caller delegation. Only the authenticated gateway
// and the matching TLS-connected worker know the HMAC key; never accept a
// capability from ordinary client metadata. No data dispatch uses this yet.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include <nix/store/store-api.hh>

#include "auth.hh"
#include "nix_remote.pb.h"

namespace nixgrpc
{

class ReverseCapability
{
  public:
    static constexpr size_t keyBytes = 32;
    static constexpr size_t macBytes = 32;
    static constexpr size_t maxPayloadBytes = 1024;
    // Bounds the time between authorization at the gateway and acceptance
    // of an Open. Once accepted, the call's lifetime is bounded separately
    // by its assignment/session and the operation's deadline/limits.
    static constexpr auto lifetime = std::chrono::seconds(30);
    // The gateway mints with its clock, the worker verifies with its own.
    // Without a margin a worker a few milliseconds behind sees more than
    // `lifetime` remaining and rejects a fresh capability.
    static constexpr auto clockSkew = std::chrono::seconds(5);

    static auto newKey() -> std::optional<std::string>
    {
        std::string key(keyBytes, '\0');
        if (RAND_bytes(reinterpret_cast<unsigned char *>(key.data()), static_cast<int>(key.size())) != 1)
        {
            return std::nullopt;
        }
        return key;
    }

    // A trusted gateway calls Auth::identify and authorize on the actual
    // inbound ServerContext before minting; this function does neither.
    // In particular, the gateway must refuse anonymous reverse delegation.
    static auto mint(std::string_view key, std::string_view workerId, uint64_t generation, uint64_t streamId,
                     std::string_view method, const Caller &caller, std::string_view drvPath = {},
                     uint64_t assignId = 0, uint32_t buildMode = 0,
                     std::chrono::system_clock::time_point now = std::chrono::system_clock::now())
        -> std::optional<std::string>
    {
        auto const required = requiredRole(method, buildMode, drvPath, assignId);
        if (key.size() != keyBytes || workerId.empty() || generation == 0 || streamId == 0 || !caller.role ||
            caller.kind != Caller::Kind::named || caller.name.empty() || caller.name.size() > 256 ||
            caller.name.contains('\0') || !required || *caller.role < *required)
        {
            return std::nullopt;
        }
        nix::remote::ReverseCapability payload;
        payload.set_worker_id(workerId);
        payload.set_generation(generation);
        payload.set_stream_id(streamId);
        payload.set_method(method);
        payload.set_caller_name(caller.name);
        payload.set_caller_role(static_cast<uint32_t>(*caller.role));
        payload.set_caller_kind(static_cast<uint32_t>(caller.kind));
        payload.set_expires_at_ms(
            std::chrono::duration_cast<std::chrono::milliseconds>((now + lifetime).time_since_epoch()).count());
        payload.set_drv_path(drvPath);
        payload.set_assign_id(assignId);
        payload.set_build_mode(buildMode);
        auto const wire = payload.SerializeAsString();
        if (wire.size() > maxPayloadBytes)
        {
            return std::nullopt;
        }
        auto mac = sign(key, wire);
        if (!mac)
        {
            return std::nullopt;
        }
        // Fixed-width little endian length, followed by protobuf and MAC.
        auto const size = static_cast<uint32_t>(wire.size());
        std::string result;
        result.reserve(4 + wire.size() + mac->size());
        for (unsigned i = 0; i < 4; ++i)
        {
            result.push_back(static_cast<char>(size >> (i * 8)));
        }
        result += wire;
        result += *mac;
        return result;
    }

    struct Verified
    {
        std::string method;
        Caller caller;
        std::string drvPath;
        uint64_t assignId = 0;
        uint32_t buildMode = 0;
    };

    // Verify the decoded request too, not only the method named by the
    // gateway. A relayed worker must call this after decoding and before
    // invoking its normal handler, then recheck Builder::admit for builds.
    static auto matchesBuild(const Verified &delegated, const nix::remote::BuildDerivationRequest &request) -> bool
    {
        return delegated.method == "BuildDerivation" && delegated.drvPath == request.drv_path() &&
               delegated.assignId == request.assign_id() &&
               delegated.buildMode == request.build_mode() && delegated.assignId != 0 && delegated.caller.role &&
               *delegated.caller.role >=
                   (request.build_mode() == static_cast<uint32_t>(nix::bmRepair) ? Role::trusted : Role::write);
    }

    // Bulk input uploads must be scoped to the same assignment as the build.
    // An unpinned AddMultipleToStore capability never authorizes uploading
    // inputs to a scheduler or an arbitrary remote worker.
    static auto matchesBuildInputs(const Verified &delegated, std::string_view drvPath, uint64_t assignId) -> bool
    {
        return delegated.method == "UploadBuildInputs" && !drvPath.empty() && delegated.drvPath == drvPath &&
            assignId != 0 && delegated.assignId == assignId
            && delegated.caller.role && *delegated.caller.role >= Role::write;
    }

    static auto permitsBuildInputUpload(const Verified &delegated, std::string_view drvPath, uint64_t assignId,
                                         const nix::remote::AddMultipleChunk & first) -> bool
    {
        return matchesBuildInputs(delegated, drvPath, assignId) && permitsImport(delegated, first);
    }

    static auto permitsImport(const Verified &delegated, const nix::remote::AddMultipleChunk &first) -> bool
    {
        // A write-role caller may import only signed, non-repair paths. The
        // existing handler must still force CheckSigs/NoRepair for that role.
        return delegated.caller.role &&
               (*delegated.caller.role == Role::trusted || (!first.repair() && first.check_sigs()));
    }

    static auto authorizeOpen(std::string_view key, const nix::remote::ReverseOpen &open, std::string_view workerId,
                              uint64_t generation, uint64_t streamId,
                              std::chrono::system_clock::time_point now = std::chrono::system_clock::now())
        -> std::optional<Verified>
    {
        return verify(key, open.capability(), workerId, generation, streamId, open.method(), now);
    }

    static auto verify(std::string_view key, std::string_view token, std::string_view workerId, uint64_t generation,
                       uint64_t streamId, std::string_view method,
                       std::chrono::system_clock::time_point now = std::chrono::system_clock::now())
        -> std::optional<Verified>
    {
        if (key.size() != keyBytes || token.size() < 4 + macBytes || token.size() > 4 + maxPayloadBytes + macBytes)
        {
            return std::nullopt;
        }
        uint32_t size = 0;
        for (unsigned i = 0; i < 4; ++i)
        {
            size |= static_cast<uint32_t>(static_cast<unsigned char>(token[i])) << (i * 8);
        }
        if (size > maxPayloadBytes || size != token.size() - 4 - macBytes)
        {
            return std::nullopt;
        }
        auto wire = token.substr(4, size);
        auto expected = sign(key, wire);
        if (!expected || CRYPTO_memcmp(expected->data(), token.data() + 4 + size, macBytes) != 0)
        {
            return std::nullopt;
        }
        nix::remote::ReverseCapability payload;
        if (!payload.ParseFromArray(wire.data(), static_cast<int>(wire.size())) || payload.worker_id() != workerId ||
            payload.generation() != generation || payload.stream_id() != streamId || streamId == 0 ||
            payload.method() != method || payload.caller_name().empty() || payload.caller_name().size() > 256 ||
            payload.caller_name().contains('\0') || payload.caller_role() > static_cast<uint32_t>(Role::trusted) ||
            payload.caller_kind() != static_cast<uint32_t>(Caller::Kind::named))
        {
            return std::nullopt;
        }
        auto const required = requiredRole(method, payload.build_mode(), payload.drv_path(), payload.assign_id());
        if (!required)
        {
            return std::nullopt;
        }
        auto const deadline = std::chrono::milliseconds(payload.expires_at_ms());
        auto const remaining = deadline - std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        if (remaining <= -std::chrono::milliseconds(clockSkew) || remaining > lifetime + clockSkew ||
            static_cast<Role>(payload.caller_role()) < *required)
        {
            return std::nullopt;
        }
        return Verified{.method = payload.method(), .caller = Caller{.name = payload.caller_name(),
                                         .role = static_cast<Role>(payload.caller_role()),
                                         .kind = static_cast<Caller::Kind>(payload.caller_kind())},
                        .drvPath = payload.drv_path(),
                        .assignId = payload.assign_id(),
                        .buildMode = payload.build_mode()};
    }

  private:
    static auto requiredRole(std::string_view method, uint32_t mode, std::string_view drv, uint64_t assign)
        -> std::optional<Role>
    {
        if (method == "UploadBuildInputs")
        {
            if (drv.empty() || assign == 0 || mode != 0) return std::nullopt;
            return Role::write;
        }
        if (method == "BuildDerivation")
        {
            if (drv.empty() || assign == 0 || mode > static_cast<uint32_t>(nix::bmCheck))
            {
                return std::nullopt;
            }
            return mode == static_cast<uint32_t>(nix::bmRepair) ? Role::trusted : Role::write;
        }
        if (!drv.empty() || assign != 0 || mode != 0)
        {
            return std::nullopt;
        }
        if (method == "Connect")
        {
            return Role::trusted;
        }
        if (method == "AddMultipleToStore")
        {
            // Unsigned imports require trusted: checked against the request
            // by the worker dispatcher, not inferred from the method name.
            return Role::write;
        }
        if (method == "QueryValidPaths" || method == "QueryPathInfos" || method == "FetchNars" ||
            method == "QueryMissing" || method == "StoreInfo")
        {
            return Role::readOnly;
        }
        return std::nullopt;
    }

    static auto sign(std::string_view key, std::string_view data) -> std::optional<std::string>
    {
        unsigned char mac[EVP_MAX_MD_SIZE];
        unsigned length = 0;
        if (!HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
                  reinterpret_cast<const unsigned char *>(data.data()), data.size(), mac, &length) ||
            length != macBytes)
        {
            return std::nullopt;
        }
        return std::string(reinterpret_cast<const char *>(mac), macBytes);
    }
};

} // namespace nixgrpc
