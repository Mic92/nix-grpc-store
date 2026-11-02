#include <cassert>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <string>

#include <nix/store/store-api.hh>

#include "acl.hh"
#include "auth.hh"
#include "nix_remote.pb.h"
#include "reverse-capability.hh"

namespace {

constexpr uint64_t generation = 7;
constexpr uint64_t buildStream = 42;
constexpr uint64_t readStream = 44;
constexpr uint64_t importStream = 45;
constexpr uint64_t anonymousStream = 46;
constexpr uint64_t uploadStream = 47;
constexpr uint64_t trustedUploadStream = 48;
constexpr uint64_t buildAssignId = 93;
constexpr uint64_t otherBuildAssignId = 94;

} // namespace

auto main() -> int
try {
    using nixgrpc::Caller;
    using nixgrpc::ReverseCapability;
    using nixgrpc::Role;
    auto key = ReverseCapability::newKey();
    auto otherKey = ReverseCapability::newKey();
    assert(key && otherKey && *key != *otherKey);
    auto const now = std::chrono::system_clock::now();
    Caller writer{.name = "oidc:client:alice", .role = Role::write, .kind = Caller::Kind::named};
    Caller reader{.name = "reader", .role = Role::readOnly, .kind = Caller::Kind::named};
    Caller trusted{.name = "builder-admin", .role = Role::trusted, .kind = Caller::Kind::named};
    const auto *const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    auto token = ReverseCapability::mint(*key, worker, generation, buildStream, "BuildDerivation", writer, "abc.drv", buildAssignId, 0, now);
    assert(token);
    auto verified = ReverseCapability::verify(*key, *token, worker, generation, buildStream, "BuildDerivation", now);
    assert(verified && verified->caller.name == writer.name && verified->caller.role == Role::write &&
           verified->drvPath == "abc.drv" && verified->assignId == buildAssignId);
    nix::remote::ReverseOpen open;
    open.set_method("BuildDerivation");
    open.set_capability(*token);
    assert(ReverseCapability::authorizeOpen(*key, open, worker, generation, buildStream, now));
    open.set_method("Connect");
    assert(!ReverseCapability::authorizeOpen(*key, open, worker, generation, buildStream, now));
    nix::remote::BuildDerivationRequest request;
    request.set_drv_path("abc.drv");
    request.set_assign_id(buildAssignId);
    assert(ReverseCapability::matchesBuild(*verified, request));
    request.set_assign_id(otherBuildAssignId);
    assert(!ReverseCapability::matchesBuild(*verified, request));
    request.set_assign_id(buildAssignId);
    request.set_build_mode(static_cast<uint32_t>(nix::bmRepair));
    assert(!ReverseCapability::matchesBuild(*verified, request));
    assert(!ReverseCapability::verify(*otherKey, *token, worker, generation, buildStream, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, "other-worker", generation, buildStream, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, worker, generation + 1, buildStream, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, worker, generation, buildStream + 1, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, worker, generation, buildStream, "StoreInfo", now));
    // A worker clock slightly behind or ahead of the gateway's still accepts
    // a fresh capability; beyond the skew margin it does not.
    assert(ReverseCapability::verify(*key, *token, worker, generation, buildStream, "BuildDerivation", now - std::chrono::milliseconds(50)));
    assert(ReverseCapability::verify(*key, *token, worker, generation, buildStream, "BuildDerivation", now - std::chrono::seconds(4)));
    assert(ReverseCapability::verify(*key, *token, worker, generation, buildStream, "BuildDerivation", now + std::chrono::seconds(34)));
    assert(!ReverseCapability::verify(*key, *token, worker, generation, buildStream, "BuildDerivation", now + std::chrono::seconds(36)));
    assert(!ReverseCapability::verify(*key, *token, worker, generation, buildStream, "BuildDerivation", now - std::chrono::seconds(6)));
    auto tampered = *token;
    tampered.at(tampered.size() - 1) ^= 1;
    assert(!ReverseCapability::verify(*key, tampered, worker, generation, buildStream, "BuildDerivation", now));
    assert(
        !ReverseCapability::verify(*key, token->substr(0, token->size() - 1), worker, generation, buildStream, "BuildDerivation", now));

    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "BuildDerivation", reader, "abc.drv", buildAssignId, 0, now));
    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "BuildDerivation", writer, "abc.drv", buildAssignId,
                                    static_cast<uint32_t>(nix::bmRepair), now));
    assert(ReverseCapability::mint(*key, worker, generation, buildStream, "BuildDerivation", trusted, "abc.drv", buildAssignId,
                                   static_cast<uint32_t>(nix::bmRepair), now));
    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "BuildDerivation", writer, "abc.drv", 0, 0, now));
    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "StoreInfo", writer, "abc.drv", buildAssignId, 0, now));
    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "Connect", writer, {}, 0, 0, now));
    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "Unknown", trusted, {}, 0, 0, now));
    auto readToken = ReverseCapability::mint(*key, worker, generation, readStream, "StoreInfo", reader, {}, 0, 0, now);
    assert(readToken);
    auto readBack = ReverseCapability::verify(*key, *readToken, worker, generation, readStream, "StoreInfo", now);
    assert(readBack && readBack->caller.role == Role::readOnly);
    auto importToken = ReverseCapability::mint(*key, worker, generation, importStream, "AddMultipleToStore", writer, {}, 0, 0, now);
    assert(importToken);
    auto import = ReverseCapability::verify(*key, *importToken, worker, generation, importStream, "AddMultipleToStore", now);
    assert(import);
    nix::remote::AddMultipleChunk first;
    first.set_check_sigs(true);
    assert(ReverseCapability::permitsImport(*import, first));
    first.set_check_sigs(false);
    assert(!ReverseCapability::permitsImport(*import, first));
    auto uploadToken = ReverseCapability::mint(*key, worker, generation, uploadStream, "UploadBuildInputs", writer,
                                               "abc.drv", buildAssignId, 0, now);
    assert(uploadToken);
    auto upload = ReverseCapability::verify(*key, *uploadToken, worker, generation, uploadStream, "UploadBuildInputs", now);
    assert(upload && ReverseCapability::matchesBuildInputs(*upload, "abc.drv", buildAssignId));
    assert(!ReverseCapability::matchesBuildInputs(*verified, "abc.drv", buildAssignId));
    assert(!ReverseCapability::matchesBuildInputs(*upload, "other.drv", buildAssignId));
    assert(!ReverseCapability::matchesBuildInputs(*upload, "abc.drv", otherBuildAssignId));
    assert(!ReverseCapability::matchesBuildInputs(*import, "abc.drv", buildAssignId));
    assert(!ReverseCapability::mint(*key, worker, generation, uploadStream, "UploadBuildInputs", writer, {}, buildAssignId, 0, now));
    assert(!ReverseCapability::mint(*key, worker, generation, uploadStream, "UploadBuildInputs", reader, "abc.drv", buildAssignId, 0, now));
    assert(!ReverseCapability::mint(*key, worker, generation, uploadStream, "UploadBuildInputs", writer, "abc.drv", buildAssignId, 1, now));
    first.set_check_sigs(true);
    first.set_repair(false);
    assert(ReverseCapability::permitsBuildInputUpload(*upload, "abc.drv", buildAssignId, first));
    first.set_check_sigs(false);
    assert(!ReverseCapability::permitsBuildInputUpload(*upload, "abc.drv", buildAssignId, first));
    auto trustedUploadToken = ReverseCapability::mint(*key, worker, generation, trustedUploadStream, "UploadBuildInputs", trusted,
                                                      "abc.drv", buildAssignId, 0, now);
    assert(trustedUploadToken);
    auto trustedUpload = ReverseCapability::verify(*key, *trustedUploadToken, worker, generation, trustedUploadStream, "UploadBuildInputs", now);
    assert(trustedUpload && ReverseCapability::permitsBuildInputUpload(*trustedUpload, "abc.drv", buildAssignId, first));
    first.set_repair(true);
    assert(!ReverseCapability::permitsImport(*import, first));
    auto noRole = writer;
    noRole.role = std::nullopt;
    assert(!ReverseCapability::mint(*key, worker, generation, buildStream, "StoreInfo", noRole, {}, 0, 0, now));
    auto anonymous = trusted;
    anonymous.kind = Caller::Kind::anonymous;
    auto anonymousToken = ReverseCapability::mint(*key, worker, generation, anonymousStream, "StoreInfo", anonymous, {}, 0, 0, now);
    assert(!anonymousToken); // neither gateway nor mint primitive delegates anonymous privilege
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << '\n';
    return 1;
}
