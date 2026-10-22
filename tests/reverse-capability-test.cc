#include <cassert>
#include <chrono>
#include <string>

#include "reverse-capability.hh"

int main()
{
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
    auto const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    auto token = ReverseCapability::mint(*key, worker, 7, 42, "BuildDerivation", writer, "abc.drv", 93, 0, now);
    assert(token);
    auto verified = ReverseCapability::verify(*key, *token, worker, 7, 42, "BuildDerivation", now);
    assert(verified && verified->caller.name == writer.name && verified->caller.role == Role::write &&
           verified->drvPath == "abc.drv" && verified->assignId == 93);
    nix::remote::ReverseOpen open;
    open.set_method("BuildDerivation");
    open.set_capability(*token);
    assert(ReverseCapability::authorizeOpen(*key, open, worker, 7, 42, now));
    open.set_method("Connect");
    assert(!ReverseCapability::authorizeOpen(*key, open, worker, 7, 42, now));
    nix::remote::BuildDerivationRequest request;
    request.set_drv_path("abc.drv");
    request.set_assign_id(93);
    assert(ReverseCapability::matchesBuild(*verified, request));
    request.set_assign_id(94);
    assert(!ReverseCapability::matchesBuild(*verified, request));
    request.set_assign_id(93);
    request.set_build_mode(static_cast<uint32_t>(nix::bmRepair));
    assert(!ReverseCapability::matchesBuild(*verified, request));
    assert(!ReverseCapability::verify(*otherKey, *token, worker, 7, 42, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, "other-worker", 7, 42, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, worker, 8, 42, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, worker, 7, 43, "BuildDerivation", now));
    assert(!ReverseCapability::verify(*key, *token, worker, 7, 42, "StoreInfo", now));
    assert(!ReverseCapability::verify(*key, *token, worker, 7, 42, "BuildDerivation", now + std::chrono::seconds(31)));
    assert(!ReverseCapability::verify(*key, *token, worker, 7, 42, "BuildDerivation", now - std::chrono::seconds(1)));
    auto tampered = *token;
    tampered.at(tampered.size() - 1) ^= 1;
    assert(!ReverseCapability::verify(*key, tampered, worker, 7, 42, "BuildDerivation", now));
    assert(
        !ReverseCapability::verify(*key, token->substr(0, token->size() - 1), worker, 7, 42, "BuildDerivation", now));

    assert(!ReverseCapability::mint(*key, worker, 7, 42, "BuildDerivation", reader, "abc.drv", 93, 0, now));
    assert(!ReverseCapability::mint(*key, worker, 7, 42, "BuildDerivation", writer, "abc.drv", 93,
                                    static_cast<uint32_t>(nix::bmRepair), now));
    assert(ReverseCapability::mint(*key, worker, 7, 42, "BuildDerivation", trusted, "abc.drv", 93,
                                   static_cast<uint32_t>(nix::bmRepair), now));
    assert(!ReverseCapability::mint(*key, worker, 7, 42, "BuildDerivation", writer, "abc.drv", 0, 0, now));
    assert(!ReverseCapability::mint(*key, worker, 7, 42, "StoreInfo", writer, "abc.drv", 93, 0, now));
    assert(!ReverseCapability::mint(*key, worker, 7, 42, "Connect", writer, {}, 0, 0, now));
    assert(!ReverseCapability::mint(*key, worker, 7, 42, "Unknown", trusted, {}, 0, 0, now));
    auto readToken = ReverseCapability::mint(*key, worker, 7, 44, "StoreInfo", reader, {}, 0, 0, now);
    assert(readToken);
    auto readBack = ReverseCapability::verify(*key, *readToken, worker, 7, 44, "StoreInfo", now);
    assert(readBack && readBack->caller.role == Role::readOnly);
    auto importToken = ReverseCapability::mint(*key, worker, 7, 45, "AddMultipleToStore", writer, {}, 0, 0, now);
    assert(importToken);
    auto import = ReverseCapability::verify(*key, *importToken, worker, 7, 45, "AddMultipleToStore", now);
    assert(import);
    nix::remote::AddMultipleChunk first;
    first.set_check_sigs(true);
    assert(ReverseCapability::permitsImport(*import, first));
    first.set_check_sigs(false);
    assert(!ReverseCapability::permitsImport(*import, first));
    auto uploadToken = ReverseCapability::mint(*key, worker, 7, 47, "UploadBuildInputs", writer,
                                               "abc.drv", 93, 0, now);
    assert(uploadToken);
    auto upload = ReverseCapability::verify(*key, *uploadToken, worker, 7, 47, "UploadBuildInputs", now);
    assert(upload && ReverseCapability::matchesBuildInputs(*upload, "abc.drv", 93));
    assert(!ReverseCapability::matchesBuildInputs(*verified, "abc.drv", 93));
    assert(!ReverseCapability::matchesBuildInputs(*upload, "other.drv", 93));
    assert(!ReverseCapability::matchesBuildInputs(*upload, "abc.drv", 94));
    assert(!ReverseCapability::matchesBuildInputs(*import, "abc.drv", 93));
    assert(!ReverseCapability::mint(*key, worker, 7, 47, "UploadBuildInputs", writer, {}, 93, 0, now));
    assert(!ReverseCapability::mint(*key, worker, 7, 47, "UploadBuildInputs", reader, "abc.drv", 93, 0, now));
    assert(!ReverseCapability::mint(*key, worker, 7, 47, "UploadBuildInputs", writer, "abc.drv", 93, 1, now));
    first.set_check_sigs(true);
    first.set_repair(false);
    assert(ReverseCapability::permitsBuildInputUpload(*upload, "abc.drv", 93, first));
    first.set_check_sigs(false);
    assert(!ReverseCapability::permitsBuildInputUpload(*upload, "abc.drv", 93, first));
    auto trustedUploadToken = ReverseCapability::mint(*key, worker, 7, 48, "UploadBuildInputs", trusted,
                                                      "abc.drv", 93, 0, now);
    assert(trustedUploadToken);
    auto trustedUpload = ReverseCapability::verify(*key, *trustedUploadToken, worker, 7, 48, "UploadBuildInputs", now);
    assert(trustedUpload && ReverseCapability::permitsBuildInputUpload(*trustedUpload, "abc.drv", 93, first));
    first.set_repair(true);
    assert(!ReverseCapability::permitsImport(*import, first));
    auto noRole = writer;
    noRole.role = std::nullopt;
    assert(!ReverseCapability::mint(*key, worker, 7, 42, "StoreInfo", noRole, {}, 0, 0, now));
    auto anonymous = trusted;
    anonymous.kind = Caller::Kind::anonymous;
    auto anonymousToken = ReverseCapability::mint(*key, worker, 7, 46, "StoreInfo", anonymous, {}, 0, 0, now);
    assert(!anonymousToken); // neither gateway nor mint primitive delegates anonymous privilege
}
