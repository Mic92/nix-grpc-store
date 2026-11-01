#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "acl.hh"
#include "auth.hh"
#include "nix_remote.pb.h"
#include "reverse-capability.hh"
#include "reverse-upload-pipe.hh"
#include "reverse-upload.hh"

namespace {

constexpr uint64_t assignedId = 42;
constexpr size_t followBytes = 70000;
constexpr size_t oddFrameBytes = 16381;
constexpr size_t largeLaterParts = 70;
constexpr size_t overloadAttempts = 80;

auto appendLengthPrefixed(std::string & out, std::string_view payload) -> void
{
    constexpr unsigned prefixBytes = 4;
    constexpr unsigned bitsPerByte = 8;
    constexpr uint32_t byteMask = 0xff;
    auto const length = static_cast<uint32_t>(payload.size());
    for (unsigned index = 0; index < prefixBytes; ++index) {
        out.push_back(static_cast<char>((length >> (bitsPerByte * index)) & byteMask));
    }
    out += payload;
}

} // namespace

auto main() -> int
{
    auto key = nixgrpc::ReverseCapability::newKey();
    assert(key);
    std::string const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    nixgrpc::Caller const writer{.name = "client", .role = nixgrpc::Role::write,
                           .kind = nixgrpc::Caller::Kind::named};
    auto token = nixgrpc::ReverseCapability::mint(*key, worker, 1, 1, "UploadBuildInputs", writer, "assigned.drv", assignedId);
    assert(token);
    auto delegated = nixgrpc::ReverseCapability::verify(*key, *token, worker, 1, 1, "UploadBuildInputs");
    assert(delegated);

    nix::remote::UploadBuildInputsChunk first;
    first.set_drv_path("assigned.drv");
    first.set_assign_id(assignedId);
    first.mutable_chunk()->set_check_sigs(true); // the first message may be flags-only
    assert(nixgrpc::ReverseUpload::first(*delegated, first));
    first.set_assign_id(assignedId + 1);
    assert(!nixgrpc::ReverseUpload::first(*delegated, first));
    first.set_assign_id(assignedId);
    first.mutable_chunk()->set_repair(true);
    assert(!nixgrpc::ReverseUpload::first(*delegated, first));
    first.mutable_chunk()->set_repair(false);
    first.mutable_chunk()->set_check_sigs(false);
    assert(!nixgrpc::ReverseUpload::first(*delegated, first));

    nix::remote::UploadBuildInputsChunk next;
    next.mutable_chunk()->set_data("compressed bytes");
    assert(nixgrpc::ReverseUpload::next(next));
    next.set_drv_path("assigned.drv");
    assert(!nixgrpc::ReverseUpload::next(next));
    next.clear_drv_path();
    next.set_assign_id(assignedId);
    assert(!nixgrpc::ReverseUpload::next(next));
    next.set_assign_id(0);
    next.mutable_chunk()->set_check_sigs(true);
    assert(!nixgrpc::ReverseUpload::next(next));
    next.mutable_chunk()->set_check_sigs(false);
    next.mutable_chunk()->set_data(std::string(nixgrpc::ReverseUpload::maxChunkBytes + 1, 'x'));
    assert(!nixgrpc::ReverseUpload::next(next));

    first.mutable_chunk()->set_data(std::string(nixgrpc::ReverseUpload::maxChunkBytes + 1, 'x'));
    assert(!nixgrpc::ReverseUpload::first(*delegated, first));
    first.mutable_chunk()->clear_data();

    auto buildToken = nixgrpc::ReverseCapability::mint(*key, worker, 1, 2, "BuildDerivation", writer, "assigned.drv", assignedId);
    assert(buildToken);
    auto build = nixgrpc::ReverseCapability::verify(*key, *buildToken, worker, 1, 2, "BuildDerivation");
    assert(build);
    first.mutable_chunk()->set_check_sigs(true);
    assert(!nixgrpc::ReverseUpload::first(*build, first)); // build capability cannot authorize an upload

    first.mutable_chunk()->set_check_sigs(true);
    first.mutable_chunk()->set_data("header");
    nix::remote::UploadBuildInputsChunk follow;
    follow.mutable_chunk()->set_data(std::string(followBytes, 'z'));
    std::string wire;
    for (auto const & part : {first.SerializeAsString(), follow.SerializeAsString()}) {
        appendLengthPrefixed(wire, part);
    }
    nixgrpc::ReverseUpload::Decoder decoder(*delegated);
    size_t count = 0;
    auto consume = [&](const nix::remote::UploadBuildInputsChunk & part) -> bool {
        ++count;
        return count == 1 ? part.chunk().data() == "header" : part.chunk().data().size() == followBytes;
    };
    for (size_t offset = 0; offset < wire.size(); offset += oddFrameBytes) {
        assert(decoder.feed(std::string_view(wire).substr(offset, oddFrameBytes), consume));
    }
    assert(count == 2 && decoder.complete());
    assert(!decoder.feed("\xff\xff\xff\xff", consume));

    nixgrpc::ReverseUpload::Decoder incomplete(*delegated);
    assert(incomplete.feed(std::string_view(wire).substr(0, 3), consume));
    assert(!incomplete.complete());
    nixgrpc::ReverseUpload::Decoder wrong(*build);
    assert(!wrong.feed(wire, consume));
    nixgrpc::ReverseUpload::Decoder aborted(*delegated);
    assert(!aborted.feed(wire, [](const nix::remote::UploadBuildInputsChunk &) -> bool { return false; }));
    assert(!aborted.complete());
    nixgrpc::ReverseUpload::Decoder invalidNext(*delegated);
    follow.set_assign_id(assignedId);
    auto invalid = first.SerializeAsString();
    std::string const wireNext = follow.SerializeAsString();
    std::string malformed;
    for (auto const & part : {invalid, wireNext}) {
        appendLengthPrefixed(malformed, part);
    }
    assert(!invalidNext.feed(malformed, [](const nix::remote::UploadBuildInputsChunk &) -> bool { return true; }));

    // More than the entire session's queue: decoding remains one protobuf at
    // a time, including frame boundaries that split the length prefix.
    nixgrpc::ReverseUpload::Decoder large(*delegated);
    size_t parts = 0;
    std::string const firstWire = first.SerializeAsString();
    nix::remote::UploadBuildInputsChunk largePart;
    largePart.mutable_chunk()->set_data(std::string(followBytes, 'z'));
    std::string const laterWire = largePart.SerializeAsString();
    auto push = [&](std::string_view data) -> void {
        std::string framed;
        appendLengthPrefixed(framed, data);
        for (size_t offset = 0; offset < framed.size(); offset += oddFrameBytes) {
            bool const fed = large.feed(std::string_view(framed).substr(offset, oddFrameBytes),
                                        [&](const nix::remote::UploadBuildInputsChunk &) -> bool {
                                            ++parts;
                                            return true;
                                        });
            assert(fed);
        }
    };
    push(firstWire);
    for (size_t part = 0; part < largeLaterParts; ++part) {
        push(laterWire);
    }
    assert(parts == largeLaterParts + 1 && large.complete());

    // Deliberate overload cannot silently drop bytes or acknowledge an
    // incomplete upload. The real registration reader must remain nonblocking
    // so cancellation/control frames can still be processed.
    nixgrpc::ReverseUploadPipe saturated(*delegated);
    auto frame = [](const nix::remote::UploadBuildInputsChunk & chunk) -> std::string {
        auto wire = chunk.SerializeAsString();
        std::string out;
        appendLengthPrefixed(out, wire);
        return out;
    };
    assert(saturated.feed(frame(first)));
    auto const data = frame(largePart);
    bool overflowed = false;
    for (size_t attempt = 0; attempt < overloadAttempts; ++attempt) {
        if (!saturated.feed(data)) {
            overflowed = true;
            break;
        }
    }
    assert(overflowed && saturated.cancelled() && !saturated.finish());
}
