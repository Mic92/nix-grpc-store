#include <cassert>
#include <string>

#include "reverse-upload.hh"
#include "reverse-upload-pipe.hh"

int main()
{
    auto key = nixgrpc::ReverseCapability::newKey();
    assert(key);
    std::string const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    nixgrpc::Caller writer{.name = "client", .role = nixgrpc::Role::write,
                           .kind = nixgrpc::Caller::Kind::named};
    auto token = nixgrpc::ReverseCapability::mint(*key, worker, 1, 1, "UploadBuildInputs", writer, "assigned.drv", 42);
    assert(token);
    auto delegated = nixgrpc::ReverseCapability::verify(*key, *token, worker, 1, 1, "UploadBuildInputs");
    assert(delegated);

    nix::remote::UploadBuildInputsChunk first;
    first.set_drv_path("assigned.drv");
    first.set_assign_id(42);
    first.mutable_chunk()->set_check_sigs(true); // the first message may be flags-only
    assert(nixgrpc::ReverseUpload::first(*delegated, first));
    first.set_assign_id(43);
    assert(!nixgrpc::ReverseUpload::first(*delegated, first));
    first.set_assign_id(42);
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
    next.set_assign_id(42);
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

    auto buildToken = nixgrpc::ReverseCapability::mint(*key, worker, 1, 2, "BuildDerivation", writer, "assigned.drv", 42);
    assert(buildToken);
    auto build = nixgrpc::ReverseCapability::verify(*key, *buildToken, worker, 1, 2, "BuildDerivation");
    assert(build);
    first.mutable_chunk()->set_check_sigs(true);
    assert(!nixgrpc::ReverseUpload::first(*build, first)); // build capability cannot authorize an upload

    first.mutable_chunk()->set_check_sigs(true);
    first.mutable_chunk()->set_data("header");
    nix::remote::UploadBuildInputsChunk follow;
    follow.mutable_chunk()->set_data(std::string(70000, 'z'));
    std::string wire;
    for (auto const & part : {first.SerializeAsString(), follow.SerializeAsString()}) {
        auto const length = static_cast<uint32_t>(part.size());
        for (unsigned i = 0; i < 4; ++i) wire.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        wire += part;
    }
    nixgrpc::ReverseUpload::Decoder decoder(*delegated);
    size_t count = 0;
    auto consume = [&](nix::remote::UploadBuildInputsChunk && part) {
        ++count;
        return count == 1 ? part.chunk().data() == "header" : part.chunk().data().size() == 70000;
    };
    for (size_t offset = 0; offset < wire.size(); offset += 16381)
        assert(decoder.feed(std::string_view(wire).substr(offset, 16381), consume));
    assert(count == 2 && decoder.complete());
    assert(!decoder.feed("\xff\xff\xff\xff", consume));

    nixgrpc::ReverseUpload::Decoder incomplete(*delegated);
    assert(incomplete.feed(std::string_view(wire).substr(0, 3), consume));
    assert(!incomplete.complete());
    nixgrpc::ReverseUpload::Decoder wrong(*build);
    assert(!wrong.feed(wire, consume));
    nixgrpc::ReverseUpload::Decoder aborted(*delegated);
    assert(!aborted.feed(wire, [](nix::remote::UploadBuildInputsChunk &&) { return false; }));
    assert(!aborted.complete());
    nixgrpc::ReverseUpload::Decoder invalidNext(*delegated);
    follow.set_assign_id(42);
    auto invalid = first.SerializeAsString();
    std::string wireNext = follow.SerializeAsString();
    std::string malformed;
    for (auto const & part : {invalid, wireNext}) {
        auto const length = static_cast<uint32_t>(part.size());
        for (unsigned i = 0; i < 4; ++i) malformed.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        malformed += part;
    }
    assert(!invalidNext.feed(malformed, [](nix::remote::UploadBuildInputsChunk &&) { return true; }));

    // More than the entire session's queue: decoding remains one protobuf at
    // a time, including frame boundaries that split the length prefix.
    nixgrpc::ReverseUpload::Decoder large(*delegated);
    size_t parts = 0;
    std::string const firstWire = first.SerializeAsString();
    nix::remote::UploadBuildInputsChunk largePart;
    largePart.mutable_chunk()->set_data(std::string(70000, 'z'));
    std::string const laterWire = largePart.SerializeAsString();
    auto push = [&](std::string_view data) {
        std::string framed;
        auto const length = static_cast<uint32_t>(data.size());
        for (unsigned i = 0; i < 4; ++i) framed.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        framed += data;
        for (size_t offset = 0; offset < framed.size(); offset += 16381)
            assert(large.feed(std::string_view(framed).substr(offset, 16381),
                              [&](nix::remote::UploadBuildInputsChunk &&) { ++parts; return true; }));
    };
    push(firstWire);
    for (size_t i = 0; i < 70; ++i) push(laterWire);
    assert(parts == 71 && large.complete());

    // Deliberate overload cannot silently drop bytes or acknowledge an
    // incomplete upload. The real registration reader must remain nonblocking
    // so cancellation/control frames can still be processed.
    nixgrpc::ReverseUploadPipe saturated(*delegated);
    auto frame = [](const nix::remote::UploadBuildInputsChunk & chunk) {
        auto wire = chunk.SerializeAsString();
        std::string out;
        auto const length = static_cast<uint32_t>(wire.size());
        for (unsigned i = 0; i < 4; ++i) out.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        out += wire;
        return out;
    };
    assert(saturated.feed(frame(first)));
    auto const data = frame(largePart);
    bool overflowed = false;
    for (size_t i = 0; i < 80; ++i) {
        if (!saturated.feed(data)) { overflowed = true; break; }
    }
    assert(overflowed && saturated.cancelled() && !saturated.finish());
}
