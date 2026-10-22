#include <cassert>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <zstd.h>

#include "pump.hh"
#include "reverse-request.hh"

int main()
{
    auto key = nixgrpc::ReverseCapability::newKey();
    assert(key);
    std::string const worker = "pod:staging:01234567-89ab-cdef-0123-456789abcdef";
    nixgrpc::Caller writer{.name = "real-client", .role = nixgrpc::Role::write,
                           .kind = nixgrpc::Caller::Kind::named};
    auto token = nixgrpc::ReverseCapability::mint(*key, worker, 1, 1, "BuildDerivation", writer, "assigned.drv", 42, 0);
    assert(token);
    nixgrpc::ReverseRequest requests(worker, 1, *key);
    nix::remote::ReverseFarmMsg frame;
    frame.set_stream_id(1);
    frame.mutable_open()->set_method("BuildDerivation");
    frame.mutable_open()->set_capability(*token);
    assert(requests.accept(frame) && !requests.takeReady());
    nix::remote::BuildDerivationRequest build;
    build.set_drv_path("assigned.drv");
    build.set_assign_id(42);
    auto payload = build.SerializeAsString();
    frame.mutable_data()->set_data(std::string(1, static_cast<char>(payload.size())));
    assert(requests.accept(frame) && !requests.takeReady());
    frame.mutable_data()->set_data(std::string("\0\0\0", 3) + payload);
    assert(requests.accept(frame) && !requests.takeReady());
    frame.set_half_close(true);
    assert(requests.accept(frame));
    auto ready = requests.takeReady();
    assert(ready && ready->payload == payload && ready->delegated.caller.name == "real-client");
    nix::remote::BuildDerivationRequest decoded;
    assert(decoded.ParseFromString(ready->payload) && nixgrpc::ReverseRequest::matchesBuild(*ready, decoded));
    decoded.set_assign_id(43);
    assert(!nixgrpc::ReverseRequest::matchesBuild(*ready, decoded));
    assert(requests.finished(1));

    auto next = nixgrpc::ReverseCapability::mint(*key, worker, 1, 2, "BuildDerivation", writer, "assigned.drv", 42, 0);
    assert(next);
    frame.Clear();
    frame.set_stream_id(2);
    frame.mutable_open()->set_method("BuildDerivation");
    frame.mutable_open()->set_capability(*next);
    assert(requests.accept(frame));
    frame.mutable_data()->set_data("\xff\xff\xff\xff");
    assert(!requests.accept(frame)); // unbounded declared payload rejected before allocation

    nixgrpc::ReverseRequest cancellable(worker, 1, *key);
    frame.Clear();
    frame.set_stream_id(2);
    frame.mutable_open()->set_method("BuildDerivation");
    frame.mutable_open()->set_capability(*next);
    assert(cancellable.accept(frame));
    frame.mutable_data()->set_data(std::string(4, '\0'));
    assert(cancellable.accept(frame));
    frame.set_half_close(true);
    assert(cancellable.accept(frame));
    frame.set_cancel(true);
    assert(cancellable.accept(frame) && !cancellable.takeReady());
    assert(cancellable.finished(2));
    assert(!cancellable.finished(2));

    nixgrpc::ReverseRequest running(worker, 1, *key);
    frame.Clear();
    frame.set_stream_id(2);
    frame.mutable_open()->set_method("BuildDerivation");
    frame.mutable_open()->set_capability(*next);
    assert(running.accept(frame));
    frame.mutable_data()->set_data(std::string(4, '\0'));
    assert(running.accept(frame));
    frame.set_half_close(true);
    assert(running.accept(frame) && running.takeReady());
    frame.set_cancel(true);
    assert(running.accept(frame));
    assert(running.finished(2)); // executor completion after a cancelled build
    assert(!running.finished(2));

    nixgrpc::ReverseRequest replacement(worker, 2, *key);
    frame.Clear();
    frame.set_stream_id(1);
    frame.mutable_open()->set_method("BuildDerivation");
    frame.mutable_open()->set_capability(*token);
    assert(!replacement.accept(frame)); // old generation cannot be replayed
    auto uploadToken = nixgrpc::ReverseCapability::mint(*key, worker, 1, 3, "UploadBuildInputs",
                                                        writer, "assigned.drv", 42);
    assert(uploadToken);
    nixgrpc::ReverseRequest uploads(worker, 1, *key, [](std::string_view drv, uint64_t id) {
        return drv == "assigned.drv" && id == 42;
    });
    frame.Clear();
    frame.set_stream_id(3);
    frame.mutable_open()->set_method("UploadBuildInputs");
    frame.mutable_open()->set_capability(*uploadToken);
    assert(uploads.accept(frame));
    auto upload = uploads.takeUploadReady();
    assert(upload && upload->delegated.assignId == 42 && !uploads.takeReady());
    nix::remote::UploadBuildInputsChunk header;
    header.set_drv_path("assigned.drv");
    header.set_assign_id(42);
    header.mutable_chunk()->set_check_sigs(true);
    auto bytes = header.SerializeAsString();
    std::string wire;
    auto const length = static_cast<uint32_t>(bytes.size());
    for (unsigned i = 0; i < 4; ++i) wire.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
    wire += bytes;
    frame.mutable_data()->set_data(wire.substr(0, 3));
    assert(uploads.accept(frame));
    frame.mutable_data()->set_data(wire.substr(3));
    assert(uploads.accept(frame));
    frame.set_half_close(true);
    assert(uploads.accept(frame));
    nix::remote::AddMultipleChunk flags;
    assert(upload->pipe->Read(&flags) && flags.check_sigs());
    assert(!upload->pipe->Read(&flags) && upload->pipe->complete());
    assert(upload->pipe->endWithoutTrailingData());
    assert(uploads.finished(3));

    nixgrpc::ReverseRequest aborted(worker, 1, *key, [](std::string_view, uint64_t) { return true; });
    frame.Clear();
    frame.set_stream_id(3);
    frame.mutable_open()->set_method("UploadBuildInputs");
    frame.mutable_open()->set_capability(*uploadToken);
    assert(aborted.accept(frame));
    auto cancelledUpload = aborted.takeUploadReady();
    assert(cancelledUpload);
    std::atomic<bool> stopped{false};
    std::jthread reader([&] {
        nix::remote::AddMultipleChunk chunk;
        stopped = !cancelledUpload->pipe->Read(&chunk);
    });
    frame.set_cancel(true);
    assert(aborted.accept(frame));
    reader.join();
    assert(stopped && cancelledUpload->pipe->cancelled() && !cancelledUpload->pipe->complete() && aborted.finished(3));

    nixgrpc::ReverseRequest malformed(worker, 1, *key, [](std::string_view, uint64_t) { return true; });
    frame.Clear();
    frame.set_stream_id(3);
    frame.mutable_open()->set_method("UploadBuildInputs");
    frame.mutable_open()->set_capability(*uploadToken);
    assert(malformed.accept(frame));
    frame.mutable_data()->set_data("\xff\xff\xff\xff");
    assert(!malformed.accept(frame));
    malformed.close();

    nixgrpc::ReverseUploadPipe pressured(upload->delegated);
    std::atomic<size_t> consumed{0};
    std::jthread consumer([&] {
        nix::remote::AddMultipleChunk chunk;
        while (pressured.Read(&chunk)) {
            ++consumed;
            chunk.Clear();
        }
    });
    assert(pressured.feed(wire));
    nix::remote::UploadBuildInputsChunk nextChunk;
    nextChunk.mutable_chunk()->set_data(std::string(96 * 1024, 'x'));
    auto const nextWire = nextChunk.SerializeAsString();
    std::string nextFramed;
    auto const nextSize = static_cast<uint32_t>(nextWire.size());
    for (unsigned i = 0; i < 4; ++i) nextFramed.push_back(static_cast<char>((nextSize >> (8 * i)) & 0xff));
    nextFramed += nextWire;
    for (size_t n = 0; n < 48; ++n) {
        for (size_t offset = 0; offset < nextFramed.size(); offset += nixgrpc::ReverseFrames::maxFrameBytes)
            assert(pressured.feed(std::string_view(nextFramed).substr(offset, nixgrpc::ReverseFrames::maxFrameBytes)));
        // Unpaced production exceeding the handoff cap is tested separately
        // as a closed failure, not assumed to succeed by winning a race.
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(pressured.finish());
    consumer.join();
    assert(consumed == 49 && pressured.complete() && !pressured.cancelled());

    nixgrpc::ReverseUploadPipe trailing(upload->delegated);
    assert(trailing.feed(wire));
    assert(trailing.feed(nextFramed));
    assert(trailing.finish());
    nix::remote::AddMultipleChunk onlyHeader;
    assert(trailing.Read(&onlyHeader) && onlyHeader.check_sigs());
    assert(!trailing.endWithoutTrailingData());
    trailing.cancel();

    nixgrpc::ReverseUploadPipe interrupted(upload->delegated);
    assert(interrupted.feed(wire));
    std::atomic<bool> exited{false};
    std::jthread producer([&] {
        for (size_t n = 0; n < 1000; ++n)
            if (!interrupted.feed(nextFramed)) { exited = true; return; }
    });
    interrupted.cancel();
    producer.join();
    assert(exited && interrupted.cancelled() && !interrupted.complete());

    nixgrpc::ReverseUploadPipe stalled(upload->delegated);
    assert(stalled.feed(wire));
    bool overflow = false;
    for (size_t n = 0; n < 100; ++n) {
        if (!stalled.feed(nextFramed)) { overflow = true; break; }
    }
    assert(overflow && stalled.cancelled() && !stalled.complete());

    nixgrpc::ReverseUploadPipe truncated(upload->delegated);
    assert(truncated.feed(wire.substr(0, wire.size() - 1)));
    assert(!truncated.finish() && !truncated.complete());

    nixgrpc::ReverseUploadPipe wrongAssignment(upload->delegated);
    header.set_assign_id(43);
    auto wrongWire = header.SerializeAsString();
    std::string wrongFramed;
    auto const wrongSize = static_cast<uint32_t>(wrongWire.size());
    for (unsigned i = 0; i < 4; ++i) wrongFramed.push_back(static_cast<char>((wrongSize >> (8 * i)) & 0xff));
    wrongFramed += wrongWire;
    assert(!wrongAssignment.feed(wrongFramed) && wrongAssignment.cancelled());

    nixgrpc::ReverseUploadPipe unsignedUpload(upload->delegated);
    header.set_assign_id(42);
    header.mutable_chunk()->set_check_sigs(false);
    wrongWire = header.SerializeAsString();
    wrongFramed.clear();
    auto const unsignedSize = static_cast<uint32_t>(wrongWire.size());
    for (unsigned i = 0; i < 4; ++i) wrongFramed.push_back(static_cast<char>((unsignedSize >> (8 * i)) & 0xff));
    wrongFramed += wrongWire;
    assert(!unsignedUpload.feed(wrongFramed) && unsignedUpload.cancelled());

    nixgrpc::ReverseRequest stale(worker, 1, *key, [](std::string_view, uint64_t) { return false; });
    frame.Clear();
    frame.set_stream_id(3);
    frame.mutable_open()->set_method("UploadBuildInputs");
    frame.mutable_open()->set_capability(*uploadToken);
    assert(!stale.accept(frame) && !stale.takeUploadReady());
    std::atomic<bool> expected{true};
    nixgrpc::ReverseUploadPipe revoked(upload->delegated, [&] { return expected.load(); });
    assert(revoked.feed(wire));
    expected = false;
    assert(!revoked.feed(nextFramed) && revoked.cancelled());
    std::atomic<bool> readingLive{true};
    nixgrpc::ReverseUploadPipe revokedWhileReading(upload->delegated, [&] { return readingLive.load(); });
    std::jthread waiting([&] {
        nix::remote::AddMultipleChunk part;
        assert(!revokedWhileReading.Read(&part));
    });
    readingLive = false;
    waiting.join();
    assert(revokedWhileReading.cancelled());

    // Real compressed AddMultipleToStore bytes, not only protobuf framing:
    // the importer-side source reads across many reverse transport frames.
    nixgrpc::ReverseUploadPipe compressed(upload->delegated);
    assert(compressed.feed(wire));
    std::string plain(5 * 1024 * 1024, 'a');
    std::string packed(ZSTD_compressBound(plain.size()), '\0');
    auto const packedSize = ZSTD_compress(packed.data(), packed.size(), plain.data(), plain.size(), 1);
    assert(!ZSTD_isError(packedSize));
    packed.resize(packedSize);
    nix::remote::UploadBuildInputsChunk data;
    data.mutable_chunk()->set_data(packed);
    auto const dataWire = data.SerializeAsString();
    std::string framed;
    auto const dataSize = static_cast<uint32_t>(dataWire.size());
    for (unsigned i = 0; i < 4; ++i) framed.push_back(static_cast<char>((dataSize >> (8 * i)) & 0xff));
    framed += dataWire;
    for (size_t offset = 0; offset < framed.size(); offset += nixgrpc::ReverseFrames::maxFrameBytes)
        assert(compressed.feed(std::string_view(framed).substr(offset, nixgrpc::ReverseFrames::maxFrameBytes)));
    assert(compressed.finish());
    nix::remote::AddMultipleChunk firstCompressed;
    assert(compressed.Read(&firstCompressed) && firstCompressed.check_sigs());
    nixgrpc::ZstdReaderSource<nixgrpc::ReverseUploadPipe, nix::remote::AddMultipleChunk> source(
        compressed, std::move(*firstCompressed.mutable_data()));
    std::string restored(plain.size(), '\0');
    source(restored.data(), restored.size());
    assert(restored == plain);
    source.finishFrame();
    nix::remote::AddMultipleChunk rest;
    assert(!compressed.Read(&rest) && compressed.complete());

    auto checkInvalidFrame = [&](std::string badCompressed) {
        nixgrpc::ReverseUploadPipe bad(upload->delegated);
        assert(bad.feed(wire));
        nix::remote::UploadBuildInputsChunk part;
        part.mutable_chunk()->set_data(badCompressed);
        auto bytes = part.SerializeAsString();
        std::string framedPart;
        auto const length = static_cast<uint32_t>(bytes.size());
        for (unsigned i = 0; i < 4; ++i) framedPart.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        framedPart += bytes;
        for (size_t offset = 0; offset < framedPart.size(); offset += nixgrpc::ReverseFrames::maxFrameBytes)
            assert(bad.feed(std::string_view(framedPart).substr(offset, nixgrpc::ReverseFrames::maxFrameBytes)));
        assert(bad.finish());
        nix::remote::AddMultipleChunk initial;
        assert(bad.Read(&initial));
        nixgrpc::ZstdReaderSource<nixgrpc::ReverseUploadPipe, nix::remote::AddMultipleChunk> decoder(
            bad, std::move(*initial.mutable_data()));
        std::string output(plain.size(), '\0');
        bool rejected = false;
        try {
            decoder(output.data(), output.size());
            assert(output == plain);
            decoder.finishFrame();
        }
        catch (const nix::Error &) { rejected = true; }
        assert(rejected);
    };
    auto truncatedZstd = packed;
    truncatedZstd.pop_back();
    checkInvalidFrame(std::move(truncatedZstd));
    checkInvalidFrame(packed + packed);
    checkInvalidFrame(packed + "junk");
}
