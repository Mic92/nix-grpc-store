#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <openssl/rand.h>

#include <nix/store/store-api.hh>
#include <nix/store/globals.hh>
#include <nix/store/store-open.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/archive.hh>
#include <nix/util/file-system.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signature/local-keys.hh>
#include <nix/util/signature/signer.hh>

#include "import-paths.hh"
#include "nix-compat.hh"
#include "pump.hh"
#include "reverse-upload-pipe.hh"
#include "upload-spool.hh"

int main()
{
    nix::initLibStore(false);
    auto const root = nix::createTempDir();
    nix::AutoDelete cleanup(root);
    auto sourceStore = nix::openStore("local?root=" + (root / "source").string());
    auto destination = nix::openStore("local?root=" + (root / "destination").string());

    // Compress poorly so the wire stream, not merely the decompressed NAR,
    // exceeds the registration reader's bounded 4 MiB queue.
    std::string random(nixgrpc::ReverseFrames::maxQueuedBytes + 2 * 1024 * 1024, '\0');
    assert(RAND_bytes(reinterpret_cast<unsigned char *>(random.data()), static_cast<int>(random.size())) == 1);
    nix::StringSink nar;
    nix::dumpString(random, nar);
    nix::StringSource firstNar(nar.s);
    auto const path = sourceStore->addToStoreFromDump(firstNar, "reverse-import-test");
    auto const info = sourceStore->queryPathInfo(path);
    assert(info->isContentAddressed(*sourceStore));
    // Simulate a derivation output: unlike the content-addressed source
    // fixture, this path must not be accepted by an untrusted write caller
    // without a valid binary-cache signature.
    nix::ValidPathInfo unsignedOutput(*info);
    unsignedOutput.ca.reset();
    unsignedOutput.ultimate = false;
    unsignedOutput.sigs.clear();
    auto rejected = nix::openStore("local?root=" + (root / "unsigned").string());
    nix::StringSource unsignedNar(nar.s);
    bool denied = false;
    try {
        rejected->addToStore(unsignedOutput, unsignedNar, nix::NoRepair, nix::CheckSigs);
    } catch (const nix::Error &) {
        denied = true;
    }
    assert(denied && !rejected->isValidPath(path));
    auto signingKey = nix::SecretKey::generate("reverse-import-test");
    auto publicKey = signingKey.toPublicKey();
    nix::LocalSigner signer(std::move(signingKey));
    unsignedOutput.sign(*sourceStore, signer);
    nix::PublicKeys keys;
    keys.emplace(publicKey.name, std::move(publicKey));
    assert(unsignedOutput.checkSignatures(*sourceStore, keys) == 1);
    // This verifies the Nix binary-cache signature itself. A production
    // gateway still needs a store configured to trust this public key.
    auto signedDestination = nix::openStore("local?root=" + (root / "signed").string());
    assert(unsignedOutput.checkSignatures(*signedDestination, keys) == 1);
    auto trustedDestination = nix::openStore("local?root=" + (root / "trusted-output").string());
    nix::StringSource trustedNar(nar.s);
    trustedDestination->addToStore(unsignedOutput, trustedNar, nix::NoRepair, nix::NoCheckSigs);
    assert(trustedDestination->isValidPath(path));
    nix::StringSink body;
    body << static_cast<uint64_t>(1);
    nix::WorkerProto::Serialise<nix::ValidPathInfo>::write(
        *sourceStore, nix::WorkerProto::WriteConn{.to = body, .version = nixcompat::infoProtocolVersion()}, *info);
    sourceStore->narFromPath(path, body);
    assert(body.s.size() > info->narSize);

    nixgrpc::Caller trusted{.name = "test-writer", .role = nixgrpc::Role::trusted,
                             .kind = nixgrpc::Caller::Kind::named};
    auto key = nixgrpc::ReverseCapability::newKey();
    assert(key);
    std::string const pod = "pod:test:01234567-89ab-cdef-0123-456789abcdef";
    auto token = nixgrpc::ReverseCapability::mint(*key, pod, 1, 1, "UploadBuildInputs",
                                                  trusted, "assigned.drv", 42);
    assert(token);
    auto delegated = nixgrpc::ReverseCapability::verify(*key, *token, pod, 1, 1, "UploadBuildInputs");
    assert(delegated);
    nixgrpc::ReverseUploadPipe pipe(*delegated, [] { return true; });
    auto send = [&](const nix::remote::UploadBuildInputsChunk & frame) {
        auto bytes = frame.SerializeAsString();
        std::string wire;
        auto const length = static_cast<uint32_t>(bytes.size());
        for (unsigned i = 0; i < 4; ++i) wire.push_back(static_cast<char>((length >> (8 * i)) & 0xff));
        wire += bytes;
        for (size_t offset = 0; offset < wire.size(); offset += nixgrpc::ReverseFrames::maxFrameBytes)
            if (!pipe.feed(std::string_view(wire).substr(offset, nixgrpc::ReverseFrames::maxFrameBytes)))
                throw std::runtime_error("bounded upload handoff overflowed");
    };
    nix::remote::UploadBuildInputsChunk header;
    header.set_drv_path("assigned.drv");
    header.set_assign_id(42);
    header.mutable_chunk()->set_check_sigs(true);
    send(header);

    auto const max = ZSTD_compressBound(body.s.size());
    std::string packed(max, '\0');
    auto const size = ZSTD_compress(packed.data(), max, body.s.data(), body.s.size(), 1);
    assert(!ZSTD_isError(size));
    packed.resize(size);
    assert(packed.size() > nixgrpc::ReverseFrames::maxQueuedBytes);
    std::exception_ptr producerError;
    std::thread producer([&] {
        try {
            for (size_t offset = 0; offset < packed.size(); offset += 256 * 1024) {
                nix::remote::UploadBuildInputsChunk chunk;
                chunk.mutable_chunk()->set_data(packed.substr(offset, 256 * 1024));
                send(chunk);
                // This test drives the pipe directly, without the gRPC writer
                // and importer's normal scheduling. An unpaced producer is
                // expected to exhaust the bounded handoff and fail closed.
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            if (!pipe.finish()) throw std::runtime_error("upload half-close rejected");
        } catch (...) {
            producerError = std::current_exception();
            pipe.cancel();
        }
    });
    nixgrpc::NarSpool spool;
    nixgrpc::SpoolRange range;
    try {
        nix::remote::AddMultipleChunk flags;
        assert(pipe.Read(&flags) && flags.check_sigs());
        nixgrpc::ZstdReaderSource<nixgrpc::ReverseUploadPipe, nix::remote::AddMultipleChunk> decoded(
            pipe, std::move(*flags.mutable_data()));
        auto stats = nixgrpc::importPaths(*destination, decoded,
            [&](const nix::ValidPathInfo & imported, nix::Source & stream) {
                assert(imported.path == path);
                range = spool.add(stream, imported.narSize);
            });
        assert(stats.paths == 1 && stats.narBytes == info->narSize);
        assert(!destination->isValidPath(path)); // parsing never installs a NAR
        decoded.finishFrame();
        assert(pipe.endWithoutTrailingData());
    } catch (...) {
        pipe.cancel();
        producer.join();
        throw;
    }
    producer.join();
    if (producerError) std::rethrow_exception(producerError);
    auto narReader = spool.reader(range);
    destination->addToStore(*info, narReader, nix::NoRepair, nix::NoCheckSigs);
    assert(destination->isValidPath(path));
    nix::StringSink restored;
    destination->narFromPath(path, restored);
    assert(restored.s == nar.s);
}
