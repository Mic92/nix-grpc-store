#pragma once
// Worker-side framing boundary. No local RPC handler may run until an entire
// bounded request has been decoded and its delegated caller revalidated.

#include <cstdint>
#include <optional>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>

#include "nix_remote.pb.h"
#include "reverse-inbound.hh"
#include "reverse-relay.hh"
#include "reverse-upload-pipe.hh"

namespace nixgrpc {

class ReverseRequest
{
public:
    struct Ready
    {
        uint64_t id{};
        std::string method;
        ReverseCapability::Verified delegated;
        std::string payload;
    };

    struct Upload
    {
        uint64_t id;
        ReverseCapability::Verified delegated;
        std::shared_ptr<ReverseUploadPipe> pipe;
    };

    ReverseRequest(std::string workerId, uint64_t generation, std::string sessionKey,
                   std::function<bool(std::string_view, uint64_t)> expects = {})
        : inbound(std::move(workerId), generation, std::move(sessionKey)), expects(std::move(expects)) { }

    // False is a fatal protocol error: disconnect, and discard all pending
    // requests. A completed request still needs method-specific validation
    // before reaching a local handler.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity): one cohesive per-frame state machine.
    [[nodiscard]] auto accept(const nix::remote::ReverseFarmMsg & frame) -> bool
    {
        std::scoped_lock const lock(mutex);
        ready.reset();
        rejection = "";
        lateCancel = false;
        auto event = inbound.accept(frame);
        if (!event) { rejection = inbound.lastRejection(); return false; }
        auto const streamId = event->streamId;
        switch (event->kind) {
        case ReverseInbound::Event::Kind::open:
            if (!event->caller) { rejection = "open without caller"; return false; }
            // Builds use the bounded unary decoder. Uploads get a separate
            // bounded pipe, never the 1 MiB request accumulator.
            if (frame.open().method() == "UploadBuildInputs") {
                auto delegated = std::move(*event->caller);
                if (!expects || !expects(delegated.drvPath, delegated.assignId)) {
                    rejection = "upload for an assignment this worker does not expect";
                    return false;
                }
                auto pipe = std::make_shared<ReverseUploadPipe>(delegated,
                    [check = expects, drv = delegated.drvPath, assign = delegated.assignId] -> bool {
                        return check(drv, assign);
                    });
                uploads.emplace(streamId, pipe);
                uploadReady = Upload{.id = streamId, .delegated = std::move(delegated), .pipe = std::move(pipe)};
                return true;
            }
            if (frame.open().method() != "BuildDerivation") { rejection = "method not served"; return false; }
            requests.emplace(streamId, Pending{.method = frame.open().method(), .delegated = std::move(*event->caller)});
            return true;
        case ReverseInbound::Event::Kind::data: {
            if (auto upload = uploads.find(streamId); upload != uploads.end()) {
                return upload->second->feed(frame.data().data());
            }
            auto & pending = requests.at(streamId);
            auto const size = frame.data().data().size();
            if (pending.wire.size() > ReverseRelay::maxRequestBytes + 4
                || size > ReverseRelay::maxRequestBytes + 4 - pending.wire.size()) { rejection = "request too large"; return false; }
            pending.wire += frame.data().data();
            if (!pending.length && pending.wire.size() >= 4) {
                uint32_t length = 0;
                for (unsigned i = 0; i != 4; ++i) {
                    length |= static_cast<uint32_t>(static_cast<unsigned char>(pending.wire.at(i))) << (bitsPerByte * i);
                }
                if (length > ReverseRelay::maxRequestBytes) { rejection = "request too large"; return false; }
                pending.length = length;
            }
            return !pending.length || pending.wire.size() <= static_cast<size_t>(*pending.length) + 4;
        }
        case ReverseInbound::Event::Kind::halfClose: {
            if (auto upload = uploads.find(streamId); upload != uploads.end()) {
                return upload->second->finish();
            }
            auto request = requests.find(streamId);
            if (request == requests.end() || !request->second.length || request->second.wire.size() != *request->second.length + 4) {
                rejection = "incomplete request at half-close";
                return false;
            }
            auto pending = std::move(request->second);
            requests.erase(request);
            pending.wire.erase(0, 4);
            ready = Ready{.id = streamId, .method = std::move(pending.method),
                          .delegated = std::move(pending.delegated), .payload = std::move(pending.wire)};
            return true;
        }
        case ReverseInbound::Event::Kind::cancel:
            lateCancel = event->late;
            if (auto upload = uploads.find(streamId); upload != uploads.end()) {
                upload->second->cancel();
                uploads.erase(upload);
            }
            if (uploadReady && uploadReady->id == streamId) {
                uploadReady.reset();
            }
            requests.erase(streamId);
            if (ready && ready->id == streamId) {
                ready.reset();
            }
            return true;
        }
        return false;
    }

    // The last accept() was a cancel for a stream that had already finished.
    [[nodiscard]] auto lastCancelWasLate() const -> bool { std::scoped_lock const lock(mutex); return lateCancel; }

    // Why the last accept() returned false; for logs only.
    [[nodiscard]] auto lastRejection() const -> const char * { std::scoped_lock const lock(mutex); return rejection; }

    [[nodiscard]] auto takeReady() -> std::optional<Ready> { std::scoped_lock const lock(mutex); return std::exchange(ready, std::nullopt); }
    [[nodiscard]] auto takeUploadReady() -> std::optional<Upload> { std::scoped_lock const lock(mutex); return std::exchange(uploadReady, std::nullopt); }

    // Reader/writer disconnect must unblock a streaming importer even when
    // the registration reader itself is stuck in a gRPC Read.
    void cancelUploads()
    {
        std::scoped_lock const lock(mutex);
        for (auto & [streamId, pipe] : uploads) {
            pipe->cancel();
        }
    }

    // Call before a local BuildDerivation handler and recheck Builder::admit
    // immediately before execution. The request itself cannot grant a role.
    [[nodiscard]] static auto matchesBuild(const Ready & call, const nix::remote::BuildDerivationRequest & request) -> bool
    {
        return call.method == "BuildDerivation" && ReverseCapability::matchesBuild(call.delegated, request);
    }

    [[nodiscard]] auto finished(uint64_t streamId) -> bool {
        std::scoped_lock const lock(mutex);
        uploads.erase(streamId);
        return inbound.finished(streamId);
    }

    void close() {
        std::scoped_lock const lock(mutex);
        for (auto & [streamId, pipe] : uploads) {
            pipe->cancel();
        }
        uploads.clear();
        uploadReady.reset();
        inbound.close(); requests.clear(); ready.reset();
    }

private:
    static constexpr unsigned bitsPerByte = 8;
    mutable std::mutex mutex;
    const char * rejection = "";
    bool lateCancel = false;
    struct Pending
    {
        std::string method;
        ReverseCapability::Verified delegated;
        std::string wire;
        std::optional<uint32_t> length;
    };
    ReverseInbound inbound;
    std::function<bool(std::string_view, uint64_t)> expects;
    std::unordered_map<uint64_t, Pending> requests;
    std::optional<Ready> ready;
    std::optional<Upload> uploadReady;
    std::unordered_map<uint64_t, std::shared_ptr<ReverseUploadPipe>> uploads;
};

} // namespace nixgrpc
