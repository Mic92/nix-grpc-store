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
        uint64_t id;
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
    [[nodiscard]] auto accept(const nix::remote::ReverseFarmMsg & frame) -> bool
    {
        std::scoped_lock lock(mutex);
        ready.reset();
        rejection = "";
        auto event = inbound.accept(frame);
        if (!event) { rejection = inbound.lastRejection(); return false; }
        auto const id = event->streamId;
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
                    [check = expects, drv = delegated.drvPath, assign = delegated.assignId] {
                        return check(drv, assign);
                    });
                uploads.emplace(id, pipe);
                uploadReady = Upload{.id = id, .delegated = std::move(delegated), .pipe = std::move(pipe)};
                return true;
            }
            if (frame.open().method() != "BuildDerivation") { rejection = "method not served"; return false; }
            requests.emplace(id, Pending{.method = frame.open().method(), .delegated = std::move(*event->caller)});
            return true;
        case ReverseInbound::Event::Kind::data: {
            if (auto it = uploads.find(id); it != uploads.end())
                return it->second->feed(frame.data().data());
            auto & pending = requests.at(id);
            auto const size = frame.data().data().size();
            if (pending.wire.size() > ReverseRelay::maxRequestBytes + 4
                || size > ReverseRelay::maxRequestBytes + 4 - pending.wire.size()) { rejection = "request too large"; return false; }
            pending.wire += frame.data().data();
            if (!pending.length && pending.wire.size() >= 4) {
                uint32_t length = 0;
                for (unsigned i = 0; i != 4; ++i)
                    length |= static_cast<uint32_t>(static_cast<unsigned char>(pending.wire[i])) << (8 * i);
                if (length > ReverseRelay::maxRequestBytes) { rejection = "request too large"; return false; }
                pending.length = length;
            }
            return !pending.length || pending.wire.size() <= static_cast<size_t>(*pending.length) + 4;
        }
        case ReverseInbound::Event::Kind::halfClose: {
            if (auto it = uploads.find(id); it != uploads.end()) return it->second->finish();
            auto it = requests.find(id);
            if (it == requests.end() || !it->second.length || it->second.wire.size() != *it->second.length + 4) {
                rejection = "incomplete request at half-close";
                return false;
            }
            auto pending = std::move(it->second);
            requests.erase(it);
            pending.wire.erase(0, 4);
            ready = Ready{.id = id, .method = std::move(pending.method),
                          .delegated = std::move(pending.delegated), .payload = std::move(pending.wire)};
            return true;
        }
        case ReverseInbound::Event::Kind::cancel:
            if (auto it = uploads.find(id); it != uploads.end()) {
                it->second->cancel();
                uploads.erase(it);
            }
            if (uploadReady && uploadReady->id == id) uploadReady.reset();
            requests.erase(id);
            if (ready && ready->id == id) ready.reset();
            return true;
        }
        return false;
    }

    // Why the last accept() returned false; for logs only.
    [[nodiscard]] auto lastRejection() const -> const char * { std::scoped_lock lock(mutex); return rejection; }

    [[nodiscard]] auto takeReady() -> std::optional<Ready> { std::scoped_lock lock(mutex); return std::exchange(ready, std::nullopt); }
    [[nodiscard]] auto takeUploadReady() -> std::optional<Upload> { std::scoped_lock lock(mutex); return std::exchange(uploadReady, std::nullopt); }

    // Reader/writer disconnect must unblock a streaming importer even when
    // the registration reader itself is stuck in a gRPC Read.
    void cancelUploads()
    {
        std::scoped_lock lock(mutex);
        for (auto & [id, pipe] : uploads) pipe->cancel();
    }

    // Call before a local BuildDerivation handler and recheck Builder::admit
    // immediately before execution. The request itself cannot grant a role.
    [[nodiscard]] static auto matchesBuild(const Ready & call, const nix::remote::BuildDerivationRequest & request) -> bool
    {
        return call.method == "BuildDerivation" && ReverseCapability::matchesBuild(call.delegated, request);
    }

    [[nodiscard]] auto finished(uint64_t id) -> bool {
        std::scoped_lock lock(mutex);
        uploads.erase(id);
        return inbound.finished(id);
    }

    void close() {
        std::scoped_lock lock(mutex);
        for (auto & [id, pipe] : uploads) pipe->cancel();
        uploads.clear();
        uploadReady.reset();
        inbound.close(); requests.clear(); ready.reset();
    }

private:
    mutable std::mutex mutex;
    const char * rejection = "";
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
