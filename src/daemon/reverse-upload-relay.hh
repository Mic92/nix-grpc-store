#pragma once
// Assignment-pinned, bounded gateway upload. Only a worker's terminal status
// can acknowledge an import; the scheduler never imports these bytes locally.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include <grpcpp/server_context.h>

#include "reverse-gateway.hh"
#include "reverse-upload.hh"

namespace nixgrpc {

class ReverseUploadRelay
{
public:
    using Select = std::function<ReverseGateway::Result(uint64_t)>;
    using Assigned = std::function<bool()>;
    using Read = std::function<bool(nix::remote::UploadBuildInputsChunk &)>;

    ReverseUploadRelay(ReverseGateway & gateway, ReverseRegistry & registry)
        : gateway(gateway), registry(registry) { }

    // NOLINTNEXTLINE(readability-function-cognitive-complexity): one sequential, fail-closed upload protocol.
    [[nodiscard]] auto call(grpc::ServerContext & context, const Select & select, const Assigned & assigned,
                            const Read & read, const nix::remote::UploadBuildInputsChunk & first) const -> grpc::Status
    {
        if (!select || !assigned || !read || first.ByteSizeLong() > ReverseUpload::maxChunkBytes
            || !first.has_chunk() || first.drv_path().empty() || first.assign_id() == 0) {
            return {grpc::StatusCode::INVALID_ARGUMENT, "invalid build-input upload header"};
        }
        auto selection = select(1);
        if (!selection.status.ok()) {
            return selection.status;
        }
        if (!selection.prepared || !selection.prepared->session || !selection.prepared->session->transport) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload session unavailable"};
        }
        auto session = selection.prepared->session;
        if (!assigned()) {
            return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment changed"};
        }
        auto const streamId = session->transport->nextStreamId.fetch_add(1);
        if (streamId == 0) {
            return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse stream IDs exhausted"};
        }
        selection = select(streamId);
        if (!selection.status.ok()) {
            return selection.status;
        }
        if (!selection.prepared || selection.prepared->session != session) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload worker changed during selection"};
        }
        auto delegated = ReverseCapability::authorizeOpen(session->sessionKey, selection.prepared->open,
                                                           session->workerId, session->generation, streamId);
        if (!delegated || !ReverseUpload::first(*delegated, first)
            || selection.prepared->drvPath != first.drv_path()
            || selection.prepared->assignId != first.assign_id()) {
            return {grpc::StatusCode::PERMISSION_DENIED, "invalid delegated upload"};
        }
        if (context.IsCancelled()) {
            return {grpc::StatusCode::CANCELLED, "upload client cancelled"};
        }
        auto & transport = *session->transport;
        auto status = gateway.enqueue(std::move(*selection.prepared), streamId);
        if (!status.ok()) {
            return status;
        }
        struct Release {
            // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): scope guard over the live session's transport.
            ReverseTransport & transport;
            uint64_t streamId{};
            bool completed = false;
            Release(ReverseTransport & owner, uint64_t reserved) : transport(owner), streamId(reserved) { }
            Release(const Release &) = delete;
            Release(Release &&) = delete;
            auto operator=(const Release &) -> Release & = delete;
            auto operator=(Release &&) -> Release & = delete;
            // NOLINTNEXTLINE(bugprone-exception-escape): only allocation failure can throw here; terminating is fine.
            ~Release() {
                if (!completed) {
                    (void) transport.outgoing.cancel(streamId);
                }
                (void) transport.outgoing.finished(streamId);
                (void) transport.responses.cancel(streamId, !completed);
            }
        } release{transport, streamId};

        auto const deadline = std::min(context.deadline(), std::chrono::system_clock::now() + maxCallDuration);
        std::stop_source wake;
        std::mutex mutex;
        std::condition_variable_any wakeCv;
        std::jthread const monitor([&](const std::stop_token & stop) -> void {
            std::unique_lock lock(mutex);
            while (!stop.stop_requested()) {
                if (context.IsCancelled() || std::chrono::system_clock::now() >= deadline || !registry.current(session)
                    || !assigned()) {
                    wake.request_stop();
                    // Read() can be waiting for the client to send the next
                    // chunk even though its assignment has been revoked.
                    context.TryCancel();
                    return;
                }
                wakeCv.wait_for(lock, stop, monitorPollInterval, [] -> bool { return false; });
            }
        });
        auto const live = [&] -> bool {
            return !wake.stop_requested() && !context.IsCancelled() && registry.current(session) && assigned();
        };
        auto send = [&](const nix::remote::UploadBuildInputsChunk & chunk) -> bool {
            auto const size = chunk.ByteSizeLong();
            if (size == 0 || size > ReverseUpload::maxChunkBytes) {
                return false;
            }
            auto wire = chunk.SerializeAsString();
            if (wire.empty() || wire.size() > ReverseUpload::maxChunkBytes) {
                return false;
            }
            std::string prefix(4, '\0');
            for (unsigned i = 0; i < 4; ++i) {
                prefix.at(i) = static_cast<char>((wire.size() >> (bitsPerByte * i)) & byteMask);
            }
            if (!live() || !transport.outgoing.waitData(streamId, prefix, wake.get_token())) {
                return false;
            }
            for (size_t offset = 0; offset < wire.size(); offset += ReverseFrames::maxFrameBytes) {
                if (!live() || !transport.outgoing.waitData(streamId,
                        std::string_view(wire).substr(offset, ReverseFrames::maxFrameBytes), wake.get_token())) {
                    return false;
                }
            }
            return true;
        };
        if (!send(first)) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload interrupted"};
        }
        nix::remote::UploadBuildInputsChunk next;
        while (read(next)) {
            if (!ReverseUpload::next(next)) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "invalid upload continuation"};
            }
            if (!send(next)) {
                return {grpc::StatusCode::UNAVAILABLE, "reverse upload interrupted"};
            }
            next.Clear();
        }
        if (!live() || !transport.outgoing.halfClose(streamId)) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload closed"};
        }
        nix::remote::ReverseWorkerMsg response;
        if (!transport.responses.waitTake(streamId, response, wake.get_token()) || !live()) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload session ended"};
        }
        if (!response.has_finish()) {
            return {grpc::StatusCode::DATA_LOSS, "unexpected upload response data"};
        }
        if (!assigned()) {
            return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment changed"};
        }
        if (!transport.responses.finished(streamId)) {
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload response stream lost"};
        }
        release.completed = true;
        return {static_cast<grpc::StatusCode>(response.finish().code()), response.finish().message()};
    }

private:
    static constexpr unsigned bitsPerByte = 8;
    static constexpr size_t byteMask = 0xff;
    static constexpr std::chrono::hours maxCallDuration{24};
    static constexpr std::chrono::milliseconds monitorPollInterval{25};
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning; the daemon outlives every relay.
    ReverseGateway & gateway;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning; the daemon outlives every relay.
    ReverseRegistry & registry;
};

} // namespace nixgrpc
