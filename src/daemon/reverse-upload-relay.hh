#pragma once
// Assignment-pinned, bounded gateway upload. Only a worker's terminal status
// can acknowledge an import; the scheduler never imports these bytes locally.

#include <algorithm>
#include <chrono>
#include <condition_variable>
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

    [[nodiscard]] auto call(grpc::ServerContext & context, const Select & select, const Assigned & assigned,
                            const Read & read, nix::remote::UploadBuildInputsChunk first) const -> grpc::Status
    {
        if (!select || !assigned || !read || first.ByteSizeLong() > ReverseUpload::maxChunkBytes
            || !first.has_chunk() || first.drv_path().empty() || first.assign_id() == 0)
            return {grpc::StatusCode::INVALID_ARGUMENT, "invalid build-input upload header"};
        auto selection = select(1);
        if (!selection.status.ok()) return selection.status;
        if (!selection.prepared || !selection.prepared->session || !selection.prepared->session->transport)
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload session unavailable"};
        auto session = selection.prepared->session;
        if (!assigned()) return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment changed"};
        auto const id = session->transport->nextStreamId.fetch_add(1);
        if (id == 0) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "reverse stream IDs exhausted"};
        selection = select(id);
        if (!selection.status.ok()) return selection.status;
        if (!selection.prepared || selection.prepared->session != session)
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload worker changed during selection"};
        auto delegated = ReverseCapability::authorizeOpen(session->sessionKey, selection.prepared->open,
                                                           session->workerId, session->generation, id);
        if (!delegated || !ReverseUpload::first(*delegated, first)
            || selection.prepared->drvPath != first.drv_path()
            || selection.prepared->assignId != first.assign_id())
            return {grpc::StatusCode::PERMISSION_DENIED, "invalid delegated upload"};
        if (context.IsCancelled()) return {grpc::StatusCode::CANCELLED, "upload client cancelled"};
        auto & transport = *session->transport;
        auto status = gateway.enqueue(std::move(*selection.prepared), id);
        if (!status.ok()) return status;
        struct Release {
            ReverseTransport & transport;
            uint64_t id;
            bool completed = false;
            ~Release() {
                if (!completed) (void) transport.outgoing.cancel(id);
                (void) transport.outgoing.finished(id);
                (void) transport.responses.cancel(id, !completed);
            }
        } release{transport, id};

        auto const deadline = std::min(context.deadline(), std::chrono::system_clock::now() + std::chrono::hours(24));
        std::stop_source wake;
        std::mutex mutex;
        std::condition_variable_any cv;
        std::jthread monitor([&](std::stop_token stop) {
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
                cv.wait_for(lock, stop, std::chrono::milliseconds(25), [] { return false; });
            }
        });
        auto const live = [&] {
            return !wake.stop_requested() && !context.IsCancelled() && registry.current(session) && assigned();
        };
        auto send = [&](const nix::remote::UploadBuildInputsChunk & chunk) -> bool {
            auto const size = chunk.ByteSizeLong();
            if (size == 0 || size > ReverseUpload::maxChunkBytes) return false;
            auto wire = chunk.SerializeAsString();
            if (wire.empty() || wire.size() > ReverseUpload::maxChunkBytes) return false;
            std::string prefix(4, '\0');
            for (unsigned i = 0; i < 4; ++i) prefix[i] = static_cast<char>((wire.size() >> (8 * i)) & 0xff);
            if (!live() || !transport.outgoing.waitData(id, prefix, wake.get_token())) return false;
            for (size_t offset = 0; offset < wire.size(); offset += ReverseFrames::maxFrameBytes)
                if (!live() || !transport.outgoing.waitData(id,
                        std::string_view(wire).substr(offset, ReverseFrames::maxFrameBytes), wake.get_token())) return false;
            return true;
        };
        if (!send(first)) return {grpc::StatusCode::UNAVAILABLE, "reverse upload interrupted"};
        nix::remote::UploadBuildInputsChunk next;
        while (read(next)) {
            if (!ReverseUpload::next(next)) return {grpc::StatusCode::INVALID_ARGUMENT, "invalid upload continuation"};
            if (!send(next)) return {grpc::StatusCode::UNAVAILABLE, "reverse upload interrupted"};
            next.Clear();
        }
        if (!live() || !transport.outgoing.halfClose(id))
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload closed"};
        nix::remote::ReverseWorkerMsg response;
        if (!transport.responses.waitTake(id, response, wake.get_token()) || !live())
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload session ended"};
        if (!response.has_finish()) return {grpc::StatusCode::DATA_LOSS, "unexpected upload response data"};
        if (!assigned()) return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment changed"};
        if (!transport.responses.finished(id))
            return {grpc::StatusCode::UNAVAILABLE, "reverse upload response stream lost"};
        release.completed = true;
        return {static_cast<grpc::StatusCode>(response.finish().code()), response.finish().message()};
    }

private:
    ReverseGateway & gateway;
    ReverseRegistry & registry;
};

} // namespace nixgrpc
