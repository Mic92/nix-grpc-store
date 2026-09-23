#pragma once

#include <atomic>
#include <flat_map>
#include <memory>
#include <string>

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <google/protobuf/any.pb.h>

#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>

#include "auth.hh"
#include "dispatcher.hh"
#include "eds.grpc.pb.h"

namespace nixgrpc {

class EdsService final : public envoy::service::endpoint::v3::EndpointDiscoveryService::CallbackService
{
public:
    static constexpr unsigned maxStreams = 64;

    EdsService(Dispatcher & dispatcher, Auth & auth);

    auto StreamEndpoints(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<envoy::service::endpoint::v3::DiscoveryRequest, envoy::service::endpoint::v3::DiscoveryResponse> *
        override;

    void setActive(bool active)
    {
        active_ = active;
    }
    [[nodiscard]] auto active() const -> bool
    {
        return active_;
    }

    auto acquire() -> bool;
    void release();

    // One serialized ClusterLoadAssignment per system, shared by all streams.
    using Rendered = std::flat_map<std::string, google::protobuf::Any>;
    auto render(const std::shared_ptr<const Dispatcher::Membership> & members) -> std::shared_ptr<const Rendered>;

private:
    Dispatcher * dispatcher;
    Auth * auth;
    std::atomic<bool> active_{true};
    std::atomic<unsigned> open_{0};
    absl::Mutex renderMutex;
    std::shared_ptr<const Dispatcher::Membership> renderedFrom ABSL_GUARDED_BY(renderMutex);
    std::shared_ptr<const Rendered> rendered ABSL_GUARDED_BY(renderMutex);
};

} // namespace nixgrpc
