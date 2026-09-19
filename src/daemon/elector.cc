#include "elector.hh"
#include "health.grpc.pb.h"
#include "health.pb.h"
#include "logfmt.hh"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace nixgrpc {

namespace {
constexpr std::chrono::milliseconds pollEvery{1000};
constexpr std::chrono::milliseconds checkDeadline{800};
// A predecessor must be gone this many polls in a row before we take over,
// so one slow health reply does not flap the farm.
constexpr int takeOverAfter = 3;
} // namespace

Elector::Elector(
    const std::vector<std::string> & predecessors,
    const std::shared_ptr<grpc::ChannelCredentials> & creds,
    std::function<void(bool)> onChange)
    : onChange(std::move(onChange))
{
    for (const auto & addr : predecessors) {
        preds.push_back(grpc::health::v1::Health::NewStub(grpc::CreateChannel(addr, creds)));
    }
    if (!preds.empty()) {
        poller = std::jthread([this](const std::stop_token & stop) -> void { run(stop); });
    }
}

Elector::~Elector()
{
    if (poller.joinable()) {
        poller.request_stop();
        poller.join();
    }
}

auto Elector::anyPredecessorServing() -> bool
{
    using grpc::health::v1::HealthCheckRequest;
    using grpc::health::v1::HealthCheckResponse;
    for (const auto & stub : preds) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + checkDeadline);
        HealthCheckRequest req;
        req.set_service("nix.scheduler");
        HealthCheckResponse resp;
        if (stub->Check(&ctx, req, &resp).ok() && resp.status() == HealthCheckResponse::SERVING) {
            return true;
        }
    }
    return false;
}

void Elector::run(const std::stop_token & stop)
{
    std::mutex mtx;
    std::condition_variable_any wake;
    int quietPolls = 0;
    bool active = false;
    while (!stop.stop_requested()) {
        const bool serving = anyPredecessorServing();
        quietPolls = serving ? 0 : quietPolls + 1;
        if (active && serving) {
            active = false;
            logLine(LogLevel::info, {{"event", "scheduler_yield"}});
            onChange(false);
        } else if (!active && quietPolls >= takeOverAfter) {
            active = true;
            logLine(LogLevel::info, {{"event", "scheduler_take_over"}});
            onChange(true);
        }
        std::unique_lock lock(mtx);
        wake.wait_for(lock, stop, pollEvery, []() -> bool { return false; });
    }
}

} // namespace nixgrpc
