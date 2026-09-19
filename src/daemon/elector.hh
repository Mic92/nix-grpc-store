#pragma once
// Static-order scheduler selection: this node serves scheduler streams only
// while no node before it in --scheduler-order does. No shared state; the
// balancer routes to the first healthy one with the same list as priorities.

#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/security/credentials.h>

#include "health.grpc.pb.h"

namespace nixgrpc {

class Elector
{
public:
    // onChange(true) = become active, onChange(false) = step down. Called from
    // the poll thread, never concurrently with itself. Starts passive; with no
    // predecessors nothing is ever called (the caller starts active).
    Elector(
        const std::vector<std::string> & predecessors,
        const std::shared_ptr<grpc::ChannelCredentials> & creds,
        std::function<void(bool)> onChange);
    ~Elector();
    Elector(const Elector &) = delete;
    auto operator=(const Elector &) -> Elector & = delete;
    Elector(Elector &&) = delete;
    auto operator=(Elector &&) -> Elector & = delete;

private:
    std::vector<std::unique_ptr<grpc::health::v1::Health::Stub>> preds;
    std::function<void(bool)> onChange;
    std::jthread poller;
    void run(const std::stop_token & stop);
    auto anyPredecessorServing() -> bool;
};

} // namespace nixgrpc
