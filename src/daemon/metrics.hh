#pragma once
// Per-client-CN Prometheus counters, served on --metrics-listen.

#include <cstdint>
#include <memory>
#include <string>

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

namespace nixgrpc {

class Metrics
{
    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();
    std::unique_ptr<prometheus::Exposer> exposer;

    prometheus::Family<prometheus::Counter> * rpcs =
        &prometheus::BuildCounter()
             .Name("nix_grpc_rpcs_total")
             .Help("RPCs handled, by method and client certificate CN")
             .Register(*registry);
    prometheus::Family<prometheus::Counter> * tunnelBytes =
        &prometheus::BuildCounter()
             .Name("nix_grpc_tunnel_bytes_total")
             .Help("Uncompressed bytes through the Connect tunnel, by direction and client certificate CN")
             .Register(*registry);
    prometheus::Family<prometheus::Counter> * narBytes =
        &prometheus::BuildCounter()
             .Name("nix_grpc_nar_bytes_total")
             .Help("Uncompressed NAR bytes imported/exported, by direction and client certificate CN")
             .Register(*registry);

public:
    // `listen` empty: keep counting but do not serve /metrics.
    explicit Metrics(const std::string & listen)
    {
        if (!listen.empty()) {
            exposer = std::make_unique<prometheus::Exposer>(listen);
            exposer->RegisterCollectable(registry);
        }
    }

    void countRpc(const std::string & method, const std::string & commonName)
    {
        rpcs->Add({{"method", method}, {"cn", commonName}}).Increment();
    }

    void countTunnelBytes(const std::string & commonName, uint64_t bytesIn, uint64_t bytesOut)
    {
        tunnelBytes->Add({{"direction", "in"}, {"cn", commonName}}).Increment(static_cast<double>(bytesIn));
        tunnelBytes->Add({{"direction", "out"}, {"cn", commonName}}).Increment(static_cast<double>(bytesOut));
    }

    void countNarBytes(const std::string & direction, const std::string & commonName, uint64_t bytes)
    {
        narBytes->Add({{"direction", direction}, {"cn", commonName}}).Increment(static_cast<double>(bytes));
    }
};

} // namespace nixgrpc
