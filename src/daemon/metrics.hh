#pragma once
// Prometheus metrics, served on --metrics-listen.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
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

    prometheus::Family<prometheus::Histogram> * phases =
        &prometheus::BuildHistogram()
             .Name("nix_grpc_phase_seconds")
             .Help("Wall time of one phase of an RPC, by method and phase")
             .Register(*registry);
    prometheus::Family<prometheus::Gauge> * inflight =
        &prometheus::BuildGauge()
             .Name("nix_grpc_inflight")
             .Help("Things currently held or awaited, by kind")
             .Register(*registry);
    prometheus::Family<prometheus::Counter> * events =
        &prometheus::BuildCounter().Name("nix_grpc_events_total").Help("Scheduling events, by kind").Register(*registry);
    prometheus::Family<prometheus::Gauge> * sched =
        &prometheus::BuildGauge().Name("nix_grpc_sched").Help("Scheduler state on this node, by kind").Register(*registry);
    // Touched on every scheduler message; Family::Add hashes the label map each time.
    prometheus::Gauge * schedQueuedGauge = &sched->Add({{"kind", "queued"}});
    prometheus::Gauge * schedWorkersGauge = &sched->Add({{"kind", "workers"}});

    // NOLINTNEXTLINE(*-magic-numbers)
    prometheus::Histogram::BucketBoundaries buckets{0.05, 0.25, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600};

public:
    // Times consecutive phases of one RPC: the current phase ends at next(), done() or destruction.
    class Phase
    {
        Metrics & metrics;
        std::string method;
        prometheus::Histogram * hist = nullptr;
        std::chrono::steady_clock::time_point start;

    public:
        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): method, then phase.
        Phase(Metrics & metrics, std::string method, const std::string & phase)
            : metrics(metrics)
            , method(std::move(method))
        {
            next(phase);
        }
        Phase(const Phase &) = delete;
        Phase(Phase &&) = delete;
        auto operator=(const Phase &) -> Phase & = delete;
        auto operator=(Phase &&) -> Phase & = delete;
        ~Phase()
        {
            done();
        }

        void next(const std::string & phase)
        {
            done();
            hist = &metrics.phases->Add({{"method", method}, {"phase", phase}}, metrics.buckets);
            start = std::chrono::steady_clock::now();
        }

        void done()
        {
            if (hist != nullptr) {
                hist->Observe(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
                hist = nullptr;
            }
        }
    };

    // Increments a gauge for its lifetime.
    class Held
    {
        prometheus::Gauge * gauge;

    public:
        Held(Metrics & metrics, const std::string & kind)
            : gauge(&metrics.inflight->Add({{"kind", kind}}))
        {
            gauge->Increment();
        }
        Held(const Held &) = delete;
        Held(Held &&) = delete;
        auto operator=(const Held &) -> Held & = delete;
        auto operator=(Held &&) -> Held & = delete;
        ~Held()
        {
            gauge->Decrement();
        }
    };

    auto inflightNow(const std::string & kind) -> double
    {
        return inflight->Add({{"kind", kind}}).Value();
    }

    void event(const std::string & kind)
    {
        events->Add({{"kind", kind}}).Increment();
    }
    // For hot paths: resolve the label once, Increment() is then lock-free.
    auto eventCounter(const std::string & kind) -> prometheus::Counter &
    {
        return events->Add({{"kind", kind}});
    }

    // Join target for dashboards; pod names are not stable on Kubernetes.
    void buildInfo(
        const std::string & version, const std::string & worker, const std::string & system, const std::string & features)
    {
        prometheus::BuildGauge()
            .Name("nix_grpc_build_info")
            .Help("Constant 1, labelled with daemon version, --worker-name, system and system-features")
            .Register(*registry)
            .Add({{"version", version}, {"worker", worker}, {"system", system}, {"features", features}})
            .Set(1);
    }

    void buildSlots(unsigned count)
    {
        prometheus::BuildGauge()
            .Name("nix_grpc_build_slots")
            .Help("Configured concurrent builds (--max-jobs)")
            .Register(*registry)
            .Add({})
            .Set(count);
    }

    void schedQueued(size_t count) const
    {
        schedQueuedGauge->Set(static_cast<double>(count));
    }
    [[nodiscard]] auto schedQueuedNow() const -> double
    {
        return schedQueuedGauge->Value();
    }
    void schedWorkers(size_t count) const
    {
        schedWorkersGauge->Set(static_cast<double>(count));
    }
    [[nodiscard]] auto schedWorkersNow() const -> double
    {
        return schedWorkersGauge->Value();
    }
    void schedClients(int delta) const
    {
        auto & gauge = sched->Add({{"kind", "clients"}});
        gauge.Set(gauge.Value() + delta);
    }

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
