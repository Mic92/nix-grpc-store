#pragma once
// Per-worker farm state: niks3 client, the push child, build slots and disk health.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <semaphore>
#include <string>

#include <grpcpp/server.h>

#include <nix/util/environment-variables.hh>
#include <nix/util/file-system.hh>
#include <nix/util/util.hh>

#include "claim.hh"
#include "push.hh"

namespace nixgrpc {

struct FarmConfig
{
    std::string niks3Url;
    std::string niks3TokenFile;
    PushProcess::Argv pushArgv;
    unsigned maxJobs = 1;
    uint64_t minFree = 0;
    std::string storeDir = nix::getEnv("NIX_STORE_DIR").value_or("/nix/store");
};

struct Farm
{
    Niks3 niks3;
    PushProcess push;
    std::counting_semaphore<> slots;
    uint64_t minFree;
    std::string storeDir;
    std::atomic<bool> healthy{true}; // enough disk, mirrored to gRPC health

    explicit Farm(const FarmConfig & cfg)
        : niks3(cfg.niks3Url, nix::chomp(nix::readFile(cfg.niks3TokenFile)))
        , push(cfg.pushArgv)
        , slots(static_cast<std::ptrdiff_t>(cfg.maxJobs))
        , minFree(cfg.minFree)
        , storeDir(cfg.storeDir)
    {
    }

    void updateHealth(grpc::Server & server);
};

// One of the worker's max-jobs, released when the pointer drops.
struct SlotRelease
{
    void operator()(std::counting_semaphore<> * sem) const
    {
        sem->release();
    }
};
using Slot = std::unique_ptr<std::counting_semaphore<>, SlotRelease>;

inline auto acquireSlot(std::counting_semaphore<> & sem) -> Slot
{
    sem.acquire();
    return Slot(&sem);
}

inline auto tryAcquireSlot(std::counting_semaphore<> & sem) -> Slot
{
    return Slot(sem.try_acquire() ? &sem : nullptr);
}


} // namespace nixgrpc
