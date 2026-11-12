#pragma once
// The worker's side of the scheduler protocol: the Expect table that gates
// BuildDerivation and the messages it sends back. No gRPC here, so tests
// drive the same code as the session does.

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nix/util/sync.hh>

#include "log-spill.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

using Clock = std::chrono::steady_clock;

// One assignment the builder accepted from its scheduler.
struct Expected
{
    uint64_t assignId = 0;
    Clock::time_point since;
    // A client reached us for it. The worker then holds the lease until Done,
    // a give-back or a Revoke: the scheduler revokes it if its client leaves.
    bool claimed = false;
    bool running = false;

    // Second BuildDerivation for the same drv attaches here.
    struct Shared
    {
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic<bool> revoked{false}; // scheduler moved the drv to a worker further along
        std::atomic<unsigned> attached{0};  // callers waiting in attach(); the build outlives its first caller for them
        bool finished = false;
        std::string resultWire; // BuildDerivationDone serialised
        // Written before `finished`.
        std::array<uint64_t, 3> settings{};
        LogSpill spill; // the owner's build log, replayed to whoever attaches

        // Owner only. Waking under the lock keeps an attacher that just
        // checked `written()` from sleeping through this record.
        void logged(SpillKind kind, std::string_view text)
        {
            spill.append(kind, text);
            const std::scoped_lock lock(mutex);
            cv.notify_all();
        }
    };

    std::shared_ptr<Shared> shared = std::make_shared<Shared>();
};

// Builder side: Expect table + outgoing session.
class Builder
{
public:
    // Delivers a message to the scheduler. False if no session is up.
    using SendFn = std::function<bool(const nix::remote::WorkerMsg &)>;

    // What the worker tells the scheduler about itself.
    struct Config
    {
        std::string advertise;
        std::vector<std::string> systems; // first is the native one
        std::vector<std::string> features;
        uint32_t maxJobs = 0;
        std::function<Clock::time_point()> now = Clock::now;
    };

    Builder(Config config, Metrics & metrics);

    // From the scheduler.
    void onExpect(const nix::remote::Expect & exp);
    void onRevoke(const nix::remote::Revoke & rev);

    // BuildDerivation entry: nullopt => FAILED_PRECONDITION. `attach` set if
    // the drv already runs. Caller then waits on it instead of building.
    struct Admission
    {
        std::shared_ptr<Expected::Shared> shared;
        uint64_t assignId = 0;
        bool attach = false;
    };

    // Any BuildDerivation for the drv, also one answered NOT_FOUND.
    void claim(const std::string & drvPath);
    auto admit(const std::string & drvPath) -> std::optional<Admission>;
    void finished(
        const std::string & drvPath,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        std::string resultWire,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    [[nodiscard]] auto holding() const -> size_t;

    // Sent on every (re)connect. Reports the builds a client came for and
    // forgets the Expects nobody claimed.
    auto hello() -> nix::remote::WorkerMsg;
    // False if the session is gone.
    auto greet() -> bool;
    void setDraining(bool draining);

    [[nodiscard]] auto draining() const -> bool
    {
        return draining_;
    }

    // Give back unclaimed Expects nobody came for. Called from the serve loop.
    void tick();

    // Swapped by the session thread on every (re)connect.
    void setSend(SendFn send);
    auto send(const nix::remote::WorkerMsg & msg) const -> bool;

private:
    Config config;
    Metrics & metrics;

    struct State
    {
        std::unordered_map<std::string, Expected> expected;
    };

    mutable nix::Sync<State> state;
    mutable nix::Sync<SendFn> sendFn;
    std::atomic<bool> draining_{false};
    static constexpr std::chrono::seconds expectTimeout{60};

    void sendDone(
        const std::string & drvPath,
        uint64_t assignId,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    void giveBack(const std::vector<std::pair<std::string, uint64_t>> & drvs, nix::remote::Done::Reason reason);
    auto takeUnclaimed(const std::function<bool(const Expected &)> & pred) -> std::vector<std::pair<std::string, uint64_t>>;
};

} // namespace nixgrpc
