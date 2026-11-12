// The scheduler (Dispatcher) and the workers' side of its protocol (Builder),
// as the daemon runs them, joined by queues instead of gRPC streams. A seed
// picks the order in which messages arrive, clients call workers, builds end,
// sessions break and the scheduler restarts. After every step the rules below
// are checked.
//
// Usage: farm-sim-test [first-seed [seeds]]

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <exception>
#include <format>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <print>
#include <random>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "builder.hh"
#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

namespace {

using nixgrpc::Builder;
using nixgrpc::Dispatcher;

constexpr uint32_t slots = 1;
constexpr size_t workerCount = 2;
constexpr size_t clientCount = 2;
constexpr unsigned stepsPerSeed = 120;
constexpr unsigned faultsPerSeed = 2;
constexpr uint64_t defaultSeeds = 3000;

enum class Rule : uint8_t { slots, noSteal };

struct Violation
{
    Rule rule;
    std::string what;
};

struct SimWorker
{
    std::string addr;
    Builder builder;
    std::unique_ptr<Dispatcher::Worker> session;
    std::deque<nix::remote::SchedCmd> down;
    std::deque<nix::remote::WorkerMsg> up;
    std::set<std::string> building; // a client's call runs the build
    std::set<std::string> stopping; // of those, the ones a Revoke reached

    SimWorker(std::string addr, nixgrpc::Metrics & metrics)
        : addr(addr)
        , builder({.advertise = std::move(addr), .systems = {"x"}, .features = {}, .maxJobs = slots}, metrics)
    {
    }
};

enum class Follow : uint8_t { wanted, assigned, building, finished };

struct SimClient
{
    Dispatcher::ClientPtr stream;
    std::deque<nix::remote::SchedMsg> down;
    std::deque<nix::remote::ClientMsg> up;

    struct Job
    {
        Follow state = Follow::wanted;
        size_t worker = 0;
    };

    std::map<std::string, Job> jobs;
};

struct Step
{
    std::string name;
    std::function<void()> run;
};

void add(std::vector<Step> & steps, std::string name, std::function<void()> run)
{
    steps.push_back({.name = std::move(name), .run = std::move(run)});
}

class Sim
{
public:
    Sim()
    {
        for (size_t idx = 0; idx < workerCount; idx++) {
            workers.push_back(std::make_unique<SimWorker>("w:" + std::to_string(idx), metrics));
        }
        for (size_t idx = 0; idx < clientCount; idx++) {
            clients.push_back(std::make_unique<SimClient>());
        }
        startScheduler();
    }

    // The steps that can happen now.
    auto enabled() -> std::vector<Step>
    {
        std::vector<Step> steps;
        for (size_t idx = 0; idx < clients.size(); idx++) {
            clientSteps(steps, idx);
        }
        for (size_t idx = 0; idx < workers.size(); idx++) {
            workerSteps(steps, idx);
        }
        if (faults < faultsPerSeed) {
            add(steps, "scheduler restarts", [this] -> void { restartScheduler(); });
        }
        return steps;
    }

    [[nodiscard]] auto check() const -> std::optional<Violation>
    {
        if (violation) {
            return violation;
        }
        for (const auto & wkr : workers) {
            if (wkr->builder.holding() > slots + wkr->stopping.size()) {
                return Violation{.rule = Rule::slots, .what = wkr->addr + " holds more builds than it has slots"};
            }
        }
        return std::nullopt;
    }

private:
    nixgrpc::Metrics metrics{""};
    std::unique_ptr<Dispatcher> disp;
    std::vector<std::unique_ptr<SimWorker>> workers;
    std::vector<std::unique_ptr<SimClient>> clients;
    unsigned faults = 0;
    std::optional<Violation> violation;

    void startScheduler()
    {
        disp = std::make_unique<Dispatcher>(
            Dispatcher::Config{.defaultSystem = "x", .present = {}, .lookupThreads = 0}, metrics);
    }

    void restartScheduler()
    {
        faults++;
        for (auto & wkr : workers) {
            dropSession(*wkr);
        }
        for (auto & cli : clients) {
            cli->stream.reset();
            cli->down.clear();
            cli->up.clear();
        }
        startScheduler();
    }

    static void dropSession(SimWorker & wkr)
    {
        wkr.builder.setSend({});
        wkr.session.reset();
        wkr.down.clear();
        wkr.up.clear();
    }

    static void connect(SimWorker & wkr)
    {
        wkr.session = std::make_unique<Dispatcher::Worker>(Dispatcher::Worker{
            .send = [&wkr](const nix::remote::SchedCmd & cmd) -> bool {
                wkr.down.push_back(cmd);
                return true;
            },
            .abort = [] -> void {},
            .id = {}});
        wkr.builder.setSend([&wkr](const nix::remote::WorkerMsg & msg) -> bool {
            wkr.up.push_back(msg);
            return true;
        });
        wkr.builder.greet();
    }

    void connect(SimClient & cli)
    {
        cli.stream = disp->connectClient(
            [&cli](const nix::remote::SchedMsg & msg) -> bool {
                cli.down.push_back(msg);
                return true;
            },
            {});
        for (const auto & [drv, job] : cli.jobs) {
            if (job.state != Follow::finished) {
                want(cli, drv);
            }
        }
    }

    static void want(SimClient & cli, const std::string & drv)
    {
        auto * wnt = cli.up.emplace_back().mutable_want();
        wnt->set_drv_path(drv);
        wnt->set_system("x");
    }

    void clientSteps(std::vector<Step> & steps, size_t idx)
    {
        auto & cli = *clients.at(idx);
        auto const who = "client " + std::to_string(idx);
        if (!cli.stream) {
            add(steps, who + " connects", [this, &cli] -> void { connect(cli); });
            return;
        }
        for (const std::string drv : {"a.drv", "b.drv"}) {
            if (!cli.jobs.contains(drv)) {
                add(steps, std::format("{} wants {}", who, drv), [&cli, drv] -> void {
                                     cli.jobs.emplace(drv, SimClient::Job{});
                                     want(cli, drv);
                                 });
            }
        }
        if (!cli.up.empty()) {
            add(steps, "scheduler reads from " + who, [this, &cli] -> void {
                                 nix::remote::ClientMsgs msgs;
                                 *msgs.add_msgs() = cli.up.front();
                                 cli.up.pop_front();
                                 disp->clientMsgs(cli.stream, msgs);
                             });
        }
        if (!cli.down.empty()) {
            add(steps, who + " reads", [this, &cli] -> void { clientReads(cli); });
        }
        for (auto & [drv, job] : cli.jobs) {
            if (job.state == Follow::assigned) {
                add(steps, std::format("{} calls {} for {}", who, workers.at(job.worker)->addr, drv),
                                 [this, &cli, &job, drv] -> void { call(cli, job, drv); });
            }
        }
    }

    void clientReads(SimClient & cli)
    {
        auto const msg = cli.down.front();
        cli.down.pop_front();
        if (!msg.has_assigned()) {
            return;
        }
        auto const job = cli.jobs.find(msg.assigned().drv_path());
        if (job == cli.jobs.end() || job->second.state != Follow::wanted) {
            return;
        }
        for (size_t idx = 0; idx < workers.size(); idx++) {
            if (workers.at(idx)->addr == msg.assigned().worker_addr()) {
                job->second = {.state = Follow::assigned, .worker = idx};
            }
        }
    }

    // BuildDerivation, as main.cc admits it.
    void call(SimClient & cli, SimClient::Job & job, const std::string & drv)
    {
        auto & wkr = *workers.at(job.worker);
        wkr.builder.claim(drv);
        auto const adm = wkr.builder.admit(drv);
        if (!adm) {
            job.state = Follow::wanted;
            if (cli.stream) {
                want(cli, drv);
            }
            return;
        }
        job.state = Follow::building;
        if (!adm->attach) {
            wkr.building.insert(drv);
        }
    }

    void workerSteps(std::vector<Step> & steps, size_t idx)
    {
        auto & wkr = *workers.at(idx);
        if (!wkr.session) {
            add(steps, wkr.addr + " connects", [&wkr] -> void { connect(wkr); });
        } else {
            if (faults < faultsPerSeed) {
                add(steps, wkr.addr + " loses its session", [this, &wkr] -> void {
                                     faults++;
                                     disp->workerGone(*wkr.session);
                                     dropSession(wkr);
                                 });
            }
            if (!wkr.up.empty()) {
                add(steps, "scheduler reads from " + wkr.addr, [this, &wkr] -> void {
                                     nix::remote::WorkerMsgs msgs;
                                     *msgs.add_msgs() = wkr.up.front();
                                     wkr.up.pop_front();
                                     disp->workerMsgs(*wkr.session, msgs);
                                 });
            }
            if (!wkr.down.empty()) {
                add(steps, wkr.addr + " reads", [this, idx] -> void { workerReads(idx); });
            }
        }
        for (const auto & drv : wkr.building) {
            add(steps, wkr.addr + " ends " + drv, [this, idx, drv] -> void { buildEnds(idx, drv); });
        }
    }

    void workerReads(size_t idx)
    {
        auto & wkr = *workers.at(idx);
        auto const cmd = wkr.down.front();
        wkr.down.pop_front();
        if (cmd.has_expect()) {
            wkr.builder.onExpect(cmd.expect());
        } else if (cmd.has_revoke()) {
            auto const & drv = cmd.revoke().drv_path();
            if (wkr.building.contains(drv)) {
                wkr.stopping.insert(drv);
                if (!builtElsewhere(idx, drv)) {
                    violation = Violation{
                        .rule = Rule::noSteal,
                        .what = "the build of " + drv + " on " + wkr.addr + " is revoked and no other worker builds it"};
                }
            }
            wkr.builder.onRevoke(cmd.revoke());
        }
    }

    [[nodiscard]] auto builtElsewhere(size_t idx, const std::string & drv) const -> bool
    {
        for (size_t other = 0; other < workers.size(); other++) {
            if (other != idx && workers.at(other)->building.contains(drv)) {
                return true;
            }
        }
        return false;
    }

    // A revoked build ends cancelled and its callers ask again.
    void buildEnds(size_t idx, const std::string & drv)
    {
        auto & wkr = *workers.at(idx);
        bool const revoked = wkr.stopping.erase(drv) != 0;
        wkr.building.erase(drv);
        if (revoked) {
            wkr.builder.finished(drv, nix::remote::Done::CANCELLED, {}, "");
        } else {
            wkr.builder.finished(drv, nix::remote::Done::BUILT, {{"/nix/store/" + drv + "-out", 1}}, "result");
        }
        for (auto & cli : clients) {
            auto const job = cli->jobs.find(drv);
            if (job == cli->jobs.end() || job->second.state != Follow::building || job->second.worker != idx) {
                continue;
            }
            job->second.state = revoked ? Follow::wanted : Follow::finished;
            if (revoked && cli->stream) {
                want(*cli, drv);
            }
        }
    }
};

// The seed whose run breaks `rule`, with the steps that led there.
auto firstViolation(Rule rule, uint64_t first, uint64_t seeds) -> std::optional<std::pair<uint64_t, std::string>>
{
    for (uint64_t seed = first; seed < first + seeds; seed++) {
        std::mt19937_64 rng(seed);
        Sim sim;
        std::string trace;
        for (unsigned step = 0; step < stepsPerSeed; step++) {
            auto steps = sim.enabled();
            if (steps.empty()) {
                break;
            }
            auto const & pick = steps.at(rng() % steps.size());
            trace += "  " + pick.name + "\n";
            pick.run();
            if (auto const bad = sim.check(); bad && bad->rule == rule) {
                return std::pair{seed, trace + "  => " + bad->what + "\n"};
            }
            if (sim.check()) {
                break; // another rule broke; this run says nothing more about ours
            }
        }
    }
    return std::nullopt;
}

struct Row
{
    Rule rule;
    const char * name;
    // The daemon breaks this rule today. The row then demands a seed that
    // shows it, so the entry cannot outlive the bug.
    bool broken;
};

constexpr auto rows = std::to_array<Row>({
    {.rule = Rule::slots, .name = "no worker holds more builds than it has slots", .broken = false},
    {.rule = Rule::noSteal, .name = "a build a client started is not revoked for a reserved slot", .broken = true},
});

auto run(std::span<char *> args) -> int
{
    // Dispatcher and Builder log every event to stderr.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    int const null = ::open("/dev/null", O_WRONLY | O_CLOEXEC);
    if (null < 0 || ::dup2(null, STDERR_FILENO) < 0) {
        return EXIT_FAILURE;
    }
    ::close(null);
    uint64_t const first = args.size() > 1 ? std::stoull(args.subspan(1).front()) : 1;
    uint64_t const seeds = args.size() > 2 ? std::stoull(args.subspan(2).front()) : defaultSeeds;
    int status = EXIT_SUCCESS;
    for (const auto & row : rows) {
        auto const found = firstViolation(row.rule, first, seeds);
        if (found && !row.broken) {
            std::print("FAIL {}\nseed {}:\n{}", row.name, found->first, found->second);
            status = EXIT_FAILURE;
        } else if (!found && row.broken) {
            std::println("FAIL {}\nis listed as broken, but no seed breaks it", row.name);
            status = EXIT_FAILURE;
        } else if (found) {
            std::println("known broken (seed {}): {}", found->first, row.name);
        } else {
            std::println("ok: {}", row.name);
        }
    }
    return status;
}

} // namespace

auto main(int argc, char ** argv) -> int
{
    try {
        return run({argv, static_cast<size_t>(argc)});
    } catch (const std::exception & err) {
        (void) std::fputs(err.what(), stdout);
        return EXIT_FAILURE;
    }
}
