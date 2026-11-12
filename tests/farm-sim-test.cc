// The scheduler (Dispatcher) and the workers' side of its protocol (Builder),
// as the daemon runs them, joined by queues instead of gRPC streams. A seed
// picks the order in which messages arrive, clients call workers, builds end,
// sessions break, the scheduler restarts and time passes. The rules below are
// checked after every step, the last one once the faults have stopped.
//
// Usage: farm-sim-test [first-seed [seeds]]

#include <algorithm>
#include <array>
#include <chrono>
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
#include <ratio>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

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
constexpr auto drvs = std::to_array<std::string_view>({"a.drv", "b.drv"});
// Enough steps for any run to end once the faults have stopped.
constexpr unsigned settleSteps = 2000;
// Longer than the scheduler waits for a client that left.
constexpr double shortWaitMs = 11'000;
// Longer than a worker keeps a slot nobody came for.
constexpr double longWaitMs = 61'000;

enum class Rule : uint8_t { slots, noSteal, answered };

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
    bool full = false;              // low on disk: takes no builds

    SimWorker(std::string addr, nixgrpc::Metrics & metrics, const double & nowMs)
        : addr(addr)
        , builder(
              {.advertise = std::move(addr),
               .systems = {"x"},
               .features = {},
               .maxJobs = slots,
               .now = [&nowMs] -> nixgrpc::Clock::time_point {
                   return nixgrpc::Clock::time_point(
                       std::chrono::duration_cast<nixgrpc::Clock::duration>(std::chrono::duration<double, std::milli>(nowMs)));
               }},
              metrics)
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

    std::map<std::string, Job, std::less<>> jobs;
};

struct Step
{
    std::string name;
    std::function<void()> run;
    bool wild = false; // not taken while the run settles
};

void add(std::vector<Step> & steps, std::string name, std::function<void()> run, bool wild = false)
{
    steps.push_back({.name = std::move(name), .run = std::move(run), .wild = wild});
}

class Sim
{
public:
    Sim()
    {
        for (size_t idx = 0; idx < workerCount; idx++) {
            workers.push_back(std::make_unique<SimWorker>("w:" + std::to_string(idx), metrics, nowMs));
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
            add(steps, "scheduler restarts", [this] -> void { restartScheduler(); }, true);
        }
        add(steps, "a short time passes", [this] -> void { wait(shortWaitMs); }, true);
        return steps;
    }

    // With no more faults, runs until nothing but time can move.
    auto settle(std::string & trace) -> std::optional<Violation>
    {
        faults = faultsPerSeed;
        for (unsigned step = 0; step < settleSteps; step++) {
            auto steps = enabled();
            std::erase_if(steps, [](const Step & one) -> bool { return one.wild; });
            if (steps.empty()) {
                if (answered()) {
                    return std::nullopt;
                }
                trace += "  a long time passes\n";
                wait(longWaitMs);
            } else {
                // In turn, so no peer is starved.
                auto const & pick = steps.at(step % steps.size());
                trace += "  " + pick.name + "\n";
                pick.run();
            }
            if (auto bad = check()) {
                return bad;
            }
        }
        return Violation{.rule = Rule::answered, .what = "a client still waits for its build"};
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
    double nowMs = 0;
    std::optional<Violation> violation;

    void wait(double forMs)
    {
        nowMs += forMs;
        disp->tick();
        for (auto & wkr : workers) {
            wkr->builder.tick();
        }
    }

    [[nodiscard]] auto answered() const -> bool
    {
        for (const auto & cli : clients) {
            for (const auto & [drv, job] : cli->jobs) {
                if (job.state != Follow::finished) {
                    return false;
                }
            }
        }
        return true;
    }

    void startScheduler()
    {
        disp = std::make_unique<Dispatcher>(
            Dispatcher::Config{
                .defaultSystem = "x", .present = {}, .lookupThreads = 0, .nowMs = [this] -> double { return nowMs; }},
            metrics);
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

    void connect(SimWorker & wkr)
    {
        wkr.session = std::make_unique<Dispatcher::Worker>(Dispatcher::Worker{
            .send = [this, &wkr](const nix::remote::SchedCmd & cmd) -> bool {
                if (cmd.has_revoke()) {
                    revokeSent(wkr, cmd.revoke().drv_path());
                }
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
        if (faults < faultsPerSeed) {
            add(
                steps,
                who + " loses its stream",
                [this, &cli] -> void {
                    faults++;
                    disp->clientGone(cli.stream);
                    cli.stream.reset();
                    cli.down.clear();
                    cli.up.clear();
                },
                true);
        }
        for (auto const name : drvs) {
            if (!cli.jobs.contains(name)) {
                add(steps, std::format("{} wants {}", who, name), [&cli, name] -> void {
                                     std::string const drv(name);
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
                                 [this, &cli, &job, &drv] -> void { call(cli, job, drv); });
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
        if (job == cli.jobs.end() || job->second.state == Follow::building || job->second.state == Follow::finished) {
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
            add(steps, wkr.addr + " connects", [this, &wkr] -> void { connect(wkr); });
        } else {
            if (faults < faultsPerSeed) {
                add(steps, wkr.addr + " loses its session", [this, &wkr] -> void {
                                     faults++;
                                     disp->workerGone(*wkr.session);
                                     dropSession(wkr);
                                 }, true);
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
        if (wkr.full) {
            add(steps, wkr.addr + " has room again", [&wkr] -> void {
                                 wkr.full = false;
                                 wkr.builder.setUnavailable(Builder::Unavailable::lowDisk, false);
                             });
        } else if (faults < faultsPerSeed) {
            add(steps, wkr.addr + " runs out of disk", [this, &wkr] -> void {
                                 faults++;
                                 wkr.full = true;
                                 wkr.builder.setUnavailable(Builder::Unavailable::lowDisk, true);
                             }, true);
        }
        for (const auto & drv : wkr.building) {
            add(steps, wkr.addr + " ends " + drv, [this, idx, name = std::string_view(drv)] -> void { buildEnds(idx, std::string(name)); });
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
            }
            wkr.builder.onRevoke(cmd.revoke());
        }
    }

    // The scheduler may stop a build nobody waits for, or one of two. It
    // must not stop the only build of a client it can talk to.
    void revokeSent(const SimWorker & wkr, const std::string & drv)
    {
        if (!wkr.building.contains(drv)) {
            return;
        }
        // A build that ended still runs for the scheduler until it read the Done.
        bool const twice = std::ranges::any_of(workers, [&](const auto & other) -> bool {
            return other.get() != &wkr
                   && (other->building.contains(drv)
                       || std::ranges::any_of(other->up, [&](const nix::remote::WorkerMsg & msg) -> bool {
                              return msg.has_done() && msg.done().drv_path() == drv
                                     && msg.done().outcome() == nix::remote::Done::BUILT;
                          }));
        });
        bool const awaited = std::ranges::any_of(clients, [&](const auto & cli) -> bool {
            auto const job = cli->jobs.find(drv);
            // A client that came back follows again once its Want is read.
            bool const heard = std::ranges::none_of(cli->up, [&](const nix::remote::ClientMsg & msg) -> bool {
                return msg.has_want() && msg.want().drv_path() == drv;
            });
            return cli->stream && heard && job != cli->jobs.end() && job->second.state == Follow::building
                   && workers.at(job->second.worker).get() == &wkr;
        });
        if (!twice && awaited) {
            violation = Violation{
                .rule = Rule::noSteal,
                .what = "the scheduler revokes the only build of " + drv + ", on " + wkr.addr + ", while a client waits for it"};
        }
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

struct Found
{
    uint64_t seed;
    std::string trace;
};

// For each rule some run breaks, the first such seed and its steps.
auto violationsIn(uint64_t first, uint64_t seeds) -> std::map<Rule, Found>
{
    std::map<Rule, Found> found;
    for (uint64_t seed = first; seed < first + seeds; seed++) {
        std::mt19937_64 rng(seed);
        Sim sim;
        std::string trace;
        std::optional<Violation> bad;
        for (unsigned step = 0; step < stepsPerSeed && !bad; step++) {
            auto steps = sim.enabled();
            auto const & pick = steps.at(rng() % steps.size());
            trace += "  " + pick.name + "\n";
            pick.run();
            bad = sim.check();
        }
        if (!bad) {
            trace += "  (no more faults)\n";
            bad = sim.settle(trace);
        }
        if (bad) {
            found.try_emplace(bad->rule, Found{.seed = seed, .trace = trace + "  => " + bad->what + "\n"});
        }
    }
    return found;
}

// Seeds are independent, so each thread takes a slice of them.
auto violations(uint64_t first, uint64_t seeds) -> std::map<Rule, Found>
{
    auto const threads = std::clamp<uint64_t>(std::thread::hardware_concurrency(), 1, std::max<uint64_t>(seeds, 1));
    auto const slice = (seeds + threads - 1) / threads;
    std::vector<std::map<Rule, Found>> parts(threads);
    {
        std::vector<std::jthread> pool;
        for (uint64_t idx = 0; idx < threads; idx++) {
            auto const from = std::min(idx * slice, seeds);
            pool.emplace_back([&part = parts.at(idx), start = first + from, count = std::min(slice, seeds - from)] -> void {
                part = violationsIn(start, count);
            });
        }
    }
    // Slices are in seed order, so the first hit of a rule is its lowest seed.
    std::map<Rule, Found> found;
    for (auto & part : parts) {
        found.merge(part);
    }
    return found;
}

struct Script
{
    const char * name;
    std::vector<std::string_view> steps;
};

// Orderings the random runs do not reach. Each must keep every rule.
auto scripts() -> std::vector<Script>
{
    return {
        {.name = "a Done for an assignment that is over arrives after the next one",
         .steps =
             {
                 "w:0 connects",
                 "scheduler reads from w:0",
                 "client 0 connects",
                 "client 0 wants a.drv",
                 "scheduler reads from client 0",
                 "w:0 reads",
                 "client 0 reads",
                 "client 0 calls w:0 for a.drv",
                 "w:0 loses its session",
                 "w:1 connects",
                 "scheduler reads from w:1",
                 "w:1 reads",
                 "w:0 connects",
                 // w:0 still builds a.drv, so w:1 is told to stop. Its Done stays unread.
                 "scheduler reads from w:0",
                 "w:1 reads",
                 "w:0 ends a.drv",
                 "scheduler reads from w:0",
                 "w:0 loses its session",
                 "client 1 connects",
                 "client 1 wants a.drv",
                 "scheduler reads from client 1",
                 "scheduler reads from w:1",
                 "client 1 wants b.drv",
                 "scheduler reads from client 1",
             }},
    };
}

// What the script breaks, with the steps up to there.
auto play(const Script & script) -> std::optional<std::string>
{
    Sim sim;
    std::string trace;
    std::optional<Violation> bad;
    for (auto const name : script.steps) {
        auto steps = sim.enabled();
        auto const pick = std::ranges::find(steps, name, &Step::name);
        trace += std::format("  {}\n", name);
        if (pick == steps.end()) {
            return trace + "  => this step cannot happen here\n";
        }
        pick->run();
        bad = sim.check();
        if (bad) {
            break;
        }
    }
    if (!bad) {
        bad = sim.settle(trace);
    }
    if (bad) {
        return trace + "  => " + bad->what + "\n";
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
    {.rule = Rule::noSteal, .name = "the only build a client waits for is not revoked", .broken = false},
    {.rule = Rule::answered, .name = "every client gets its build once the faults stop", .broken = false},
});

auto run(std::span<char *> args) -> int
{
    // Dispatcher and Builder log every event to stderr.
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    if (std::freopen("/dev/null", "w", stderr) == nullptr) {
        return EXIT_FAILURE;
    }
    uint64_t const first = args.size() > 1 ? std::stoull(args.subspan(1).front()) : 1;
    uint64_t const seeds = args.size() > 2 ? std::stoull(args.subspan(2).front()) : defaultSeeds;
    int status = EXIT_SUCCESS;
    auto const all = violations(first, seeds);
    for (const auto & row : rows) {
        auto const iter = all.find(row.rule);
        auto const * const found = iter == all.end() ? nullptr : &iter->second;
        if (found != nullptr && !row.broken) {
            std::print("FAIL {}\nseed {}:\n{}", row.name, found->seed, found->trace);
            status = EXIT_FAILURE;
        } else if (found == nullptr && row.broken) {
            std::println("FAIL {}\nis listed as broken, but no seed breaks it", row.name);
            status = EXIT_FAILURE;
        } else if (found != nullptr) {
            std::println("known broken (seed {}): {}", found->seed, row.name);
        } else {
            std::println("ok: {}", row.name);
        }
    }
    for (const auto & script : scripts()) {
        if (auto const bad = play(script)) {
            std::print("FAIL {}\n{}", script.name, *bad);
            status = EXIT_FAILURE;
        } else {
            std::println("ok: {}", script.name);
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
