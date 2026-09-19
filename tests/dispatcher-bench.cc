// Drives the Dispatcher (protobuf in, callbacks out, one mutex) with a
// nixpkgs-shaped workload in simulated time and reports host CPU spent in
// it per message. Clients behave like GrpcStore::buildPaths: Want every
// drv whose inputs are done, build on Assigned, release dependents on
// completion. Workers answer Expect with Done after the drv's duration.
//
//   dispatcher-bench [preset] [key=value ...]
//   presets: mixed (default), large, firehose
//   keys: seed clients drvs universe fanin depth workers slots arrival
//
// Output is logfmt on stdout; use `perf record -g -- dispatcher-bench
// firehose`. Calls over 1 ms are printed as event=spike. With glibc malloc a
// few of those remain at firehose scale inside operator new (heap consolidation
// after ~10^6 frees); they vanish under LD_PRELOAD=libjemalloc.so and are
// not the scheduler's doing.

// NOLINTBEGIN
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

using namespace nixgrpc;
using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedMsg;
using nix::remote::WorkerMsgs;

namespace {

struct Cfg
{
    uint64_t seed = 1;
    size_t clients = 20;
    size_t drvsPerClient = 300;
    size_t universe = 3000;
    size_t fanin = 3;
    size_t depth = 12;
    double cachedFrac = 0.3;
    double durMedian = 8.0;
    double durSigma = 1.4;
    double arrivalS = 60.0;
    size_t workers = 8;
    uint32_t slots = 16;
};

void preset(Cfg & cfg, std::string_view name)
{
    if (name == "mixed") {
    } else if (name == "large") {
        cfg.clients = 120;
        cfg.drvsPerClient = 250;
        cfg.universe = 12000;
        cfg.arrivalS = 3600;
        cfg.workers = 12;
    } else if (name == "firehose") {
        cfg.clients = 300;
        cfg.drvsPerClient = 2000;
        cfg.universe = 400000;
        cfg.cachedFrac = 0.2;
        cfg.durMedian = 1.0;
        cfg.durSigma = 1.0;
        cfg.arrivalS = 120;
        cfg.workers = 150;
    } else {
        std::cerr << "unknown preset " << name << "\n";
        std::exit(2);
    }
}

struct Spec
{
    double duration;
    bool cached;
    uint16_t layer;
    std::string drvPath;
    std::string outPath;
};

struct Dag
{
    double start;
    std::vector<uint32_t> ids;               // into specs
    std::vector<std::vector<uint32_t>> deps; // local indices
    std::vector<std::vector<uint32_t>> dnts;
    std::vector<double> cp;
};

auto storePath(std::mt19937_64 & rng, std::string_view suffix) -> std::string
{
    static constexpr std::string_view alphabet = "0123456789abcdfghijklmnpqrsvwxyz";
    std::string out = "/nix/store/";
    for (int i = 0; i < 32; i++) {
        out += alphabet[rng() % alphabet.size()];
    }
    out += '-';
    out += suffix;
    return out;
}

struct World
{
    std::vector<Spec> specs;
    std::vector<Dag> dags;

    explicit World(const Cfg & cfg)
    {
        std::mt19937_64 rng(cfg.seed);
        std::lognormal_distribution<double> dur(std::log(cfg.durMedian), cfg.durSigma);
        std::uniform_real_distribution<double> unit;
        specs.reserve(cfg.universe);
        for (size_t d = 0; d < cfg.universe; d++) {
            auto name = "drv" + std::to_string(d);
            specs.push_back(
                Spec{
                    .duration = std::clamp(dur(rng), 0.2, 7200.0),
                    .cached = unit(rng) < cfg.cachedFrac,
                    .layer = static_cast<uint16_t>(rng() % std::max<size_t>(cfg.depth, 1)),
                    .drvPath = storePath(rng, name + ".drv"),
                    .outPath = storePath(rng, name),
                });
        }
        for (size_t c = 0; c < cfg.clients; c++) {
            Dag dag;
            dag.start = unit(rng) * cfg.arrivalS;
            auto base = rng() % cfg.universe;
            std::unordered_set<uint32_t> picked;
            auto want = std::min(cfg.drvsPerClient, cfg.universe);
            auto span = std::max<uint64_t>(cfg.drvsPerClient * 3 / 2, 1);
            while (dag.ids.size() < want) {
                uint64_t off = rng() % span;
                while ((rng() & 3) == 0) {
                    off += span;
                }
                auto id = static_cast<uint32_t>((base + off) % cfg.universe);
                if (picked.insert(id).second) {
                    dag.ids.push_back(id);
                }
            }
            std::ranges::sort(dag.ids, [&](uint32_t a, uint32_t b) {
                return std::pair(specs[a].layer, a) < std::pair(specs[b].layer, b);
            });
            std::vector<size_t> layerStart;
            for (size_t k = 0; k < dag.ids.size(); k++) {
                auto l = specs[dag.ids[k]].layer;
                while (layerStart.size() <= l) {
                    layerStart.push_back(k);
                }
            }
            auto n = dag.ids.size();
            dag.deps.resize(n);
            dag.dnts.resize(n);
            for (size_t k = 0; k < n; k++) {
                auto lower = layerStart[specs[dag.ids[k]].layer];
                if (lower == 0) {
                    continue;
                }
                std::mt19937_64 r(dag.ids[k] * 0x100000001B3ULL ^ 77);
                for (size_t f = 0; f < cfg.fanin; f++) {
                    auto d = static_cast<uint32_t>(r() % lower);
                    if (std::ranges::find(dag.deps[k], d) == dag.deps[k].end()) {
                        dag.deps[k].push_back(d);
                        dag.dnts[d].push_back(static_cast<uint32_t>(k));
                    }
                }
            }
            dag.cp.assign(n, 0);
            for (size_t k = n; k-- > 0;) {
                const auto & sp = specs[dag.ids[k]];
                double m = 0;
                for (auto d : dag.dnts[k]) {
                    m = std::max(m, dag.cp[d]);
                }
                dag.cp[k] = (sp.cached ? 0 : sp.duration) + m;
            }
            dags.push_back(std::move(dag));
        }
    }
};

// ------------------------------------------------------------ simulation

struct Event
{
    double at;
    uint64_t seq;
    std::function<void()> fn;

    auto operator>(const Event & o) const -> bool
    {
        return std::pair(at, seq) > std::pair(o.at, o.seq);
    }
};

struct Sim
{
    const Cfg & cfg;
    const World & world;
    Metrics metrics{""};
    Dispatcher disp;
    double now = 0;
    uint64_t seq = 0;
    std::priority_queue<Event, std::vector<Event>, std::greater<>> events;

    // host time inside Dispatcher calls
    using HClock = std::chrono::steady_clock;

    struct Stat
    {
        uint64_t n = 0;
        HClock::duration total{};
        HClock::duration max{};
    };

    std::map<std::string, Stat> stats;

    // Calls over 1 ms are printed with CLOCK_MONOTONIC bounds so that
    // `perf record -k CLOCK_MONOTONIC` + `perf report --time a,b` shows them.
    static constexpr std::chrono::milliseconds spikeThreshold{1};

    template<typename F>
    void timed(const char * what, F && fn)
    {
        auto t0 = HClock::now();
        fn();
        auto t1 = HClock::now();
        auto dt = t1 - t0;
        auto & st = stats[what];
        st.n++;
        st.total += dt;
        st.max = std::max(st.max, dt);
        if (dt > spikeThreshold) {
            auto mono = [](HClock::time_point t) {
                return std::chrono::duration<double>(t.time_since_epoch()).count();
            };
            std::printf(
                "event=spike op=%s us=%.0f sim_s=%.1f builds=%zu queued=%.0f "
                "mono=%.6f,%.6f\n",
                what,
                std::chrono::duration<double, std::micro>(dt).count(),
                now,
                builds,
                metrics.schedQueuedNow(),
                mono(t0),
                mono(t1));
        }
    }

    void at(double t, std::function<void()> fn)
    {
        events.push(Event{t, seq++, std::move(fn)});
    }

    // ---- workers
    struct WorkerSim
    {
        std::string addr;
        Dispatcher::Worker conn;
        size_t running = 0;
    };

    std::vector<std::unique_ptr<WorkerSim>> workers;
    std::unordered_map<std::string, const Spec *> byDrv;
    std::unordered_set<std::string> built; // outputs some worker has

    // ---- clients
    struct ClientSim
    {
        size_t idx;
        const Dag * dag;
        Dispatcher::ClientPtr conn;
        std::vector<uint32_t> waiting; // unfinished deps per node
        std::vector<bool> done;
        std::unordered_map<std::string_view, uint32_t> local; // drvPath -> node
        size_t remaining = 0;
        double finishedAt = 0;
    };

    std::vector<std::unique_ptr<ClientSim>> clients;
    size_t builds = 0;
    size_t clientsDone = 0;

    Sim(const Cfg & cfg, const World & world)
        : cfg(cfg)
        , world(world)
        , disp(
              Dispatcher::Config{
                  .defaultSystem = "x86_64-linux",
                  .present = [this](const std::vector<std::string> & keys) -> std::unordered_set<std::string> {
                      std::unordered_set<std::string> have;
                      for (const auto & k : keys) {
                          if (built.contains(k)) {
                              have.insert(k);
                          }
                      }
                      return have;
                  },
                  // Single-threaded sim: keep lookups on this thread.
                  .lookupThreads = 0},
              metrics)
    {
        for (const auto & sp : world.specs) {
            byDrv.emplace(sp.drvPath, &sp);
            if (sp.cached) {
                built.insert(sp.outPath);
            }
        }
        for (size_t w = 0; w < cfg.workers; w++) {
            auto ws = std::make_unique<WorkerSim>();
            ws->addr = "10.0." + std::to_string(w / 250) + "." + std::to_string(w % 250) + ":50051";
            auto * wp = ws.get();
            ws->conn = Dispatcher::Worker{.send = [this, wp](const SchedCmd & cmd) -> bool {
                onCmd(*wp, cmd);
                return true;
            }};
            WorkerMsgs hello;
            auto * hel = hello.add_msgs()->mutable_hello();
            hel->set_addr(ws->addr);
            hel->add_systems("x86_64-linux");
            hel->set_max_jobs(cfg.slots);
            timed("hello", [&] { disp.workerMsgs(ws->conn, hello); });
            workers.push_back(std::move(ws));
        }
        for (size_t c = 0; c < world.dags.size(); c++) {
            at(world.dags[c].start, [this, c] { startClient(c); });
        }
    }

    // Under the dispatcher mutex, like the real loopback worker: defer.
    void onCmd(WorkerSim & ws, const SchedCmd & cmd)
    {
        if (!cmd.has_expect()) {
            return;
        }
        const auto * sp = byDrv.at(cmd.expect().drv_path());
        ws.running++;
        at(now + sp->duration, [this, &ws, sp] {
            ws.running--;
            built.insert(sp->outPath);
            builds++;
            WorkerMsgs msg;
            auto * done = msg.add_msgs()->mutable_done();
            done->set_drv_path(sp->drvPath);
            done->set_outcome(nix::remote::Done::BUILT);
            auto * out = done->add_outputs();
            out->set_path(sp->outPath);
            out->set_nar_size(1 << 20);
            timed("done", [&] { disp.workerMsgs(ws.conn, msg); });
            // Clients learn via their BuildDerivation stream; model as immediate.
            auto range = followers.equal_range(sp->drvPath);
            std::vector<std::pair<ClientSim *, uint32_t>> fs;
            for (auto it = range.first; it != range.second; ++it) {
                fs.push_back(it->second);
            }
            followers.erase(range.first, range.second);
            for (auto [cs, k] : fs) {
                nodeDone(*cs, k);
            }
        });
    }

    std::unordered_multimap<std::string, std::pair<ClientSim *, uint32_t>> followers;

    void startClient(size_t c)
    {
        auto cs = std::make_unique<ClientSim>();
        cs->idx = c;
        cs->dag = &world.dags[c];
        auto * cp = cs.get();
        cs->conn = disp.connectClient([this, cp](const SchedMsg & msg) -> bool {
            // Under the dispatcher mutex: defer like a real stream would.
            at(now, [this, cp, msg] { onSched(*cp, msg); });
            return true;
        });
        auto n = cs->dag->ids.size();
        cs->waiting.resize(n);
        cs->done.assign(n, false);
        cs->remaining = n;
        cs->local.reserve(n);
        for (size_t k = 0; k < n; k++) {
            cs->waiting[k] = static_cast<uint32_t>(cs->dag->deps[k].size());
            cs->local.emplace(world.specs[cs->dag->ids[k]].drvPath, static_cast<uint32_t>(k));
        }
        clients.push_back(std::move(cs));
        for (size_t k = 0; k < n; k++) {
            if (cp->waiting[k] == 0) {
                sendWant(*cp, static_cast<uint32_t>(k));
            }
        }
    }

    void sendWant(ClientSim & cs, uint32_t k)
    {
        const auto & sp = world.specs[cs.dag->ids[k]];
        ClientMsgs msg;
        auto * want = msg.add_msgs()->mutable_want();
        want->set_drv_path(sp.drvPath);
        want->set_system("x86_64-linux");
        want->add_out_keys(sp.outPath);
        want->set_cp_hint_ms(static_cast<uint64_t>(cs.dag->cp[k] * 1000));
        for (auto d : cs.dag->deps[k]) {
            want->add_inputs(world.specs[cs.dag->ids[d]].outPath);
        }
        timed("want", [&] { disp.clientMsgs(cs.conn, msg); });
    }

    void onSched(ClientSim & cs, const SchedMsg & msg)
    {
        const std::string * drv = nullptr;
        if (msg.has_assigned()) {
            drv = &msg.assigned().drv_path();
        } else if (msg.has_cached()) {
            drv = &msg.cached().drv_path();
        } else if (msg.has_unplaceable()) {
            std::cerr << "unplaceable: " << msg.unplaceable().reason() << "\n";
            std::exit(1);
        } else {
            return;
        }
        auto it = cs.local.find(*drv);
        if (it == cs.local.end()) {
            return;
        }
        auto k = it->second;
        if (cs.done[k]) {
            return;
        }
        if (msg.has_cached()) {
            nodeDone(cs, k);
        } else {
            followers.emplace(*drv, std::pair(&cs, k));
        }
    }

    void nodeDone(ClientSim & cs, uint32_t k)
    {
        if (cs.done[k]) {
            return;
        }
        cs.done[k] = true;
        cs.remaining--;
        for (auto d : cs.dag->dnts[k]) {
            if (--cs.waiting[d] == 0) {
                sendWant(cs, d);
            }
        }
        if (cs.remaining == 0) {
            cs.finishedAt = now;
            clientsDone++;
            timed("client_gone", [&] { disp.clientGone(cs.conn); });
        }
    }

    void run()
    {
        while (!events.empty()) {
            auto ev = events.top();
            events.pop();
            now = ev.at;
            ev.fn();
        }
    }

    void report(double wallS) const
    {
        std::printf(
            "event=summary clients=%zu clients_done=%zu builds=%zu "
            "sim_makespan_s=%.0f wall_s=%.3f\n",
            clients.size(),
            clientsDone,
            builds,
            now,
            wallS);
        HClock::duration all{};
        uint64_t msgs = 0;
        for (const auto & [name, st] : stats) {
            all += st.total;
            msgs += st.n;
            auto us = [](HClock::duration d) { return std::chrono::duration<double, std::micro>(d).count(); };
            std::printf(
                "event=op op=%s n=%lu total_ms=%.1f mean_us=%.2f max_us=%.0f\n",
                name.c_str(),
                st.n,
                us(st.total) / 1000,
                us(st.total) / static_cast<double>(std::max<uint64_t>(st.n, 1)),
                us(st.max));
        }
        auto totalS = std::chrono::duration<double>(all).count();
        std::printf(
            "event=total msgs=%lu dispatcher_s=%.3f msgs_per_s=%.0f\n",
            msgs,
            totalS,
            static_cast<double>(msgs) / std::max(totalS, 1e-9));
    }
};

} // namespace

auto main(int argc, char ** argv) -> int
{
    Cfg cfg;
    preset(cfg, "mixed");
    for (int i = 1; i < argc; i++) {
        std::string_view arg = argv[i];
        auto eq = arg.find('=');
        if (eq == std::string_view::npos) {
            preset(cfg, arg);
            continue;
        }
        auto key = arg.substr(0, eq);
        auto val = std::string(arg.substr(eq + 1));
        auto u = [&] { return std::stoull(val); };
        auto f = [&] { return std::stod(val); };
        if (key == "seed")
            cfg.seed = u();
        else if (key == "clients")
            cfg.clients = u();
        else if (key == "drvs")
            cfg.drvsPerClient = u();
        else if (key == "universe")
            cfg.universe = u();
        else if (key == "fanin")
            cfg.fanin = u();
        else if (key == "depth")
            cfg.depth = u();
        else if (key == "workers")
            cfg.workers = u();
        else if (key == "slots")
            cfg.slots = static_cast<uint32_t>(u());
        else if (key == "arrival")
            cfg.arrivalS = f();
        else if (key == "cached")
            cfg.cachedFrac = f();
        else {
            std::cerr << "unknown key " << key << "\n";
            return 2;
        }
    }
    const World world(cfg);
    Sim sim(cfg, world);
    auto t0 = std::chrono::steady_clock::now();
    sim.run();
    sim.report(std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count());
    return sim.clientsDone == sim.clients.size() ? 0 : 1;
}

// NOLINTEND
