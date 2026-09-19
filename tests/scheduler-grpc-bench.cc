// The scheduler behind a real gRPC server on loopback, same reactors as
// SchedulerService. Measures what dispatcher-bench cannot: gRPC per-message
// cost, HTTP/2 framing, TLS.
//
// Workload is saturation, not a DAG: W workers answer every Expect with Done
// immediately; C clients each keep K Wants in flight and send the next as
// soon as one is Assigned. So throughput = scheduler round trips per second.
//
//   scheduler-grpc-bench [workers=16] [slots=8] [clients=32] [inflight=64] [seconds=5] [tls=0]
//   tls=1 needs `openssl` in PATH (throwaway self-signed cert).
//   serve=PORT: only run the server (profile this process); target=HOST:PORT:
//   only run the load against it. Split mode is plaintext.
//   niks3_ms=N cached_pct=P lookup_threads=32: fake binary-cache lookup that
//   sleeps N ms per call and reports P% of outputs present (0 threads = on
//   the gRPC thread, to see why not).

// NOLINTBEGIN
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <stdlib.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include "dispatcher.hh"
#include "sched-reactor.hh"
#include "metrics.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

using namespace nixgrpc;
using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedCmds;
using nix::remote::SchedMsg;
using nix::remote::SchedMsgs;
using nix::remote::WorkerMsgs;
using Clock = std::chrono::steady_clock;

namespace {

struct Cfg
{
    size_t workers = 16;
    uint32_t slots = 8;
    size_t clients = 32;
    size_t inflight = 64;
    double seconds = 5;
    bool tls = false;
    int serve = 0;
    std::string target;
    double niks3Ms = -1; // <0: no present() configured
    unsigned cachedPct = 0;
    unsigned lookupThreads = 32;
};

struct Creds
{
    std::shared_ptr<grpc::ServerCredentials> server = grpc::InsecureServerCredentials();
    std::shared_ptr<grpc::ChannelCredentials> channel = grpc::InsecureChannelCredentials();
    grpc::ChannelArguments args;
};

auto readFile(const std::string & path) -> std::string
{
    std::FILE * f = std::fopen(path.c_str(), "rb");
    if (f == nullptr) {
        std::perror(path.c_str());
        std::exit(1);
    }
    std::string out;
    char buf[4096];
    while (auto n = std::fread(buf, 1, sizeof buf, f)) {
        out.append(buf, n);
    }
    std::fclose(f);
    return out;
}

auto makeTls() -> Creds
{
    char dir[] = "/tmp/sched-bench-XXXXXX";
    if (mkdtemp(dir) == nullptr) {
        std::perror("mkdtemp");
        std::exit(1);
    }
    auto d = std::string(dir);
    auto cmd =
        "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost -keyout "
        + d + "/key.pem -out " + d + "/cert.pem >/dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) {
        std::cerr << "openssl req failed (tls=1 needs openssl in PATH)\n";
        std::exit(1);
    }
    auto cert = readFile(d + "/cert.pem");
    auto key = readFile(d + "/key.pem");
    grpc::SslServerCredentialsOptions sopts;
    sopts.pem_key_cert_pairs.push_back({key, cert});
    Creds creds;
    creds.server = grpc::SslServerCredentials(sopts);
    creds.channel = grpc::SslCredentials({.pem_root_certs = cert});
    creds.args.SetSslTargetNameOverride("localhost");
    return creds;
}

// SchedulerService from coordinator.cc, minus auth.
class Service final : public nix::remote::Scheduler::CallbackService
{
    Dispatcher & disp;

public:
    explicit Service(Dispatcher & disp)
        : disp(disp)
    {
    }

    auto Schedule(grpc::CallbackServerContext * ctx) -> grpc::ServerBidiReactor<ClientMsgs, SchedMsgs> * override
    {
        return new ScheduleReactor(ctx, disp);
    }

    auto WorkerSession(grpc::CallbackServerContext * ctx) -> grpc::ServerBidiReactor<WorkerMsgs, SchedCmds> * override
    {
        return new WorkerSessionReactor(ctx, disp);
    }
};

auto storePath(uint64_t n, std::string_view suffix) -> std::string
{
    static constexpr std::string_view alphabet = "0123456789abcdfghijklmnpqrsvwxyz";
    std::mt19937_64 rng(n);
    std::string out = "/nix/store/";
    for (int i = 0; i < 32; i++) {
        out += alphabet[rng() % alphabet.size()];
    }
    out += '-';
    out += suffix;
    return out;
}

struct Lat
{
    std::mutex mu;
    std::vector<uint32_t> us;

    void add(Clock::duration d)
    {
        auto v = static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(d).count());
        const std::scoped_lock lock(mu);
        us.push_back(v);
    }
};

void runWorker(
    const std::string & addr,
    const Creds & creds,
    size_t idx,
    uint32_t slots,
    std::atomic<bool> & stop,
    std::atomic<uint64_t> & builds)
{
    auto chan = grpc::CreateCustomChannel(addr, creds.channel, creds.args);
    auto stub = nix::remote::Scheduler::NewStub(chan);
    grpc::ClientContext ctx;
    auto stream = stub->WorkerSession(&ctx);
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("10.0." + std::to_string(idx / 250) + "." + std::to_string(idx % 250) + ":50051");
    hel->set_system("x86_64-linux");
    hel->set_max_jobs(slots);
    stream->Write(hello);
    SchedCmds cmds;
    while (!stop.load(std::memory_order_relaxed) && stream->Read(&cmds)) {
        WorkerMsgs reply;
        for (const auto & cmd : cmds.msgs()) {
            if (!cmd.has_expect()) {
                continue;
            }
            auto * done = reply.add_msgs()->mutable_done();
            done->set_drv_path(cmd.expect().drv_path());
            done->set_assign_id(cmd.expect().assign_id());
            done->set_outcome(nix::remote::Done::BUILT);
            auto * out = done->add_outputs();
            const auto & drv = cmd.expect().drv_path();
            out->set_path(drv.substr(0, drv.size() - 4));
            out->set_nar_size(1 << 20);
        }
        if (reply.msgs_size() == 0) {
            continue;
        }
        if (!stream->Write(reply)) {
            break;
        }
        builds.fetch_add(reply.msgs_size(), std::memory_order_relaxed);
    }
    ctx.TryCancel();
    stream->Finish();
}

void runClient(
    const std::string & addr,
    const Creds & creds,
    size_t idx,
    size_t inflight,
    std::atomic<bool> & stop,
    std::atomic<uint64_t> & assigned,
    Lat & lat)
{
    auto chan = grpc::CreateCustomChannel(addr, creds.channel, creds.args);
    auto stub = nix::remote::Scheduler::NewStub(chan);
    grpc::ClientContext ctx;
    auto stream = stub->Schedule(&ctx);
    std::mutex mu; // sentAt, seq and Write (sync streams allow one writer)
    std::unordered_map<std::string, Clock::time_point> sentAt;
    uint64_t seq = idx << 40;
    auto sendWants = [&](size_t n) {
        const std::scoped_lock lock(mu);
        ClientMsgs batch;
        auto now = Clock::now();
        for (size_t i = 0; i < n; i++) {
            auto * want = batch.add_msgs()->mutable_want();
            auto drv = storePath(seq++, "d.drv");
            want->set_drv_path(drv);
            want->set_system("x86_64-linux");
            want->add_out_keys(drv.substr(0, drv.size() - 4));
            want->set_cp_hint_ms(1000);
            sentAt.emplace(drv, now);
        }
        return stream->Write(batch);
    };
    std::thread reader([&] {
        SchedMsgs msgs;
        while (stream->Read(&msgs)) {
            size_t freed = 0;
            auto now = Clock::now();
            for (const auto & msg : msgs.msgs()) {
                const std::string * drv = msg.has_assigned()      ? &msg.assigned().drv_path()
                                          : msg.has_cached()      ? &msg.cached().drv_path()
                                          : msg.has_unplaceable() ? &msg.unplaceable().drv_path()
                                                                  : nullptr;
                if (drv == nullptr) {
                    continue;
                }
                const std::scoped_lock lock(mu);
                auto it = sentAt.find(*drv);
                if (it == sentAt.end()) {
                    continue;
                }
                lat.add(now - it->second);
                sentAt.erase(it);
                freed++;
            }
            assigned.fetch_add(freed, std::memory_order_relaxed);
            if (stop.load(std::memory_order_relaxed) || (freed > 0 && !sendWants(freed))) {
                break;
            }
        }
    });
    sendWants(inflight);
    while (!stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ctx.TryCancel();
    reader.join();
    stream->Finish();
}

} // namespace

auto main(int argc, char ** argv) -> int
{
    Cfg cfg;
    for (int i = 1; i < argc; i++) {
        std::string_view arg = argv[i];
        auto eq = arg.find('=');
        if (eq == std::string_view::npos) {
            std::cerr << "bad arg " << arg << "\n";
            return 2;
        }
        auto key = arg.substr(0, eq);
        auto val = std::string(arg.substr(eq + 1));
        if (key == "workers")
            cfg.workers = std::stoull(val);
        else if (key == "slots")
            cfg.slots = static_cast<uint32_t>(std::stoul(val));
        else if (key == "clients")
            cfg.clients = std::stoull(val);
        else if (key == "inflight")
            cfg.inflight = std::stoull(val);
        else if (key == "seconds")
            cfg.seconds = std::stod(val);
        else if (key == "tls")
            cfg.tls = val == "1";
        else if (key == "serve")
            cfg.serve = std::stoi(val);
        else if (key == "target")
            cfg.target = val;
        else if (key == "niks3_ms")
            cfg.niks3Ms = std::stod(val);
        else if (key == "cached_pct")
            cfg.cachedPct = static_cast<unsigned>(std::stoul(val));
        else if (key == "lookup_threads")
            cfg.lookupThreads = static_cast<unsigned>(std::stoul(val));
        else {
            std::cerr << "unknown key " << key << "\n";
            return 2;
        }
    }

    Metrics metrics("");
    Dispatcher::Config dcfg{.defaultSystem = "x86_64-linux", .lookupThreads = cfg.lookupThreads};
    std::atomic<uint64_t> presentCalls{0};
    if (cfg.niks3Ms >= 0) {
        dcfg.present = [&cfg, &presentCalls](const std::vector<std::string> & keys) -> std::unordered_set<std::string> {
            presentCalls++;
            std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(cfg.niks3Ms));
            std::unordered_set<std::string> have;
            for (const auto & k : keys) {
                if (std::hash<std::string>{}(k) % 100 < cfg.cachedPct) {
                    have.insert(k);
                }
            }
            return have;
        };
    }
    Dispatcher disp(dcfg, metrics);
    Service svc(disp);
    Creds creds = cfg.tls ? makeTls() : Creds{};
    // One TCP connection per simulated peer, as in a real farm; by default
    // gRPC would multiplex every channel to the same target over one.
    creds.args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    std::unique_ptr<grpc::Server> server;
    std::string addr = cfg.target;
    if (addr.empty()) {
        grpc::ServerBuilder builder;
        int port = cfg.serve;
        builder.AddListeningPort("127.0.0.1:" + std::to_string(port), creds.server, &port);
        builder.RegisterService(&svc);
        server = builder.BuildAndStart();
        addr = std::string(cfg.tls ? "localhost:" : "127.0.0.1:") + std::to_string(port);
    }
    if (cfg.serve != 0) {
        std::printf("event=serving addr=%s\n", addr.c_str());
        std::fflush(stdout);
        server->Wait();
        return 0;
    }

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> builds{0};
    std::atomic<uint64_t> assigned{0};
    Lat lat;
    std::vector<std::thread> threads;
    for (size_t w = 0; w < cfg.workers; w++) {
        threads.emplace_back(runWorker, addr, std::cref(creds), w, cfg.slots, std::ref(stop), std::ref(builds));
    }
    // Workers first so the first Wants are placeable.
    if (cfg.target.empty()) {
        while (metrics.schedWorkersNow() < static_cast<double>(cfg.workers)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    for (size_t c = 0; c < cfg.clients; c++) {
        threads.emplace_back(
            runClient, addr, std::cref(creds), c, cfg.inflight, std::ref(stop), std::ref(assigned), std::ref(lat));
    }

    auto t0 = Clock::now();
    uint64_t lastA = 0;
    for (int tick = 1; std::chrono::duration<double>(Clock::now() - t0).count() < cfg.seconds; tick++) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto a = assigned.load();
        std::printf("event=tick s=%d assigned_per_s=%lu builds=%lu\n", tick, a - lastA, builds.load());
        std::fflush(stdout);
        lastA = a;
    }
    auto wall = std::chrono::duration<double>(Clock::now() - t0).count();
    stop = true;
    if (server) {
        server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
    }
    for (auto & t : threads) {
        t.join();
    }

    std::sort(lat.us.begin(), lat.us.end());
    auto pct = [&](double p) {
        return lat.us.empty() ? 0U : lat.us[static_cast<size_t>(p * static_cast<double>(lat.us.size() - 1))];
    };
    auto total = assigned.load();
    // Each assignment is Want→(Expect→Done)→Assigned: 4 entries through the server.
    std::printf(
        "event=summary tls=%d workers=%zu clients=%zu inflight=%zu present_calls=%lu replies=%lu per_s=%.0f server_entries_per_s=%.0f "
        "lat_us_p50=%u p90=%u p99=%u max=%u\n",
        cfg.tls ? 1 : 0,
        cfg.workers,
        cfg.clients,
        cfg.inflight,
        presentCalls.load(),
        total,
        static_cast<double>(total) / wall,
        4.0 * static_cast<double>(total) / wall,
        pct(0.5),
        pct(0.9),
        pct(0.99),
        pct(1.0));
    return 0;
}

// NOLINTEND
