// NarFetcher against in-process servers whose FetchNars fails in scripted ways.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <zstd.h>

#include <nix/store/path.hh>
#include <nix/util/error.hh>
#include <nix/util/serialise.hh>

#include "nar-fetcher.hh"
#include "retry.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

namespace {

using Handler = std::function<grpc::Status(int call, const nix::remote::FetchNarsRequest &,
                                           grpc::ServerWriter<nix::remote::NarFrame> &)>;

class ScriptedService final : public nix::remote::NixRemote::Service
{
public:
    explicit ScriptedService(Handler handler) : handler(std::move(handler)) {}

    [[nodiscard]] auto calls() const -> int { return callCount; }

    [[nodiscard]] auto requests() -> std::vector<std::vector<std::string>>
    {
        std::scoped_lock const lock(mutex);
        return seen;
    }

    auto FetchNars(
        grpc::ServerContext * /*ctx*/,
        const nix::remote::FetchNarsRequest * request,
        grpc::ServerWriter<nix::remote::NarFrame> * writer) -> grpc::Status override
    {
        {
            std::scoped_lock const lock(mutex);
            seen.emplace_back(request->paths().begin(), request->paths().end());
        }
        return handler(callCount++, *request, *writer);
    }

private:
    Handler handler;
    std::atomic<int> callCount = 0;
    std::mutex mutex;
    std::vector<std::vector<std::string>> seen;
};

struct Fixture
{
    ScriptedService service;
    std::unique_ptr<grpc::Server> server;
    std::shared_ptr<grpc::Channel> channel;

    explicit Fixture(Handler handler) : service(std::move(handler))
    {
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
        assert(server);
        channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
    }

    auto fetcher(nixgrpc::RetryPolicy retry = {}) -> std::unique_ptr<nixgrpc::NarFetcher>
    {
        return std::make_unique<nixgrpc::NarFetcher>(
            [this] -> std::shared_ptr<grpc::Channel> { return channel; }, "test", 1, std::move(retry));
    }
};

auto sendNar(grpc::ServerWriter<nix::remote::NarFrame> & writer, uint32_t index, const std::string & nar) -> void
{
    std::string compressed(ZSTD_compressBound(nar.size()), '\0');
    auto const size = ZSTD_compress(compressed.data(), compressed.size(), nar.data(), nar.size(), 1);
    assert(!ZSTD_isError(size));
    compressed.resize(size);
    nix::remote::NarFrame frame;
    frame.set_path_index(index);
    frame.set_data(compressed);
    frame.set_eof(true);
    writer.Write(frame);
}

auto narOf(const std::string & path) -> std::string { return "nar of " + path; }

// What the build hook sees when the balancer does not finish the TLS
// handshake in time.
auto connectFailed() -> grpc::Status
{
    return {grpc::StatusCode::UNAVAILABLE,
            "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1: handshaker shutdown"};
}

constexpr std::chrono::seconds retryBudget{10};

auto anyUnavailable() -> nixgrpc::RetryPolicy
{
    return {
        .retryable = [](const grpc::Status & status) -> bool {
            return status.error_code() == grpc::StatusCode::UNAVAILABLE;
        },
        .budget = [] -> std::chrono::milliseconds { return retryBudget; },
    };
}

auto fetch(nixgrpc::NarFetcher & fetcher, const nix::StorePath & path) -> std::string
{
    nix::StringSink sink;
    fetcher.fetchInto(path, sink);
    return sink.s;
}

auto fetchFails(nixgrpc::NarFetcher & fetcher, const nix::StorePath & path) -> bool
{
    try {
        fetch(fetcher, path);
    } catch (nix::Error &) {
        return true;
    }
    return false;
}

auto pathA() -> nix::StorePath { return {"00000000000000000000000000000000-a"}; }
auto pathB() -> nix::StorePath { return {"11111111111111111111111111111111-b"}; }

// A failed fetch must not be cached: nix repl keeps the store after ^C and
// asks for the same path again.
void failedFetchIsRetried()
{
    Fixture fix([](int, const auto &, auto &) -> grpc::Status {
        return {grpc::StatusCode::UNAVAILABLE, "worker draining"};
    });
    auto fetcher = fix.fetcher();

    assert(fetchFails(*fetcher, pathA()));
    assert(fetchFails(*fetcher, pathA()));
    assert(fix.service.calls() == 2);
}

// A starved client misses the connect deadline once; the copy must survive.
void connectFailureIsRetried()
{
    Fixture fix([](int call, const auto & request, auto & writer) -> grpc::Status {
        if (call < 2) {
            return connectFailed();
        }
        sendNar(writer, 0, narOf(request.paths(0)));
        return grpc::Status::OK;
    });
    auto fetcher = fix.fetcher(anyUnavailable());

    assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    assert(fix.service.calls() == 3);
}

// After a mid-stream break, finished paths are kept and only the paths with
// nothing received yet are asked for again.
void midStreamBreakRefetchesUntouchedPaths()
{
    Fixture fix([](int call, const auto & request, auto & writer) -> grpc::Status {
        for (int i = 0; i < request.paths_size(); ++i) {
            sendNar(writer, static_cast<uint32_t>(i), narOf(request.paths(i)));
            if (call == 0) {
                return {grpc::StatusCode::UNAVAILABLE, "Stream removed (Socket closed)"};
            }
        }
        return grpc::Status::OK;
    });
    auto fetcher = fix.fetcher(anyUnavailable());
    fetcher->recordOrder({pathA(), pathB()}, {{pathA(), 2}, {pathB(), 1}});

    assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    assert(fetch(*fetcher, pathB()) == narOf(std::string(pathB().to_string())));
    auto const requests = fix.service.requests();
    assert(requests.size() == 2);
    assert(requests.at(1) == std::vector<std::string>{std::string(pathB().to_string())});
}

void nonRetryableFailsAtOnce()
{
    Fixture fix([](int, const auto &, auto &) -> grpc::Status {
        return {grpc::StatusCode::PERMISSION_DENIED, "no"};
    });
    auto fetcher = fix.fetcher(anyUnavailable());

    assert(fetchFails(*fetcher, pathA()));
    assert(fix.service.calls() == 1);
}

// The paths a failed session never delivered must be asked for again too.
void siblingFailureIsRetried()
{
    Fixture fix([](int, const auto &, auto &) -> grpc::Status {
        return {grpc::StatusCode::PERMISSION_DENIED, "no"};
    });
    auto fetcher = fix.fetcher();
    fetcher->recordOrder({pathA(), pathB()}, {});

    assert(fetchFails(*fetcher, pathA()));
    assert(fetchFails(*fetcher, pathB()));
    assert(fix.service.calls() == 2);
}

// Each unannounced path gets a session; finished ones must not pile up.
void finishedSessionsGo()
{
    Fixture fix([](int, const auto & request, auto & writer) -> grpc::Status {
        sendNar(writer, 0, narOf(request.paths(0)));
        return grpc::Status::OK;
    });
    auto fetcher = fix.fetcher();
    for (int i = 0; i < 3; ++i) {
        assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    }
    // The last thread may still be on its way out.
    constexpr int polls = 100;
    constexpr std::chrono::milliseconds poll{10};
    for (int i = 0; i < polls && fetcher->sessionCount() > 0; ++i) {
        std::this_thread::sleep_for(poll);
    }
    assert(fetcher->sessionCount() == 0);
}

} // namespace

auto main() -> int
try {
    failedFetchIsRetried();
    connectFailureIsRetried();
    midStreamBreakRefetchesUntouchedPaths();
    nonRetryableFailsAtOnce();
    siblingFailureIsRetried();
    finishedSessionsGo();
    return 0;
} catch (...) {
    return 1;
}
