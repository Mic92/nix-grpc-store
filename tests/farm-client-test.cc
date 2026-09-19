// Drives src/daemon/cache.hh and push.hh against tests/farm-mock.py. Run via `meson test`.

#include <chrono>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp> // IWYU pragma: keep
#include <nlohmann/json_fwd.hpp>

#include <nix/util/error.hh>
#include <nix/util/file-system.hh>

#include "cache.hh"
#include "niks3-client.hh"
#include "http.hh"
#include "push.hh"

namespace {

constexpr std::chrono::milliseconds settle{300};

class Suite
{
    std::string base;
    int failures = 0;

public:
    explicit Suite(std::string base)
        : base(std::move(base))
    {
    }

    void check(bool cond, const char * what)
    {
        std::cerr << (cond ? "ok:   " : "FAIL: ") << what << "\n";
        failures += cond ? 0 : 1;
    }

    [[nodiscard]] auto result() const -> int
    {
        return failures == 0 ? 0 : 1;
    }

    [[nodiscard]] auto log() const -> nlohmann::json
    {
        nixgrpc::http::Call call(base + "/_mock/log", "testtoken", std::nullopt);
        call.perform();
        return nlohmann::json::parse(call.body());
    }

    [[nodiscard]] auto countLog(auto pred) const -> int
    {
        int found = 0;
        for (const auto & entry : log()) {
            found += pred(entry) ? 1 : 0;
        }
        return found;
    }
};

void testPush(Suite & tst, nixgrpc::PushProcess & push)
{
    // The child must outlive the thread that spawned it.
    std::thread([&]() -> void { push.pushWait({"/nix/store/x.drv"}); }).join();
    {
        constexpr std::chrono::milliseconds wellUnderCancelPoll{800};
        auto const started = std::chrono::steady_clock::now();
        push.pushWait({"/nix/store/quick"});
        tst.check(std::chrono::steady_clock::now() - started < wellUnderCancelPoll, "pushWait wakes on the ack");
    }
    push.pushWait({"/nix/store/y", "/nix/store/y2"});
    {
        // spec/push.qnt: two RPCs publishing the same path at once.
        std::thread other([&]() -> void { push.pushWait({"/nix/store/shared"}); });
        push.pushWait({"/nix/store/shared"});
        other.join();
    }
    {
        // More request and ack bytes in flight than both pipes hold.
        std::vector<std::string> many;
        constexpr size_t count = 2000;
        constexpr size_t nameLen = 80;
        many.reserve(count);
        for (size_t i = 0; i < count; i++) {
            many.push_back("/nix/store/" + std::string(nameLen, 'p') + std::to_string(i));
        }
        std::vector<std::thread> threads;
        constexpr int writers = 8;
        threads.reserve(writers);
        for (int i = 0; i < writers; i++) {
            threads.emplace_back([&]() -> void { push.pushWait(many); });
        }
        for (auto & thr : threads) {
            thr.join();
        }
    }
    auto throws = [&](const char * path) -> std::string {
        try {
            push.pushWait({path});
        } catch (nix::Error &) {
            return "error";
        }
        return "ok";
    };
    tst.check(throws("/nix/store/fail") == "error", "push error surfaces as exception");
    tst.check(throws("/nix/store/crash") == "error", "dead child fails the waiter");
    tst.check(throws("/nix/store/z") == "ok", "child is respawned");
    {
        bool stop = false;
        std::thread canceller([&]() -> void {
            std::this_thread::sleep_for(settle);
            stop = true;
        });
        std::string got = "ok";
        try {
            push.pushWait({"/nix/store/hang"}, [&]() -> bool { return stop; });
        } catch (nixgrpc::CancelledWait &) {
            got = "cancelled";
        } catch (nix::Error &) {
            got = "error";
        }
        canceller.join();
        tst.check(got == "cancelled", "pushWait returns once cancelled while niks3 never acks");
        tst.check(throws("/nix/store/z2") == "ok", "push usable after a cancelled wait");
    }
    tst.check(
        tst.countLog([](const nlohmann::json & entry) -> bool {
            return entry.value("ev", "") == "push" && entry.at("paths") == nlohmann::json{"/nix/store/y", "/nix/store/y2"}
                   && !entry.value("has_claim_token", true);
        }) == 1,
        "request line carried both outputs and no claim token");
}

void testCache(Suite & tst, const std::string & base, const std::string & mock)
{
    nixgrpc::Cache off(nixgrpc::Niks3Config{});
    tst.check(!off.hasRemote() && off.present({"a.narinfo"}).empty(), "no niks3: present is empty, publish is a no-op");
    off.publish({"/nix/store/whatever"});

    auto tokenFile = std::filesystem::path(nix::createTempDir()).string() + "/token";
    nix::writeFile(tokenFile, "stale");
    nixgrpc::Cache cache(
        {.url = base + "/", .tokenFile = tokenFile, .pushArgv = {"python3", mock, "push", "--stdin", "--server-url", base}});
    bool denied = false;
    try {
        (void) cache.present({"x.narinfo"});
    } catch (nix::Error &) {
        denied = true;
    }
    tst.check(denied, "present with a bad token throws");
    // Projected volumes swap the file, giving it a new inode.
    nix::writeFile(tokenFile + ".new", "testtoken");
    std::filesystem::rename(tokenFile + ".new", tokenFile);
    tst.check(cache.present({"aaaa.narinfo"}).empty(), "token rotation picked up; unknown key absent");
    cache.publish({"/nix/store/aaaa-foo", "/nix/store/bbbb-bar"});
    auto have = cache.present({"aaaa.narinfo", "bbbb.narinfo", "cccc.narinfo"});
    tst.check(have.size() == 2 && have.contains("aaaa.narinfo") && !have.contains("cccc.narinfo"), "published paths are present");
    tst.check(cache.present({}).empty(), "empty query, no request");
}

} // namespace

auto main(int argc, char ** argv) -> int
try {
    std::span const args(argv, static_cast<size_t>(argc));
    if (args.size() != 3) {
        std::cerr << "usage: farm-client-test <niks3-url> <farm-mock.py>\n";
        return 2;
    }
    std::string const base = *std::next(args.begin());
    std::string const mock = *std::next(args.begin(), 2);
    Suite suite(base);
    testCache(suite, base, mock);
    nixgrpc::PushProcess push({"python3", mock, "push", "--stdin", "--server-url", base});
    testPush(suite, push);
    return suite.result();
} catch (const std::exception & err) {
    std::cerr << "error: " << err.what() << "\n";
    return 1;
}
