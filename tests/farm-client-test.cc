// Drives src/farm.hh against tests/farm-mock.py. Run via `meson test`.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <iterator>
#include <utility>

#include <nlohmann/json.hpp> // IWYU pragma: keep
#include <nlohmann/json_fwd.hpp>

#include <nix/util/error.hh>

#include "farm.hh"

namespace {

using Status = nixgrpc::Claim::Status;

// Mock heartbeat is 200 ms.
constexpr std::chrono::milliseconds settle{300};
constexpr std::chrono::milliseconds reclaimWindow{800};
constexpr std::chrono::milliseconds lostAfter{2500};
// Two sheds at Retry-After: 1 each.
constexpr std::chrono::milliseconds retryAfterFloor{1900};
constexpr int64_t someToken = 42;

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

    void post(const std::string & path, const nlohmann::json & body) const
    {
        nixgrpc::http::Call(base + path, "testtoken", body).perform();
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

void testClaims(Suite & tst, const nixgrpc::Niks3 & niks3)
{
    {
        auto holder = niks3.claim({"a.narinfo"}, {});
        tst.check(holder->await() == Status::build && holder->token() > 0, "first claim gets build + token");
        auto waiter = niks3.claim({"a.narinfo"}, {});
        tst.check(waiter->first() == Status::wait, "second claim waits");
        tst.post("/_mock/complete", {{"outputs", {"a.narinfo"}}});
        tst.check(waiter->await() == Status::built, "waiter sees built on complete");
        tst.check(!holder->lost(), "holder not lost while niks3 alive");
    }
    {
        auto holder = niks3.claim({"b.narinfo"}, {});
        tst.check(holder->await() == Status::build, "b: holder");
        auto waiter = niks3.claim({"b.narinfo"}, {});
        tst.check(waiter->first() == Status::wait, "b: waiter");
        auto tok = holder->token();
        holder.reset();
        tst.check(waiter->await() == Status::build, "waiter promoted when holder releases");
        tst.check(
            tst.countLog([&](const nlohmann::json & entry) -> bool {
                return entry.value("ev", "") == "fail" && entry.value("token", int64_t{0}) == tok
                    && entry.value("kind", "-").empty();
            }) == 1,
            "dropping a held claim releases it explicitly");
    }
    {
        auto holder = niks3.claim({"c.narinfo"}, {});
        tst.check(holder->await() == Status::build, "c: holder");
        auto waiter = niks3.claim({"c.narinfo"}, {});
        std::this_thread::sleep_for(settle);
        tst.check(holder->fail("PermanentFailure"), "fail accepted with our token");
        tst.check(waiter->await() == Status::failed && waiter->kind() == "PermanentFailure", "waiter sees failed + kind");
        auto retry = niks3.claim({"c.narinfo"}, {});
        tst.check(retry->await() == Status::build, "failure not remembered, next claim builds");
        tst.check(retry->fail(""), "transient fail = release");
    }
    {
        auto holder = niks3.claim({"d.narinfo"}, {"in1.narinfo"});
        tst.check(holder->await() == Status::build, "d: holder");
        auto tok = holder->token();
        tst.post("/_mock/drop", {{"outputs", {"d.narinfo"}}});
        std::this_thread::sleep_for(reclaimWindow);
        tst.check(!holder->lost() && holder->token() == tok, "re-claimed after stream drop, same token");
        tst.check(
            tst.countLog([&](const nlohmann::json & entry) -> bool {
                return entry.value("ev", "") == "claim" && entry.value("token", int64_t{0}) == tok;
            }) >= 1,
            "re-claim carried token");
    }
    {
        tst.post("/_mock/overload", {{"n", 2}});
        auto before = std::chrono::steady_clock::now();
        auto waiter = niks3.claim({"f.narinfo"}, {});
        tst.check(waiter->await() == Status::build, "claim survives 503 while not yet holding");
        auto took = std::chrono::steady_clock::now() - before;
        tst.check(took >= retryAfterFloor, "Retry-After honoured");
        tst.check(
            tst.countLog([](const nlohmann::json & entry) -> bool { return entry.value("ev", "") == "shed"; }) == 2,
            "exactly two attempts were shed");
    }
    {
        auto holder = niks3.claim({"e.narinfo"}, {});
        tst.check(holder->await() == Status::build, "e: holder");
        tst.post("/_mock/freeze", {{"frozen", true}});
        std::this_thread::sleep_for(lostAfter);
        tst.check(holder->lost(), "claim lost after 3 silent heartbeats");
        tst.post("/_mock/freeze", {{"frozen", false}});
    }
}

void testPush(Suite & tst, nixgrpc::PushProcess & push)
{
    push.queue({"/nix/store/x.drv"});
    push.pushWait({"/nix/store/y", "/nix/store/y2"}, someToken);
    auto throws = [&](const char * path) -> std::string {
        try {
            push.pushWait({path}, 1);
        } catch (nixgrpc::StaleClaim &) {
            return "stale";
        } catch (nix::Error &) {
            return "error";
        }
        return "ok";
    };
    tst.check(throws("/nix/store/fail") == "error", "push error surfaces as exception");
    tst.check(throws("/nix/store/stale") == "stale", "stale token surfaces as StaleClaim");
    // The child dies mid-request: the waiter fails, the next request respawns.
    tst.check(throws("/nix/store/crash") == "error", "dead child fails the waiter");
    tst.check(throws("/nix/store/z") == "ok", "child is respawned");
    tst.check(
        tst.countLog([](const nlohmann::json & entry) -> bool {
            return entry.value("ev", "") == "push" && entry.value("claim_token", int64_t{0}) == someToken
                   && entry.at("paths").size() == 2;
        }) == 1,
        "request line carried both outputs and the claim token");
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
    testClaims(suite, nixgrpc::Niks3(base, "testtoken"));
    nixgrpc::PushProcess push({"python3", mock, "push", "--stdin", "--server-url", base});
    testPush(suite, push);
    return suite.result();
} catch (const std::exception & err) {
    std::cerr << "error: " << err.what() << "\n";
    return 1;
}
