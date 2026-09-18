#pragma once
// niks3 build claims: the NDJSON claim stream and the client that opens it.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <nix/util/error.hh>
#include <nix/util/sync.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>
#include <nix/util/processes.hh>

#include "http.hh"
#include "logfmt.hh"
#include "push.hh"
#include "token-file.hh"

namespace nixgrpc {

// The stream runs on its own thread and is re-opened with our token when it
// breaks. Three heartbeats of silence end it.
class Claim
{
public:
    enum class Status : uint8_t { pending, wait, build, built, failed };

private:
    using Clock = std::chrono::steady_clock;

    const std::string baseUrl;
    const std::shared_ptr<TokenFile> bearer;
    const Clock::duration heartbeat;

    struct State
    {
        nlohmann::json request; // gains "token" once granted
        Status status = Status::pending;
        int64_t token = 0;
        std::string kind;
        Clock::time_point lastSeen = Clock::now();
        std::chrono::seconds retryAfter{};
        bool stop = false;
        bool released = false;

        [[nodiscard]] auto decided() const -> bool
        {
            return status == Status::built || status == Status::failed;
        }

        // A holder is fenced after 3 heartbeats, a waiter can afford patience.
        [[nodiscard]] auto giveUpAfter(Clock::duration heartbeat) const -> Clock::duration
        {
            constexpr int waiterPatience = 60;
            return (status == Status::build ? 3 : waiterPatience) * heartbeat;
        }
    };

    nix::Sync<State> state_;
    std::condition_variable cv;

    std::thread streamThread; // last: joined before the rest goes

    void onLine(std::string_view line)
    {
        auto msg = nlohmann::json::parse(line, nullptr, /*allow_exceptions=*/false);
        if (!msg.is_object()) {
            return;
        }
        auto status = msg.value("status", "");
        auto tok = msg.value("token", int64_t{0});
        auto state(state_.lock());
        state->lastSeen = Clock::now();
        if (status == "build" && tok > 0) {
            state->status = Status::build;
            state->token = tok;
            state->request["token"] = tok; // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): json object insert
        } else if (status == "wait" && state->status == Status::pending) {
            state->status = Status::wait;
        } else if (status == "built") {
            state->status = Status::built;
        } else if (status == "failed") {
            state->status = Status::failed;
            state->kind = msg.value("kind", "");
        }
        cv.notify_all();
    }

    struct StreamCtx
    {
        Claim * self;
        std::string buf;
    };

    static auto onData(char * data, size_t size, size_t nmemb, void * userp) noexcept -> size_t
    {
        constexpr size_t maxLine = size_t{64} * 1024;
        try {
            auto & ctx = *static_cast<StreamCtx *>(userp);
            if (ctx.self->state_.lock()->stop) {
                return 0;
            }
            ctx.buf.append(data, size * nmemb);
            for (auto pos = ctx.buf.find('\n'); pos != std::string::npos; pos = ctx.buf.find('\n')) {
                ctx.self->onLine(std::string_view(ctx.buf).substr(0, pos));
                ctx.buf.erase(0, pos + 1);
            }
            return ctx.buf.size() > maxLine ? 0 : size * nmemb;
        } catch (...) {
            return 0;
        }
    }

    static auto onProgress(
        void * userp, curl_off_t /*dlt*/, curl_off_t /*dln*/, curl_off_t /*ult*/, curl_off_t /*uln*/) noexcept
        -> int
    {
        auto & self = *static_cast<Claim *>(userp);
        auto state(self.state_.lock());
        return state->stop || Clock::now() - state->lastSeen > state->giveUpAfter(self.heartbeat) ? 1 : 0;
    }

    void runStream()
    {
        std::optional<nlohmann::json> const body = state_.lock()->request;
        StreamCtx ctx{.self = this, .buf = {}};
        http::Call call(baseUrl + "/api/builds/claim", bearer->get(), body);
        call.opt(CURLOPT_TIMEOUT, 0L);
        call.opt(CURLOPT_WRITEFUNCTION, &Claim::onData);
        call.opt(CURLOPT_WRITEDATA, &ctx);
        call.opt(CURLOPT_NOPROGRESS, 0L);
        call.opt(CURLOPT_XFERINFOFUNCTION, &Claim::onProgress);
        call.opt(CURLOPT_XFERINFODATA, this);
        call.perform();
        state_.lock()->retryAfter = call.retryAfter();
    }

    static auto jitter(Clock::duration base) -> Clock::duration
    {
        thread_local std::minstd_rand rng{std::random_device{}()};
        return base / 2 + Clock::duration(std::uniform_int_distribution<Clock::rep>(0, base.count())(rng)) / 2;
    }

    template<typename Pred>
    auto waitFor(Pred done, const Cancelled & cancelled) -> Status
    {
        auto state(state_.lock());
        while (!done(*state) && !state->stop) {
            if (cancelled()) {
                throw CancelledWait("gave up waiting for niks3 claim");
            }
            state.wait_for(cv, cancelPoll);
        }
        if (!done(*state)) {
            throw nix::Error("niks3 claim stream lost before a decision");
        }
        return state->status;
    }

    void halt()
    {
        state_.lock()->stop = true;
        cv.notify_all();
    }

    void run()
    {
        constexpr int maxBackoffHeartbeats = 8;
        auto backoff = heartbeat / 2;
        while (!state_.lock()->stop) {
            auto started = Clock::now();
            try {
                runStream();
            } catch (...) {
                nix::ignoreExceptionExceptInterrupt();
            }
            auto state(state_.lock());
            if (state->stop || state->decided() || Clock::now() - state->lastSeen > state->giveUpAfter(heartbeat)) {
                break;
            }
            backoff = Clock::now() - started > heartbeat ? heartbeat / 2 : std::min(backoff * 2, maxBackoffHeartbeats * heartbeat);
            auto pause = std::max<Clock::duration>(jitter(backoff), state->retryAfter);
            if (state->status == Status::build) {
                pause = std::min(pause, heartbeat);
            }
            state.wait_for(cv, pause, [&]() -> bool { return state->stop; });
        }
        halt();
    }

public:
    Claim(std::string baseUrl, std::shared_ptr<TokenFile> bearer, Clock::duration heartbeat, nlohmann::json request)
        : baseUrl(std::move(baseUrl))
        , bearer(std::move(bearer))
        , heartbeat(heartbeat)
        , state_(State{.request = std::move(request)})
        , streamThread([this]() -> void { run(); })
    {
    }

    Claim(const Claim &) = delete;
    Claim(Claim &&) = delete;
    auto operator=(const Claim &) -> Claim & = delete;
    auto operator=(Claim &&) -> Claim & = delete;

    // niks3 keeps the row when the stream drops, so a holder hands it back.
    ~Claim()
    {
        bool held = false;
        {
            auto state(state_.lock());
            held = state->status == Status::build && !state->released;
        }
        if (held) {
            try {
                fail("");
            } catch (...) {
                nix::ignoreExceptionInDestructor();
            }
        }
        halt();
        if (streamThread.joinable()) {
            streamThread.join();
        }
    }

    // Possibly `wait`.
    auto first(const Cancelled & cancelled = never) -> Status
    {
        return waitFor([](const State & state) -> bool { return state.status != Status::pending; }, cancelled);
    }

    // build, built or failed.
    auto await(const Cancelled & cancelled = never) -> Status
    {
        return waitFor(
            [](const State & state) -> bool { return state.status == Status::build || state.decided(); }, cancelled);
    }

    auto token() -> int64_t
    {
        return state_.lock()->token;
    }

    auto kind() -> std::string
    {
        return state_.lock()->kind;
    }

    // Held, then silent for 3 heartbeats.
    auto lost() -> bool
    {
        auto state(state_.lock());
        return state->status == Status::build && state->stop;
    }

    void published()
    {
        state_.lock()->released = true;
    }

    // Empty kind = transient. False on stale token.
    auto fail(const std::string & kind) -> bool
    {
        state_.lock()->released = true;
        http::Call call(baseUrl + "/api/builds/fail", bearer->get(), nlohmann::json{{"claim_token", token()}, {"kind", kind}});
        auto status = call.perform();
        halt();
        if (status == http::conflict) {
            return false;
        }
        if (!http::ok(status)) {
            throw nix::Error("niks3 fail: HTTP %d: %s", status, call.body());
        }
        return true;
    }
};

class Niks3
{
    std::string baseUrl;
    std::shared_ptr<TokenFile> bearer;
    std::chrono::milliseconds heartbeat{0};

public:
    Niks3(std::string url, std::shared_ptr<TokenFile> token)
        : baseUrl(std::move(url))
        , bearer(std::move(token))
    {
        while (baseUrl.ends_with('/')) {
            baseUrl.pop_back();
        }
        http::Call call(baseUrl + "/api/cache-config", bearer->get(), std::nullopt);
        if (auto status = call.perform(); !http::ok(status)) {
            throw nix::Error("niks3 cache-config: HTTP %d", status);
        }
        auto cfg = nlohmann::json::parse(call.body(), nullptr, /*allow_exceptions=*/false);
        if (!cfg.is_object()) {
            throw nix::Error("niks3 cache-config: not a JSON object");
        }
        auto secs = cfg.value("claim_heartbeat_secs", 0.0);
        if (secs <= 0) {
            throw nix::Error("niks3 at %s does not support build claims", baseUrl);
        }
        heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration<double>(secs));
    }

    [[nodiscard]] auto claim(const std::vector<std::string> & outputs, const std::vector<std::string> & inputs) const
        -> std::unique_ptr<Claim>
    {
        return std::make_unique<Claim>(
            baseUrl, bearer, heartbeat, nlohmann::json{{"outputs", outputs}, {"inputs", inputs}});
    }
};

} // namespace nixgrpc
