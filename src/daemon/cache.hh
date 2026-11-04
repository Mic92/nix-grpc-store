#pragma once
// Local store plus, if configured, a niks3 binary cache. Same calls in every topology.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>

#include "http.hh"
#include "nix-compat.hh"
#include "push.hh"
#include "niks3-client.hh"

namespace nixgrpc {

// How long a publish keeps retrying a failing upload. The default tries once.
struct PublishRetry
{
    static constexpr std::chrono::seconds defaultFirstPause{1};
    static constexpr std::chrono::seconds defaultMaxPause{30};

    std::chrono::milliseconds budget{0};
    std::chrono::milliseconds firstPause{defaultFirstPause};
    std::chrono::milliseconds maxPause{defaultMaxPause};
    // Told why the upload failed and how long until the next attempt.
    std::function<void(const std::string & error, std::chrono::milliseconds pause)> onRetry;
};

// The upload kept failing for the whole PublishRetry budget.
struct PublishFailing : nix::Error
{
    using nix::Error::Error;
};

class Cache
{
    struct Remote
    {
        Niks3Client client;
        PushProcess push;
        // Only spawned on first use.
        PushProcess verifyingPush;

        explicit Remote(const Niks3Config & cfg)
            : client(cfg)
            , push(cfg.pushArgv)
            , verifyingPush(withVerify(cfg.pushArgv))
        {
        }

        static auto withVerify(PushProcess::Argv argv) -> PushProcess::Argv
        {
            argv.emplace_back("--verify-s3-integrity");
            return argv;
        }
    };
    std::optional<Remote> remote;

public:
    explicit Cache(const Niks3Config & cfg)
    {
        if (cfg.enabled()) {
            remote.emplace(cfg);
        }
    }

    [[nodiscard]] auto hasRemote() const -> bool
    {
        return remote.has_value();
    }

    [[nodiscard]] auto client() const -> const Niks3Client *
    {
        return remote ? &remote->client : nullptr;
    }

    // Subset of `keys` ("<hash>.narinfo") the binary cache has. Empty without niks3.
    [[nodiscard]] auto present(const std::vector<std::string> & keys) -> std::unordered_set<std::string>
    {
        std::unordered_set<std::string> res;
        if (!remote || keys.empty()) {
            return res;
        }
        auto call = remote->client.post("/api/objects/present", nlohmann::json{{"keys", keys}});
        auto status = call->perform();
        if (!http::ok(status)) {
            throw nix::Error("niks3 present: HTTP %d: %s", status, call->body());
        }
        auto reply = nlohmann::json::parse(call->body(), nullptr, /*allow_exceptions=*/false);
        auto const found = reply.is_object() ? reply.find("present") : reply.end();
        // Go encodes an empty slice as null.
        if (found == reply.end() || !(found->is_array() || found->is_null())) {
            throw nix::Error("niks3 present: malformed reply: %s", call->body());
        }
        for (const auto & key : *found) {
            if (key.is_string()) {
                res.insert(key.get<std::string>());
            }
        }
        return res;
    }

    // Make locally valid paths visible to other nodes. No-op without niks3.
    // With `verify`, niks3 checks the bucket instead of trusting its database,
    // at one S3 lookup per object in the closure.
    // A failed upload is retried with backoff for `retry.budget`, then
    // PublishFailing; the paths stay valid locally all along.
    auto publish(
        const std::vector<std::string> & paths,
        const Cancelled & cancelled = never,
        bool verify = false,
        const PublishRetry & retry = {}) -> PushProcess::Signatures
    {
        if (!remote || paths.empty()) {
            return {};
        }
        auto & push = verify ? remote->verifyingPush : remote->push;
        auto const deadline = std::chrono::steady_clock::now() + retry.budget;
        auto pause = retry.firstPause;
        while (true) {
            try {
                return push.pushWait(paths, cancelled);
            } catch (CancelledWait &) {
                throw;
            } catch (nix::Error & err) {
                if (retry.budget <= std::chrono::milliseconds::zero()) {
                    throw;
                }
                if (std::chrono::steady_clock::now() + pause > deadline) {
                    auto const secs = std::chrono::duration_cast<std::chrono::seconds>(retry.budget);
                    throw PublishFailing("cache upload failing for %ds, giving up: %s", secs.count(), err.what());
                }
                if (retry.onRetry) {
                    retry.onRetry(err.what(), pause);
                }
                sleepUnlessCancelled(pause, cancelled);
                pause = std::min(pause * 2, retry.maxPause);
            }
        }
    }

    // Also puts the cache's signatures on the local paths: built here they are
    // only "ultimate", and a client checking signatures would refuse them.
    void publish(
        nix::Store & store,
        const std::vector<std::string> & paths,
        const Cancelled & cancelled = never,
        bool verify = false,
        const PublishRetry & retry = {})
    {
        for (const auto & [path, sigs] : publish(paths, cancelled, verify, retry)) {
            nixcompat::addSignatures(store, store.parseStorePath(path), sigs);
        }
    }

    // Path valid locally afterwards, or throws.
    static void substitute(nix::Store & store, const nix::StorePath & path)
    {
        if (!store.isValidPath(path)) {
            nixcompat::ensurePath(store, path);
        }
    }

    // References the client skipped as present may live only in the cache.
    void completeRefs(nix::Store & store, const nix::ValidPathInfo & info) const
    {
        if (!remote) {
            return;
        }
        for (const auto & ref : info.references) {
            if (ref != info.path) {
                substitute(store, ref);
            }
        }
    }

private:
    static void sleepUnlessCancelled(std::chrono::milliseconds pause, const Cancelled & cancelled)
    {
        auto const until = std::chrono::steady_clock::now() + pause;
        while (std::chrono::steady_clock::now() < until) {
            if (cancelled()) {
                throw CancelledWait("gave up retrying the upload");
            }
            std::this_thread::sleep_for(
                std::min<std::chrono::steady_clock::duration>(cancelPoll, until - std::chrono::steady_clock::now()));
        }
    }
};

} // namespace nixgrpc
