#pragma once
// Local store plus, if configured, a niks3 binary cache. Same calls in every topology.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <nix/store/globals.hh>
#include <nix/store/keys.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/sync.hh>
#include <nix/util/terminal.hh>

#include "http.hh"
#include "logfmt.hh"
#include "nix-compat.hh"
#include "push.hh"
#include "niks3-client.hh"

namespace nixgrpc {

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

    // Subset of `keys` ("<hash>.narinfo") the binary cache can serve, pushed
    // or as the dependency of a pushed path. Empty without niks3.
    [[nodiscard]] auto holds(const std::vector<std::string> & keys) -> std::unordered_set<std::string>
    {
        std::unordered_set<std::string> res;
        if (!remote || keys.empty()) {
            return res;
        }
        auto call = remote->client.post("/api/objects/exists", nlohmann::json{{"keys", keys}});
        auto status = call->perform();
        if (!http::ok(status)) {
            throw nix::Error("niks3 objects/exists: HTTP %d: %s", status, call->body());
        }
        auto reply = nlohmann::json::parse(call->body(), nullptr, /*allow_exceptions=*/false);
        auto const found = reply.is_object() ? reply.find("exists") : reply.end();
        // Go encodes an empty slice as null.
        if (found == reply.end() || !(found->is_array() || found->is_null())) {
            throw nix::Error("niks3 objects/exists: malformed reply: %s", call->body());
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
    auto publish(const std::vector<std::string> & paths, const Cancelled & cancelled = never, bool verify = false)
        -> PushProcess::Signatures
    {
        if (!remote || paths.empty()) {
            return {};
        }
        return (verify ? remote->verifyingPush : remote->push).pushWait(paths, cancelled);
    }

    // Keeps the log of a failed build in the cache, where `nix log` finds it.
    // A successful build's log goes up with its outputs.
    void publishLog(const std::string & drvPath)
    {
        if (!remote) {
            return;
        }
        remote->push.pushLog(drvPath);
    }

    struct Offered
    {
        nix::ref<nix::Store> store;
        nix::ref<const nix::ValidPathInfo> info;
    };

    // Finds a path in a substituter. We must not copy it into our store.
    // A build hook on this machine may hold the lock for that path.
    auto offered(const nix::Store & local, const nix::StorePath & path) -> std::optional<Offered>
    {
        auto const keys = nix::getDefaultPublicKeys();
        for (const auto & ref : nix::settings.getWorkerSettings().substituters.get()) {
            auto const name = ref.render();
            auto const miss = [&](const std::string & why) -> void {
                logLine(LogLevel::debug, {{"event", "offer_miss"}, {"path", std::string(path.to_string())}, {"substituter", name}, {"error", why}});
            };
            try {
                auto const sub = substituter(ref);
                auto const info = sub->queryPathInfo(path);
                if (sub->config.isTrusted || info->isContentAddressed(local) || info->checkSignatures(local, keys) > 0) {
                    return Offered{.store = sub, .info = info};
                }
                miss("no signature by a trusted key");
            } catch (nix::Error & err) {
                miss(nix::filterANSIEscapes(err.info().msg.str(), true));
            }
        }
        return std::nullopt;
    }

private:
    // Opens a substituter once. If that fails, the next call tries again.
    auto substituter(const nix::StoreReference & ref) -> nix::ref<nix::Store>
    {
        auto opened = substituters.lock();
        auto const name = ref.render();
        if (auto const found = opened->find(name); found != opened->end()) {
            return found->second;
        }
        return opened->emplace(name, nix::openStore(nix::StoreReference{ref})).first->second;
    }

    nix::Sync<std::map<std::string, nix::ref<nix::Store>>> substituters;

public:
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
};

} // namespace nixgrpc
