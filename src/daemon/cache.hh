#pragma once
// Local store plus, if configured, a niks3 binary cache. Same calls in every topology.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
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

class Cache
{
    struct Remote
    {
        Niks3Client client;
        PushProcess push;

        explicit Remote(const Niks3Config & cfg)
            : client(cfg)
            , push(cfg.pushArgv)
        {
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
        auto found = reply.is_object() ? reply.find("present") : reply.end();
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
    auto publish(const std::vector<std::string> & paths, const Cancelled & cancelled = never) -> PushProcess::Signatures
    {
        if (!remote || paths.empty()) {
            return {};
        }
        return remote->push.pushWait(paths, cancelled);
    }

    // Also puts the cache's signatures on the local paths: built here they are
    // only "ultimate", and a client checking signatures would refuse them.
    void publish(nix::Store & store, const std::vector<std::string> & paths, const Cancelled & cancelled = never)
    {
        for (const auto & [path, sigs] : publish(paths, cancelled)) {
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
};

} // namespace nixgrpc
