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
#include "token-file.hh"

namespace nixgrpc {

struct Niks3Config
{
    std::string url;
    std::string tokenFile;
    PushProcess::Argv pushArgv;

    [[nodiscard]] auto enabled() const -> bool
    {
        return !url.empty();
    }
};

class Cache
{
    struct Remote
    {
        std::string baseUrl;
        std::shared_ptr<TokenFile> bearer;
        PushProcess push;

        explicit Remote(const Niks3Config & cfg)
            : baseUrl(cfg.url)
            , bearer(std::make_shared<TokenFile>(cfg.tokenFile))
            , push(cfg.pushArgv)
        {
            while (baseUrl.ends_with('/')) {
                baseUrl.pop_back();
            }
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

    // Subset of `keys` ("<hash>.narinfo") the binary cache has. Empty without niks3.
    [[nodiscard]] auto present(const std::vector<std::string> & keys) -> std::unordered_set<std::string>
    {
        std::unordered_set<std::string> res;
        if (!remote || keys.empty()) {
            return res;
        }
        http::Call call(remote->baseUrl + "/api/objects/present", remote->bearer->get(), nlohmann::json{{"keys", keys}});
        auto status = call.perform();
        if (!http::ok(status)) {
            throw nix::Error("niks3 present: HTTP %d: %s", status, call.body());
        }
        auto reply = nlohmann::json::parse(call.body(), nullptr, /*allow_exceptions=*/false);
        auto found = reply.is_object() ? reply.find("present") : reply.end();
        // Go encodes an empty slice as null.
        if (found == reply.end() || !(found->is_array() || found->is_null())) {
            throw nix::Error("niks3 present: malformed reply: %s", call.body());
        }
        for (const auto & key : *found) {
            if (key.is_string()) {
                res.insert(key.get<std::string>());
            }
        }
        return res;
    }

    // Make locally valid paths visible to other nodes. No-op without niks3.
    void publish(const std::vector<std::string> & paths, const Cancelled & cancelled = never)
    {
        if (remote && !paths.empty()) {
            remote->push.pushWait(paths, cancelled);
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
