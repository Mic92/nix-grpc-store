#pragma once
// The nix-daemon a worker stores and builds with, and how to tell that it
// went away.

#include <algorithm>
#include <array>
#include <cerrno>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/util/error.hh>
#include <nix/util/ref.hh>
#include <nix/util/serialise.hh>
#include <nix/util/terminal.hh>
#include <nix/util/types.hh>

namespace nixgrpc {

class LocalStore
{
public:
    explicit LocalStore(std::string uri_)
        : uri(std::move(uri_))
    {
    }

    // For reads. Its connections are pooled and shared by all callers.
    auto shared() -> nix::ref<nix::Store>
    {
        std::scoped_lock const lock(mutex);
        if (!pool) {
            pool = open({{"max-connections", sharedConnections}, {"max-connection-age", sharedConnectionAge}}).get_ptr();
        }
        return nix::ref<nix::Store>(pool);
    }

    // For writes (temp roots end with the caller) and anything long.
    [[nodiscard]] auto scoped() const -> nix::ref<nix::Store>
    {
        return open();
    }

    // What nix says when it cannot open a connection. It reports these as a
    // plain Error, so the text is all there is to go by.
    static constexpr auto cannotOpen = std::to_array<std::string_view>({
        "cannot connect to socket at",
        "cannot open connection to remote store",
        "opening a connection to remote store",
    });

    // The error means the nix-daemon is gone, not that the request is bad.
    static auto lost(const std::exception & err) -> bool
    {
        if (dynamic_cast<const nix::EndOfFile *>(&err) != nullptr) {
            return true;
        }
        if (const auto * sys = dynamic_cast<const nix::SysError *>(&err);
            sys != nullptr && (sys->errNo == EPIPE || sys->errNo == ECONNRESET)) {
            return true;
        }
        // nix colours parts of its messages, in the middle of a phrase too.
        auto const what = nix::filterANSIEscapes(err.what(), true);
        return std::ranges::any_of(cannotOpen, [&](std::string_view needle) -> bool { return what.contains(needle); });
    }

    // A restarted nix-daemon leaves the pool full of dead connections that
    // only fail on the next write, and a store that failed to connect once
    // refuses every later attempt. Dropping the pool gets the retry a fresh one.
    auto dropIfLost(const std::exception & err) -> bool
    {
        bool const gone = lost(err);
        if (gone) {
            std::scoped_lock const lock(mutex);
            pool.reset();
        }
        return gone;
    }

private:
    std::string uri;
    std::mutex mutex;
    std::shared_ptr<nix::Store> pool;

    // Aged-out connections release their temp roots. Not sooner: a fresh
    // nix-daemon probes every substituter again.
    static constexpr auto sharedConnections = "16";
    static constexpr auto sharedConnectionAge = "600";

    // Without the path info cache a path deleted behind our back (gc) is
    // invalid here too, instead of failing later when its NAR is read.
    [[nodiscard]] auto open(nix::StringMap params = {}) const -> nix::ref<nix::Store>
    {
        params.emplace("path-info-cache-size", "0");
        return nix::openStore(uri, params);
    }
};

} // namespace nixgrpc
