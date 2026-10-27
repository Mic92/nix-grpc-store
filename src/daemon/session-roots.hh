#pragma once
// Temp roots per client session (x-nix-session), on a nix-daemon connection
// per session that is closed after `ttl` without calls.

#include <chrono>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/util/error.hh>
#include <nix/util/ref.hh>

namespace nixgrpc {

class SessionRoots
{
public:
    using Clock = std::chrono::steady_clock;
    using Open = std::function<nix::ref<nix::Store>()>;

    static constexpr std::chrono::seconds defaultTtl{3600};
    static constexpr size_t defaultCap = 256;

    explicit SessionRoots(Open open, std::chrono::seconds ttl = defaultTtl, size_t cap = defaultCap)
        : open_(std::move(open))
        , ttl_(ttl)
        , cap_(cap)
    {
    }

    // Also refreshes the session. No-op without a session.
    void hold(std::string_view session, const std::vector<nix::StorePath> & paths, Clock::time_point now = Clock::now())
    {
        if (session.empty()) {
            return;
        }
        auto const add = [&](nix::Store & store) -> void {
            for (const auto & path : paths) {
                store.addTempRoot(path);
            }
        };
        try {
            add(*roots(session, now));
        } catch (nix::Error &) {
            // Its nix-daemon connection is gone, and the roots with it.
            drop(session);
            add(*roots(session, now));
        }
    }

private:
    auto roots(std::string_view session, Clock::time_point now) -> std::shared_ptr<nix::Store>
    {
        std::shared_ptr<nix::Store> store;
        std::shared_ptr<nix::Store> evicted; // closed outside the lock
        {
            absl::MutexLock const lock(mutex_);
            auto found = held_.find(session);
            if (found == held_.end()) {
                if (held_.size() >= cap_) {
                    auto oldest = held_.begin();
                    for (auto it = held_.begin(); it != held_.end(); ++it) {
                        if (it->second.last < oldest->second.last) {
                            oldest = it;
                        }
                    }
                    evicted = std::move(oldest->second.store);
                    held_.erase(oldest);
                }
                found = held_.emplace(std::string(session), Held{.store = open_().get_ptr(), .last = now}).first;
            }
            found->second.last = now;
            store = found->second.store;
        }
        return store;
    }

    void drop(std::string_view session)
    {
        std::shared_ptr<nix::Store> store;
        absl::MutexLock const lock(mutex_);
        if (auto const found = held_.find(session); found != held_.end()) {
            store = std::move(found->second.store);
            held_.erase(found);
        }
    }

public:

    auto expire(Clock::time_point now = Clock::now()) -> size_t
    {
        std::vector<std::shared_ptr<nix::Store>> ended;
        {
            absl::MutexLock const lock(mutex_);
            for (auto it = held_.begin(); it != held_.end();) {
                if (now - it->second.last >= ttl_) {
                    ended.push_back(std::move(it->second.store));
                    it = held_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        return ended.size();
    }

    [[nodiscard]] auto size() -> size_t
    {
        absl::MutexLock const lock(mutex_);
        return held_.size();
    }

private:
    struct Held
    {
        std::shared_ptr<nix::Store> store;
        Clock::time_point last;
    };

    Open open_;
    std::chrono::seconds ttl_;
    size_t cap_;
    absl::Mutex mutex_;
    std::map<std::string, Held, std::less<>> held_ ABSL_GUARDED_BY(mutex_);
};

} // namespace nixgrpc
