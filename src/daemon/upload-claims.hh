#pragma once
// In-flight uploads by store path. A dropped owner promise fails the waiters.

#include <algorithm>
#include <chrono>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <utility>

namespace nixgrpc {

class UploadClaims
{
public:
    struct Entry
    {
        std::promise<void> promise; // only the owner's is ever fulfilled
        std::shared_future<void> result;
        bool owner;
    };

    auto enter(const std::string & path) -> Entry
    {
        std::promise<void> promise;
        std::scoped_lock const lock(mutex_);
        std::erase_if(active_, [](const auto & item) -> bool { return finished(item.second); });
        auto [slot, inserted] = active_.try_emplace(path, promise.get_future().share());
        return {.promise = std::move(promise), .result = slot->second, .owner = inserted};
    }

private:
    static auto finished(const std::shared_future<void> & result) -> bool
    {
        return result.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    std::mutex mutex_;
    std::map<std::string, std::shared_future<void>> active_;
};

} // namespace nixgrpc
