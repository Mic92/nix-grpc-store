#pragma once
// Who is importing which store path right now. Two clients that upload the
// same path at once would each spool a full copy, so only the first one
// (the owner) does. The others read their stream to the end and then wait
// for the owner's result.

#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace nixgrpc {

class UploadClaims
{
public:
    enum class Outcome : std::uint8_t { pending, done, failed };

    // Held by the owner. Not marked done means failed.
    class Claim
    {
    public:
        Claim(Claim && other) noexcept : table_(std::exchange(other.table_, nullptr)), path_(std::move(other.path_)) {}
        auto operator=(Claim &&) -> Claim & = delete;
        Claim(const Claim &) = delete;
        auto operator=(const Claim &) -> Claim & = delete;
        ~Claim()
        {
            if (table_ != nullptr) {
                table_->finish(path_, Outcome::failed);
            }
        }

        void done()
        {
            if (table_ != nullptr) {
                table_->finish(path_, Outcome::done);
                table_ = nullptr;
            }
        }

    private:
        friend class UploadClaims;
        Claim(UploadClaims & table, std::string path) : table_(&table), path_(std::move(path)) {}
        UploadClaims * table_;
        std::string path_;
    };

    // Held by everyone else, also when they never get to wait.
    class Waiter
    {
    public:
        Waiter(Waiter && other) noexcept : table_(std::exchange(other.table_, nullptr)), path_(std::move(other.path_)) {}
        auto operator=(Waiter &&) -> Waiter & = delete;
        Waiter(const Waiter &) = delete;
        auto operator=(const Waiter &) -> Waiter & = delete;
        ~Waiter()
        {
            if (table_ != nullptr) {
                table_->leave(path_);
            }
        }

        // Blocks until the owner is finished.
        auto wait() const -> Outcome { return table_->await(path_); }

    private:
        friend class UploadClaims;
        Waiter(UploadClaims & table, std::string path) : table_(&table), path_(std::move(path)) {}
        UploadClaims * table_;
        std::string path_;
    };

    struct Ticket
    {
        std::optional<Claim> owner;
        std::optional<Waiter> waiter;
    };

    auto enter(const std::string & path) -> Ticket
    {
        std::scoped_lock const lock(mutex_);
        if (active_.emplace(path, Outcome::pending).second) {
            return {.owner = Claim(*this, path), .waiter = std::nullopt};
        }
        ++waiters_[path];
        return {.owner = std::nullopt, .waiter = Waiter(*this, path)};
    }

private:
    void finish(const std::string & path, Outcome outcome)
    {
        std::scoped_lock const lock(mutex_);
        if (waiters_.contains(path)) {
            active_.at(path) = outcome;
        } else {
            active_.erase(path);
        }
        wakeup_.notify_all();
    }

    auto await(const std::string & path) -> Outcome
    {
        std::unique_lock lock(mutex_);
        wakeup_.wait(lock, [&] -> bool { return active_.at(path) != Outcome::pending; });
        return active_.at(path);
    }

    void leave(const std::string & path)
    {
        std::scoped_lock const lock(mutex_);
        if (--waiters_.at(path) == 0) {
            waiters_.erase(path);
            if (active_.at(path) != Outcome::pending) {
                active_.erase(path);
            }
        }
    }

    std::mutex mutex_;
    std::condition_variable wakeup_;
    std::map<std::string, Outcome> active_;
    std::map<std::string, unsigned> waiters_;
};

} // namespace nixgrpc
