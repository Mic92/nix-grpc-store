#pragma once
// The long-running `niks3 push --stdin` child and acknowledged publishing through it.

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

#include "logfmt.hh"

namespace nixgrpc {

// Polled about once a second by blocking waits so a gone client or a
// daemon shutdown does not pin a handler thread forever.
using Cancelled = std::function<bool()>;
constexpr std::chrono::seconds cancelPoll{1};

struct CancelledWait : nix::Error
{
    using nix::Error::Error;
};

inline auto never() -> bool
{
    return false;
}


// One long-lived `niks3 push --stdin`. A request line names paths and blocks
// until every one was acked. If the child dies all waiters fail and the next
// request respawns it.
class PushProcess
{
public:
    using Argv = std::vector<std::string>;

    explicit PushProcess(Argv argv)
        : argv(std::move(argv))
    {
    }

    PushProcess(const PushProcess &) = delete;
    PushProcess(PushProcess &&) = delete;
    auto operator=(const PushProcess &) -> PushProcess & = delete;
    auto operator=(PushProcess &&) -> PushProcess & = delete;

    ~PushProcess()
    {
        {
            auto lck = state.lock();
            lck->stdinFd.reset(); // EOF: child drains and exits
        }
        if (reader.joinable()) {
            reader.join();
        }
    }

    // Returns after niks3 committed all of `paths`.
    void pushWait(const std::vector<std::string> & paths, const Cancelled & cancelled = never)
    {
        auto pending = std::make_shared<Pending>(paths.size());
        uint64_t reqId = 0;
        std::shared_ptr<nix::AutoCloseFD> stdinFd;
        {
            auto lck = state.lock();
            ensureChild(lck);
            reqId = ++lck->lastId;
            lck->waiting[reqId] = pending;
            stdinFd = lck->stdinFd;
        }
        // Not under the State lock: a full stdin pipe must not keep readAcks
        // from draining the child's stdout.
        nlohmann::json const req{{"id", reqId}, {"paths", paths}};
        try {
            std::scoped_lock const one(writeMutex);
            nix::writeFull(stdinFd->get(), req.dump() + "\n", false);
        } catch (...) {
            state.lock()->waiting.erase(reqId);
            throw;
        }
        auto [status, message] = pending->wait(cancelled);
        if (status == "cancelled") {
            state.lock()->waiting.erase(reqId);
            throw CancelledWait("niks3 push: %s", message);
        }
        if (status != "ok") {
            throw nix::Error("niks3 push: %s", message);
        }
    }

private:
    // ack()/fail() run under the State lock, wait() under its own.
    struct Pending
    {
        using Result = std::pair<std::string, std::string>;

        explicit Pending(size_t count)
            : sync(Inner{.left = count})
        {
        }

        // True once every path of the request was acked.
        auto ack(const std::string & status, const std::string & message) -> bool
        {
            auto inner = sync.lock();
            if (status != "ok") {
                inner->worst = {status, message};
            }
            if (inner->left > 0) {
                inner->left--;
            }
            if (inner->left == 0) {
                done.notify_all();
            }
            return inner->left == 0;
        }

        void fail(const std::string & message)
        {
            auto inner = sync.lock();
            inner->worst = {"error", message};
            inner->left = 0;
            done.notify_all();
        }

        auto wait(const Cancelled & cancelled) -> Result
        {
            auto inner = sync.lock();
            while (inner->left > 0) {
                if (cancelled()) {
                    return {"cancelled", "gave up waiting for niks3 push"};
                }
                inner.wait_for(done, cancelPoll);
            }
            return inner->worst;
        }

    private:
        struct Inner
        {
            size_t left;
            Result worst{"ok", ""};
        };
        nix::Sync<Inner> sync;
        std::condition_variable done;
    };

    struct State
    {
        nix::Pid pid;
        std::shared_ptr<nix::AutoCloseFD> stdinFd;
        uint64_t lastId = 0;
        std::map<uint64_t, std::shared_ptr<Pending>> waiting;
    };

    Argv argv;
    nix::Sync<State> state;
    std::mutex writeMutex;
    std::thread reader;

    void ensureChild(nix::Sync<State>::WriteLock & lck)
    {
        if (lck->stdinFd) {
            return;
        }
        if (reader.joinable()) {
            reader.join(); // previous child is gone, its reader returned
        }
        nix::Pipe toChild;
        nix::Pipe fromChild;
        toChild.create();
        fromChild.create();
        // dieWithParent (PDEATHSIG) fires when the forking gRPC thread exits.
        lck->pid = nix::startProcess(
            [&]() -> void {
                if (dup2(toChild.readSide.get(), STDIN_FILENO) == -1
                    || dup2(fromChild.writeSide.get(), STDOUT_FILENO) == -1) {
                    throw nix::SysError("dup2");
                }
                nix::Strings const args(argv.begin(), argv.end());
                execvp(argv.front().c_str(), nix::stringsToCharPtrs(args).data());
                throw nix::SysError("exec %s", argv.front());
            },
            {.dieWithParent = false});
        lck->stdinFd = std::make_shared<nix::AutoCloseFD>(std::move(toChild.writeSide));
        reader = std::thread([this, acks = std::make_shared<nix::AutoCloseFD>(std::move(fromChild.readSide))]() -> void {
            readAcks(acks->get());
        });
        logLine(LogLevel::info, {{"event", "niks3_push_started"}, {"pid", std::to_string(static_cast<pid_t>(lck->pid))}});
    }

    void readAcks(int acksFd)
    {
        std::string why;
        try {
            while (true) {
                auto line = nix::readLine(acksFd);
                auto ack = nlohmann::json::parse(line, nullptr, /*allow_exceptions=*/false);
                if (!ack.is_object()) {
                    continue;
                }
                auto lck = state.lock();
                auto found = lck->waiting.find(ack.value("id", uint64_t{0}));
                if (found == lck->waiting.end()) {
                    continue;
                }
                if (found->second->ack(ack.value("status", "error"), ack.value("message", ""))) {
                    lck->waiting.erase(found);
                }
            }
        } catch (nix::EndOfFile &) {
            why = "niks3 push exited";
        } catch (std::exception & err) {
            why = err.what();
        }
        auto lck = state.lock();
        lck->stdinFd.reset();
        int const status = lck->pid.kill();
        logLine(LogLevel::info, {{"event", "niks3_push_exited"}, {"status", nix::statusToString(status)}, {"why", why}});
        for (auto & [reqId, pending] : lck->waiting) {
            pending->fail(why);
        }
        lck->waiting.clear();
    }
};

} // namespace nixgrpc
