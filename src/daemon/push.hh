#pragma once
// The long-running `niks3 push --stdin` child and acknowledged publishing through it.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <stop_token>
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
        // Before stdin closes: the queued logs still go to the child.
        logWriter = {};
        {
            auto lck = state.lock();
            lck->stdinFd.reset(); // EOF: child drains and exits
        }
        if (reader.joinable()) {
            reader.join();
        }
    }

    // What niks3 signed each path with. Paths it already had come without.
    using Signatures = std::map<std::string, std::set<std::string>>;

    // Returns after niks3 committed all of `paths`.
    auto pushWait(const std::vector<std::string> & paths, const Cancelled & cancelled = never) -> Signatures
    {
        auto const pending = std::make_shared<Pending>(paths.size());
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
        auto res = pending->wait(cancelled);
        if (res.status == "cancelled") {
            state.lock()->waiting.erase(reqId);
            throw CancelledWait("niks3 push: %s", res.message);
        }
        if (res.status != "ok") {
            throw nix::Error("niks3 push: %s", res.message);
        }
        return std::move(res.signatures);
    }

    // Uploads the log Nix kept of `drvPath`. Returns at once: a log must not
    // delay the result of its build. The outcome goes to our own log.
    void pushLog(std::string drvPath)
    {
        std::scoped_lock const lock(logsMutex);
        if (logs.size() >= maxQueuedLogs) {
            logLine(LogLevel::info, {{"event", "build_log_dropped"}, {"drv", logs.front()}});
            logs.pop_front();
        }
        logs.push_back(std::move(drvPath));
        if (!logWriter.joinable()) {
            logWriter = std::jthread([this](const std::stop_token & stop) -> void { writeLogs(stop); });
        }
        logQueued.notify_one();
    }

private:
    // ack()/fail() run under the State lock, wait() under its own.
    struct Pending
    {
        void report()
        {
            if (log.empty()) {
                return;
            }
            auto const res = wait(never);
            if (res.status == "ok") {
                logLine(LogLevel::info, {{"event", "build_log_pushed"}, {"drv", log}});
            } else {
                logLine(LogLevel::info, {{"event", "build_log_push_failed"}, {"drv", log}, {"why", res.message}});
            }
        }

        struct Result
        {
            std::string status = "ok";
            std::string message;
            Signatures signatures;
        };

        // `log` names the derivation of a build log, which nobody waits for.
        explicit Pending(size_t count, std::string log = {})
            : log(std::move(log))
            , sync(Inner{.left = count})
        {
        }

        // True once every path of the request was acked.
        struct Ack
        {
            std::string status;
            std::string message;
            std::string path;
            std::set<std::string> signatures;
        };

        auto ack(Ack msg) -> bool
        {
            auto inner = sync.lock();
            if (msg.status != "ok") {
                inner->res.status = std::move(msg.status);
                inner->res.message = std::move(msg.message);
            } else if (!msg.signatures.empty()) {
                inner->res.signatures[msg.path] = std::move(msg.signatures);
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
            inner->res.status = "error";
            inner->res.message = message;
            inner->left = 0;
            done.notify_all();
        }

        auto wait(const Cancelled & cancelled) -> Result
        {
            auto inner = sync.lock();
            while (inner->left > 0) {
                if (cancelled()) {
                    return {.status = "cancelled", .message = "gave up waiting for niks3 push"};
                }
                inner.wait_for(done, cancelPoll);
            }
            return std::move(inner->res);
        }

    private:
        struct Inner
        {
            size_t left;
            Result res;
        };
        std::string log;
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

    // A child that reads slowly may hold up a write, so build logs go
    // through a thread of their own, and the oldest gives way.
    static constexpr size_t maxQueuedLogs = 64;
    std::mutex logsMutex;
    std::deque<std::string> logs;
    std::condition_variable_any logQueued;
    std::jthread logWriter;

    void writeLogs(const std::stop_token & stop)
    {
        for (;;) {
            std::string drvPath;
            {
                std::unique_lock lock(logsMutex);
                logQueued.wait(lock, stop, [&] -> bool { return !logs.empty(); });
                if (logs.empty()) {
                    return;
                }
                drvPath = std::move(logs.front());
                logs.pop_front();
            }
            try {
                sendLog(drvPath);
            } catch (nix::Error & err) {
                logLine(LogLevel::info, {{"event", "build_log_push_failed"}, {"drv", drvPath}, {"why", err.what()}});
            }
        }
    }

    void sendLog(const std::string & drvPath)
    {
        auto const pending = std::make_shared<Pending>(1, drvPath);
        uint64_t reqId = 0;
        std::shared_ptr<nix::AutoCloseFD> stdinFd;
        {
            auto lck = state.lock();
            ensureChild(lck);
            reqId = ++lck->lastId;
            lck->waiting[reqId] = pending;
            stdinFd = lck->stdinFd;
        }
        nlohmann::json const req{{"id", reqId}, {"log", drvPath}};
        try {
            std::scoped_lock const one(writeMutex);
            nix::writeFull(stdinFd->get(), req.dump() + "\n", false);
        } catch (...) {
            state.lock()->waiting.erase(reqId);
            throw;
        }
    }

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
            [&] -> void {
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
        reader = std::thread([this, acks = std::make_shared<nix::AutoCloseFD>(std::move(fromChild.readSide))] -> void {
            readAcks(acks->get());
        });
        logLine(LogLevel::info, {{"event", "niks3_push_started"}, {"pid", std::to_string(static_cast<pid_t>(lck->pid))}});
    }

    void readAcks(int acksFd)
    {
        std::string why;
        try {
            while (true) {
                auto const line = nix::readLine(acksFd);
                auto const ack = nlohmann::json::parse(line, nullptr, /*allow_exceptions=*/false);
                if (!ack.is_object()) {
                    continue;
                }
                auto lck = state.lock();
                auto const found = lck->waiting.find(ack.value("id", uint64_t{0}));
                if (found == lck->waiting.end()) {
                    continue;
                }
                if (found->second->ack(
                        {.status = ack.value("status", "error"),
                         .message = ack.value("message", ""),
                         .path = ack.value("path", ""),
                         .signatures = ack.value("signatures", std::set<std::string>{})})) {
                    found->second->report();
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
            pending->report();
        }
        lck->waiting.clear();
    }
};

} // namespace nixgrpc
