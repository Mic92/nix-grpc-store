#pragma once
// Copies the build logs of builds that ran here to the farm's log store
// (--build-log-store), so they outlive the builder: a retired cloud builder
// takes Nix's kept logs with it. One thread, off the build's path: Done
// never waits for an upload.

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "logfmt.hh"
#include "metrics.hh"

namespace nixgrpc {

class LogUploader
{
public:
    static constexpr size_t maxQueue = 256;
    // A larger log is skipped: addBuildLog takes a log whole, in memory.
    static constexpr uint64_t maxLog = uint64_t{64} << 20U;

    // The kept log of a derivation, decoded, at most `limit` bytes plus one;
    // nullopt when there is none.
    using ReadLog = std::function<std::optional<std::string>(const std::string & drvPath, uint64_t limit)>;
    // Stores one log. Throws when the store refuses it.
    using Store = std::function<void(const std::string & drvPath, std::string_view log)>;

    LogUploader(ReadLog read, Store store, Metrics & metrics)
        : read(std::move(read))
        , store(std::move(store))
        , metrics(metrics)
        , thread([this](const std::stop_token & stop) -> void { loop(stop); })
    {
    }

    LogUploader(const LogUploader &) = delete;
    auto operator=(const LogUploader &) -> LogUploader & = delete;
    LogUploader(LogUploader &&) = delete;
    auto operator=(LogUploader &&) -> LogUploader & = delete;

    ~LogUploader()
    {
        thread.request_stop();
        wake.notify_all();
    }

    // After a build that ran here reported Done. A full queue drops its
    // oldest entry.
    void enqueue(std::string drvPath)
    {
        {
            const std::scoped_lock lock(mutex);
            if (queue.size() >= maxQueue) {
                queue.pop_front();
                metrics.buildLogUpload("dropped");
            }
            queue.push_back(std::move(drvPath));
            metrics.buildLogUploadQueue(queue.size() + (busy ? 1 : 0));
        }
        wake.notify_all();
    }

    // Nothing queued or uploading.
    [[nodiscard]] auto idle() const -> bool
    {
        const std::scoped_lock lock(mutex);
        return queue.empty() && !busy;
    }

private:
    void loop(const std::stop_token & stop)
    {
        while (true) {
            std::string drv;
            {
                std::unique_lock lock(mutex);
                busy = false;
                metrics.buildLogUploadQueue(queue.size());
                wake.wait(lock, stop, [&] -> bool { return !queue.empty(); });
                if (stop.stop_requested()) {
                    return;
                }
                drv = std::move(queue.front());
                queue.pop_front();
                busy = true;
            }
            metrics.buildLogUpload(upload(drv));
        }
    }

    auto upload(const std::string & drv) -> std::string_view
    {
        try {
            auto log = read(drv, maxLog);
            if (!log) {
                return "skipped";
            }
            if (log->size() > maxLog) {
                logLine(LogLevel::info, {{"event", "build_log_upload_skipped"}, {"drv", drv}, {"reason", "over 64 MiB"}});
                return "skipped";
            }
            store(drv, *log);
            return "ok";
        } catch (std::exception & err) {
            logLine(LogLevel::info, {{"event", "build_log_upload_failed"}, {"drv", drv}, {"error", err.what()}});
            return "failed";
        }
    }

    ReadLog read;
    Store store;
    Metrics & metrics;
    mutable std::mutex mutex;
    std::condition_variable_any wake;
    std::deque<std::string> queue;
    bool busy = false;
    std::jthread thread; // last: it uses everything above
};

} // namespace nixgrpc
