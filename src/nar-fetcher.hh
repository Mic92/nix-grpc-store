#pragma once
// Sharded NAR download client for the grpc store plugin.
//
// Downloads are sharded over N NarsFromPaths streams, each on its own TCP
// connection, because one connection cannot fill a high
// bandwidth-delay-product link. Paths go to the least-loaded stream and
// are pipelined in the order recorded via recordOrder(), so many small
// paths cost ~1 round trip. Each stream has one reader thread that spools
// every NAR to an unlinked temp file and fulfills a promise. fetch()
// waits on the future, so restores run concurrently in the caller's
// thread pool. The path and byte caps bound spool size and the bytes
// wasted on an aborted copy.
//
// Two structural properties carry the safety argument: the futures map
// is the single "already requested" marker and entries never revert to
// absent while a request can be in flight, so a path cannot be requested
// twice; and each promise travels inside its session's queue entry, so
// the reader can never see a request without its promise.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>
#include <map>
#include <memory>
#include <mutex>
#include <nix/store/path.hh>
#include <nix/util/archive.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

namespace nixgrpc {

class NarFetcher
{
public:
    NarFetcher(
        std::function<std::shared_ptr<grpc::Channel>()> channelFactory,
        std::string authority,
        unsigned connections)
        : channelFactory(std::move(channelFactory))
        , authority(std::move(authority))
        , connections(std::max(1U, connections))
    {
    }

    NarFetcher(const NarFetcher &) = delete;
    NarFetcher(NarFetcher &&) = delete;
    auto operator=(const NarFetcher &) -> NarFetcher & = delete;
    auto operator=(NarFetcher &&) -> NarFetcher & = delete;

    // Predicted fetch() call order with narSizes, used for pipelining.
    void recordOrder(std::vector<nix::StorePath> order, std::map<nix::StorePath, uint64_t> sizes)
    {
        std::scoped_lock const lock(narMutex);
        expectedOrder = std::move(order);
        narSizes = std::move(sizes);
        cursor = 0;
    }

    // Returns the NAR for `path` as an unlinked spool file, rewound.
    auto fetch(const nix::StorePath & path) -> nix::AutoCloseFD
    {
        std::future<nix::AutoCloseFD> pending;
        {
            std::scoped_lock const lock(narMutex);
            if (sessions.empty()) {
                for (unsigned i = 0; i < connections; ++i) {
                    sessions.push_back(std::unique_ptr<Session>(new Session(*this)));
                }
            }
            auto requested = futures.find(path);
            // A present but invalid entry is a consumed request. Only then
            // is a fresh request safe: nothing for the path is in flight.
            if (requested == futures.end() || !requested->second.valid()) {
                requested = futures.insert_or_assign(path, requestPath(path)).first;
            }
            pending = std::move(requested->second);
            topUpPipeline();
        }

        auto spoolFd = pending.get();
        if (::lseek(spoolFd.get(), 0, SEEK_SET) == -1) {
            throw nix::SysError("seeking NAR spool file");
        }
        return spoolFd;
    }

    ~NarFetcher()
    {
        for (const auto & session : sessions) {
            bool idle = false;
            {
                std::scoped_lock const lock(session->mutex);
                session->closing = true;
                idle = session->queue.empty() && !session->spoolingActive;
            }
            session->wakeup.notify_all();
            // With NARs still in flight (aborted copy), draining would
            // download them all just to close the stream. Cancel instead.
            if (!idle) {
                session->ctx.TryCancel();
            }
            if (session->reader.joinable()) {
                session->reader.join();
            }
            if (session->broken) {
                continue;
            }
            // Let the server end its NarsFromPaths handler cleanly.
            session->stream->WritesDone();
            nix::remote::NarChunk chunk;
            while (session->stream->Read(&chunk)) {
                ;
            }
            (void)session->stream->Finish();
        }
    }

private:
    static constexpr size_t kPipelineWindow = 64;
    static constexpr uint64_t kPipelineWindowBytes = 128ULL * 1024 * 1024;

    using NarStream = grpc::ClientReaderWriter<nix::remote::NarRequest, nix::remote::NarChunk>;

    // A requested NAR: the promise travels with the queue entry.
    struct Item
    {
        uint64_t size;
        std::promise<nix::AutoCloseFD> promise;
    };

    class Session
    {
        friend class NarFetcher;

        std::unique_ptr<nix::remote::NixRemote::Stub> stub;
        grpc::ClientContext ctx;
        std::unique_ptr<NarStream> stream;
        ZstdReaderSource<NarStream, nix::remote::NarChunk> source;

        std::mutex mutex;
        std::condition_variable wakeup;
        // Guarded by mutex.
        std::deque<Item> queue;
        bool spoolingActive = false;
        bool closing = false;

        // Guarded by the fetcher's narMutex.
        uint64_t loadBytes = 0;

        // Set before the reader finishes the stream. Refuses new requests.
        std::atomic<bool> broken = false;

        NarFetcher & fetcher;
        std::thread reader;

        explicit Session(NarFetcher & owner)
            : stub(nix::remote::NixRemote::NewStub(owner.channelFactory()))
            , stream(requireStream(stub->NarsFromPaths(&ctx), owner.authority))
            , source(*stream, std::string{})
            , fetcher(owner)
            , reader([this] -> void { run(); })
        {
        }

    public:
        Session(const Session &) = delete;
        Session(Session &&) = delete;
        auto operator=(const Session &) -> Session & = delete;
        auto operator=(Session &&) -> Session & = delete;
        ~Session() = default;

    private:
        static auto requireStream(std::unique_ptr<NarStream> stream, const std::string & authority)
            -> std::unique_ptr<NarStream>
        {
            if (!stream) {
                throw nix::Error("failed to open gRPC NarsFromPaths stream to '%s'", authority);
            }
            return stream;
        }

        // Caller holds narMutex.
        auto request(const nix::StorePath & path, uint64_t narSize) -> std::future<nix::AutoCloseFD>
        {
            nix::remote::NarRequest req;
            req.set_path(std::string(path.to_string()));
            if (broken || !stream->Write(req)) {
                throw nix::Error("gRPC stream closed by peer");
            }
            Item item{.size = narSize, .promise = {}};
            auto result = item.promise.get_future();
            {
                std::scoped_lock const lock(mutex);
                queue.push_back(std::move(item));
            }
            loadBytes += narSize;
            wakeup.notify_one();
            return result;
        }

        void run()
        {
            while (true) {
                std::optional<Item> item;
                {
                    std::unique_lock lock(mutex);
                    wakeup.wait(lock, [&] -> bool { return closing || !queue.empty(); });
                    if (queue.empty()) {
                        return;
                    }
                    item = std::move(queue.front());
                    queue.pop_front();
                    spoolingActive = true;
                }
                try {
                    auto spoolFd = spoolOne(source);
                    {
                        std::scoped_lock const lock(fetcher.narMutex);
                        fetcher.settle(*this, item->size);
                    }
                    {
                        std::scoped_lock const lock(mutex);
                        spoolingActive = false;
                    }
                    item->promise.set_value(std::move(spoolFd));
                } catch (...) {
                    fail(std::current_exception(), std::move(*item));
                    return;
                }
            }
        }

        void fail(const std::exception_ptr & cause, Item current)
        {
            broken = true;
            ctx.TryCancel();
            grpc::Status status;
            std::deque<Item> abandoned;
            {
                // narMutex keeps Finish() from racing concurrent Write()s.
                std::scoped_lock const narLock(fetcher.narMutex);
                status = stream->Finish();
                std::scoped_lock const lock(mutex);
                abandoned.swap(queue);
                spoolingActive = false;
                fetcher.settle(*this, current.size);
                for (const auto & item : abandoned) {
                    fetcher.settle(*this, item.size);
                }
            }
            auto failure = !status.ok() && status.error_code() != grpc::StatusCode::CANCELLED
                               ? std::make_exception_ptr(nix::Error(
                                     "gRPC NarsFromPaths failed: %s", status.error_message()))
                               : cause;
            current.promise.set_exception(failure);
            for (auto & item : abandoned) {
                item.promise.set_exception(failure);
            }
        }

        static auto spoolOne(nix::Source & source) -> nix::AutoCloseFD
        {
            auto [tmpFd, tmpPath] = nix::createTempFile("nix-grpc-nar");
            ::unlink(tmpPath.c_str());
            nix::FdSink tmpSink(tmpFd.get());
            copyNAR(source, tmpSink);
            tmpSink.flush();
            return std::move(tmpFd);
        }
    };

    std::function<std::shared_ptr<grpc::Channel>()> channelFactory;
    std::string authority;
    unsigned connections;

    std::mutex narMutex;
    std::vector<std::unique_ptr<Session>> sessions;

    // Guarded by narMutex. A futures entry means the path was requested;
    // fetch() moves the future out but leaves the key in place.
    std::map<nix::StorePath, std::future<nix::AutoCloseFD>> futures;
    std::vector<nix::StorePath> expectedOrder;
    std::map<nix::StorePath, uint64_t> narSizes;
    size_t cursor = 0;
    size_t flyingPaths = 0;
    uint64_t flyingBytes = 0;

    [[nodiscard]] auto narSizeOf(const nix::StorePath & path) const -> uint64_t
    {
        auto found = narSizes.find(path);
        return found == narSizes.end() ? 0 : found->second;
    }

    // Callers of settle/requestPath/topUpPipeline hold narMutex.
    void settle(Session & session, uint64_t size)
    {
        session.loadBytes -= size;
        flyingBytes -= size;
        --flyingPaths;
    }

    auto requestPath(const nix::StorePath & path) -> std::future<nix::AutoCloseFD>
    {
        Session * session = nullptr;
        for (const auto & candidate : sessions) {
            if (candidate->broken) {
                continue;
            }
            if (session == nullptr || candidate->loadBytes < session->loadBytes) {
                session = candidate.get();
            }
        }
        if (session == nullptr) {
            throw nix::Error("all gRPC NarsFromPaths streams failed");
        }
        auto size = narSizeOf(path);
        auto result = session->request(path, size);
        flyingBytes += size;
        ++flyingPaths;
        return result;
    }

    void topUpPipeline()
    {
        while (cursor < expectedOrder.size()) {
            const auto & candidate = expectedOrder.at(cursor);
            if (futures.contains(candidate)) {
                ++cursor;
                continue;
            }
            if (flyingPaths >= kPipelineWindow || flyingBytes >= kPipelineWindowBytes) {
                break;
            }
            futures.emplace(candidate, requestPath(candidate));
            ++cursor;
        }
    }
};

} // namespace nixgrpc
