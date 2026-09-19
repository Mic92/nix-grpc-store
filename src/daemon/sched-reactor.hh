#pragma once
// Callback-API pumps for the two Scheduler streams. One reactor per stream:
// reads feed the Dispatcher, the Dispatcher's send() only enqueues, and the
// reactor writes whatever has queued as one batched message when the previous
// write completes. No thread per stream, no I/O under the Dispatcher mutex,
// and a peer that stops reading fills its queue and is cancelled instead of
// stalling everyone else.

#include <cstddef>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>

#include "dispatcher.hh"
#include "logfmt.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

// Send queue shared between a reactor and the Dispatcher's send closure; the
// closure may outlive the reactor briefly (worker table), hence shared_ptr.
template<typename Msg, typename Batch>
struct SendQueue
{
    // ~1 min of Expects at full tilt; a live peer never gets near it.
    static constexpr size_t limit = 1U << 16U;
    // Per Write; bounds peer parse latency and message size.
    static constexpr int maxBatch = 1024;

    std::mutex mutex;
    std::deque<Msg> queue;
    bool writing = false; // a StartWrite is outstanding
    bool closed = false;

    // Under `mutex`: move up to maxBatch into `out`; true if there is anything to write.
    auto fill(Batch & out) -> bool
    {
        out.clear_msgs();
        while (!queue.empty() && out.msgs_size() < maxBatch) {
            *out.add_msgs() = std::move(queue.front());
            queue.pop_front();
        }
        return out.msgs_size() > 0;
    }
};

template<typename In, typename OutMsg, typename Out>
class SchedReactorBase : public grpc::ServerBidiReactor<In, Out>
{
public:
    using Queue = SendQueue<OutMsg, Out>;

    explicit SchedReactorBase(grpc::CallbackServerContext * ctx)
        : ctx(ctx)
        , sendq(std::make_shared<Queue>())
    {
    }

    // For the Dispatcher: enqueue under its lock (keeps order), kick the
    // write after it unlocks if none is in flight. False = treat peer as gone.
    auto sender(Dispatcher & disp) -> std::function<bool(const OutMsg &)>
    {
        return [this, &disp, sendq = sendq](const OutMsg & msg) -> bool {
            bool kick = false;
            {
                const std::scoped_lock lock(sendq->mutex);
                if (sendq->closed || sendq->queue.size() >= Queue::limit) {
                    if (!sendq->closed) {
                        sendq->closed = true;
                        ctx->TryCancel();
                    }
                    return false;
                }
                sendq->queue.push_back(msg);
                if (!sendq->writing) {
                    sendq->writing = kick = true;
                }
            }
            if (kick) {
                // `writing` is claimed, so OnDone cannot fire before this runs.
                disp.afterUnlock([this]() -> void { writeNext(); });
            }
            return true;
        };
    }

    void start()
    {
        this->StartRead(&in);
    }

    void OnReadDone(bool isOk) override
    {
        if (!isOk) {
            finish(grpc::Status::OK);
            return;
        }
        try {
            onMsgs(in);
        } catch (std::exception & err) {
            finish({grpc::StatusCode::INVALID_ARGUMENT, err.what()});
            return;
        }
        this->StartRead(&in);
    }

    void OnWriteDone(bool isOk) override
    {
        if (!isOk) {
            {
                const std::scoped_lock lock(sendq->mutex);
                sendq->closed = true;
                sendq->writing = false;
            }
            ctx->TryCancel();
            maybeDone();
            return;
        }
        writeNext();
    }

    void OnCancel() override
    {
        const std::scoped_lock lock(sendq->mutex);
        sendq->closed = true;
    }

    void OnDone() override
    {
        delete this;
    }

protected:
    virtual void onMsgs(const In & msgs) = 0;
    virtual void gone() = 0;
    [[nodiscard]] auto context() const -> grpc::CallbackServerContext *
    {
        return ctx;
    }

private:
    grpc::CallbackServerContext * ctx;
    // Called with `writing` already claimed.
    void writeNext()
    {
        bool have = false;
        {
            const std::scoped_lock lock(sendq->mutex);
            have = !sendq->closed && sendq->fill(out);
            if (!have) {
                sendq->writing = false;
            }
        }
        if (have) {
            this->StartWrite(&out);
        } else {
            maybeDone();
        }
    }

    // Reads ended: detach from the Dispatcher, then Finish once no write is in flight.
    void finish(grpc::Status fin)
    {
        gone();
        status = std::move(fin);
        {
            const std::scoped_lock lock(sendq->mutex);
            sendq->closed = true;
            readsDone = true;
        }
        maybeDone();
    }

    void maybeDone()
    {
        bool fin = false;
        {
            const std::scoped_lock lock(sendq->mutex);
            fin = readsDone && !sendq->writing && !finished;
            finished |= fin;
        }
        if (fin) {
            this->Finish(status);
        }
    }

    std::shared_ptr<Queue> sendq;
    In in;
    Out out;
    grpc::Status status = grpc::Status::OK;
    bool readsDone = false; // under sendq->mutex
    bool finished = false;  // under sendq->mutex
};

class ScheduleReactor final
    : public SchedReactorBase<nix::remote::ClientMsgs, nix::remote::SchedMsg, nix::remote::SchedMsgs>
{
public:
    ScheduleReactor(grpc::CallbackServerContext * ctx, Dispatcher & disp)
        : SchedReactorBase(ctx)
        , disp(disp)
        , client(disp.connectClient(sender(disp)))
    {
        start();
    }

protected:
    void onMsgs(const nix::remote::ClientMsgs & msgs) override
    {
        disp.clientMsgs(client, msgs);
    }

    void gone() override
    {
        disp.clientGone(client);
    }

private:
    Dispatcher & disp;
    Dispatcher::ClientPtr client;
};

class WorkerSessionReactor final
    : public SchedReactorBase<nix::remote::WorkerMsgs, nix::remote::SchedCmd, nix::remote::SchedCmds>
{
public:
    WorkerSessionReactor(grpc::CallbackServerContext * ctx, Dispatcher & disp)
        : SchedReactorBase(ctx)
        , disp(disp)
        , worker{.send = sender(disp)}
    {
        start();
    }

protected:
    void onMsgs(const nix::remote::WorkerMsgs & msgs) override
    {
        disp.workerMsgs(worker, msgs);
    }

    void gone() override
    {
        logLine(LogLevel::info, {{"event", "worker_session_closed"}, {"peer", context()->peer()}});
        disp.workerGone(worker);
    }

private:
    Dispatcher & disp;
    Dispatcher::Worker worker;
};

// Rejects before any reactor state exists.
template<typename In, typename Out>
class RejectReactor final : public grpc::ServerBidiReactor<In, Out>
{
public:
    explicit RejectReactor(const grpc::Status & status)
    {
        this->Finish(status);
    }

    void OnDone() override
    {
        delete this;
    }
};

} // namespace nixgrpc
