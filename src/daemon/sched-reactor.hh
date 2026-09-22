#pragma once
// Callback reactors for the Schedule and WorkerSession streams. Reads go to
// the Dispatcher. Its send() only enqueues, and the reactor writes the queue
// out in batches, so no I/O happens under the Dispatcher mutex and a peer
// that stops reading is cancelled once its queue is full.

#include <cstddef>
#include <deque>
#include <exception>
#include <memory>
#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <string>
#include <utility>

#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>

#include "dispatcher.hh"
#include "logfmt.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

// shared_ptr: the send closure in the Dispatcher may outlive the reactor.
template<typename Msg, typename Batch>
struct SendQueue
{
    // About a minute of Expects at full rate. Beyond it the peer is stuck.
    static constexpr size_t limit = 1U << 16U;
    // Messages per Write.
    static constexpr int maxBatch = 1024;

    // Leaf lock, may be taken inside Dispatcher::mutex.
    absl::Mutex mutex;
    std::deque<Msg> queue ABSL_GUARDED_BY(mutex);
    bool writing ABSL_GUARDED_BY(mutex) = false; // a StartWrite is outstanding
    bool closed ABSL_GUARDED_BY(mutex) = false;

    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) auto fill(Batch & out) -> bool
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

    // Runs under the Dispatcher mutex (keeps order). Starts a Write after it
    // unlocks if none is in flight. False means treat the peer as gone.
    auto sender(Dispatcher & disp) -> std::function<bool(const OutMsg &)>
    {
        return [this, &disp, sendq = sendq](const OutMsg & msg) ABSL_EXCLUSIVE_LOCKS_REQUIRED(disp.sendLock()) -> bool {
            bool kick = false;
            {
                const absl::MutexLock lock(sendq->mutex);
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
                // `writing` is set, so OnDone cannot fire before this runs.
                disp.afterUnlock([this] -> void { writeNext(); });
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
                const absl::MutexLock lock(sendq->mutex);
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
        const absl::MutexLock lock(sendq->mutex);
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

    // Caller has set `writing`.
    void writeNext()
    {
        bool have = false;
        {
            const absl::MutexLock lock(sendq->mutex);
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

    // Reads ended. Detach from the Dispatcher, Finish once no Write is in flight.
    void finish(grpc::Status fin)
    {
        gone();
        status = std::move(fin);
        {
            const absl::MutexLock lock(sendq->mutex);
            sendq->closed = true;
            readsDone = true;
        }
        maybeDone();
    }

    void maybeDone()
    {
        bool fin = false;
        {
            const absl::MutexLock lock(sendq->mutex);
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
    bool readsDone ABSL_GUARDED_BY(sendq->mutex) = false;
    bool finished ABSL_GUARDED_BY(sendq->mutex) = false;
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

// Turns a stream away with a status, without touching the Dispatcher.
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
