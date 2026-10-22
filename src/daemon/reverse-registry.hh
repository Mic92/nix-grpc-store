#pragma once
// Exact-worker lookup for future worker-initiated data sessions. This is only
// the lifetime/identity boundary; no RPC is routed through it yet.

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "reverse-transport.hh"

namespace nixgrpc {

class ReverseRegistry
{
public:
    struct Session
    {
        std::string workerId;
        uint64_t generation = 0;
        // Per-registration key delivered only to the verified worker. Never
        // expose this to client-facing metadata or log it.
        std::string sessionKey;
        std::shared_ptr<ReverseTransport> transport;
        // The eventual stream owner must cancel in-flight calls when closed.
        std::function<void()> cancel;
    };
    using Handle = std::shared_ptr<const Session>;

    // `peerId` must come from a verified *unique* worker credential, never
    // from RPC metadata or Hello. The chart's shared worker certificate is
    // insufficient; a pod-bound, tunnel-audience JWT is one possible source.
    // No anonymous registration, even with an empty ACL.
    auto registerWorker(
        std::string_view peerId, std::string_view workerId, std::function<void()> cancel,
        std::string sessionKey = {}, std::shared_ptr<ReverseTransport> transport = {})
        -> std::optional<Handle>
    {
        if (peerId.empty() || peerId != workerId || workerId.size() > 128 || workerId.contains('\0') || !cancel) {
            return std::nullopt;
        }
        Handle previous;
        Handle current;
        {
            const std::scoped_lock lock(mutex);
            if (!active) {
                return std::nullopt;
            }
            auto & slot = sessions[std::string(workerId)];
            previous = std::move(slot);
            current = std::make_shared<Session>(Session{
                .workerId = std::string(workerId), .generation = ++generation,
                .sessionKey = std::move(sessionKey), .transport = std::move(transport), .cancel = std::move(cancel)});
            slot = current;
        }
        // Never call transport code under the registry lock.
        if (previous) {
            if (previous->transport) {
                previous->transport->close();
            }
            previous->cancel();
        }
        return current;
    }

    // The caller owns a Handle for the duration of any selected RPC. Before
    // forwarding it must check current(handle) again, fail closed on false,
    // and cancel ongoing calls if the transport dies. A check alone is not
    // a lease: replacement can occur immediately afterwards; the transport
    // must also reject writes to displaced handles and cancel their calls.
    [[nodiscard]] auto find(std::string_view workerId) const -> Handle
    {
        const std::scoped_lock lock(mutex);
        if (!active) {
            return {};
        }
        auto it = sessions.find(std::string(workerId));
        return it == sessions.end() ? Handle{} : it->second;
    }

    [[nodiscard]] auto current(const Handle & handle) const -> bool
    {
        if (!handle) {
            return false;
        }
        const std::scoped_lock lock(mutex);
        auto it = sessions.find(handle->workerId);
        return active && it != sessions.end() && it->second == handle;
    }

    // External disconnects cancel in-flight transport work. A stream owner
    // that is already returning a status may release its own slot without
    // cancelling its context (which would mask the original status).
    void disconnected(const Handle & handle, bool cancel = true)
    {
        if (!handle) {
            return;
        }
        Handle removed;
        {
            const std::scoped_lock lock(mutex);
            auto it = sessions.find(handle->workerId);
            if (it != sessions.end() && it->second == handle) {
                removed = std::move(it->second);
                sessions.erase(it);
            }
        }
        if (removed) {
            if (removed->transport) {
                removed->transport->close();
            }
            if (cancel) {
                removed->cancel();
            }
        }
    }

    void setActive(bool value)
    {
        std::vector<Handle> previous;
        {
            const std::scoped_lock lock(mutex);
            active = value;
            if (!value) {
                for (auto & [id, session] : sessions) {
                    previous.push_back(std::move(session));
                }
                sessions.clear();
            }
        }
        for (const auto & session : previous) {
            if (session->transport) {
                session->transport->close();
            }
            session->cancel();
        }
    }

private:
    mutable std::mutex mutex;
    bool active = false; // never accept a session on an unelected standby
    uint64_t generation = 0;
    std::unordered_map<std::string, Handle> sessions;
};

} // namespace nixgrpc
