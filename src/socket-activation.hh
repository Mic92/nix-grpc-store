#pragma once
// systemd socket activation. gRPC cannot adopt a listening socket, but it
// accepts already-connected fds with any ServerCredentials, so we accept()
// ourselves.

#include <algorithm>
#include <chrono>
#include <optional>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <grpcpp/server_builder.h>

#include <nix/util/environment-variables.hh>
#include <nix/util/file-descriptor.hh>

#include "parse-int.hh"

namespace nixgrpc {

template<typename T>
inline auto envInt(const char * name) -> std::optional<T>
{
    return parseInt<T>(nix::getEnv(name).value_or(""));
}

// sd_listen_fds(3) without libsystemd. Call before starting threads.
inline auto systemdListenFds() -> std::vector<int>
{
    constexpr int firstFd = 3;
    auto count = envInt<int>("LISTEN_FDS");
    if (!count || envInt<pid_t>("LISTEN_PID") != ::getpid()) {
        return {};
    }
    std::vector<int> fds;
    for (int sockFd = firstFd; sockFd < firstFd + *count; ++sockFd) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): C API.
        ::fcntl(sockFd, F_SETFD, FD_CLOEXEC);
        fds.push_back(sockFd);
    }
    for (const char * name : {"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"}) {
        // NOLINTNEXTLINE(concurrency-mt-unsafe): runs before any thread starts.
        ::unsetenv(name);
    }
    return fds;
}

// sd_notify(3) without libsystemd. Best effort, errors are ignored.
inline void sdNotify(std::string_view state)
{
    auto path = nix::getEnv("NOTIFY_SOCKET");
    if (!path || path->size() < 2 || path->size() >= sizeof(sockaddr_un::sun_path)) {
        return;
    }
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::ranges::copy(*path, std::begin(addr.sun_path));
    if (path->starts_with('@')) {
        addr.sun_path[0] = '\0'; // abstract namespace
    }
    auto addrLen = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path->size());
    nix::AutoCloseFD const sock(::socket(AF_UNIX, SOCK_DGRAM, 0));
    if (!sock) {
        return;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
    ::sendto(sock.get(), state.data(), state.size(), 0, reinterpret_cast<const sockaddr *>(&addr), addrLen);
}

// Half of WatchdogSec=, or zero if the watchdog is off.
inline auto sdWatchdogInterval() -> std::chrono::microseconds
{
    auto usec = envInt<uint64_t>("WATCHDOG_USEC");
    return std::chrono::microseconds(usec.value_or(0) / 2);
}

// The threads live until process exit. After Server::Shutdown() the acceptor
// drops new connections itself.
inline void
acceptInto(const std::vector<int> & listenFds, const std::shared_ptr<grpc::experimental::ExternalConnectionAcceptor> & acceptor)
{
    for (int const listenFd : listenFds) {
        std::thread([listenFd, acceptor]() -> void {
            while (true) {
                // No accept4 on macOS.
                int const conn = ::accept(listenFd, nullptr, nullptr);
                if (conn < 0) {
                    continue;
                }
                // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): C API.
                ::fcntl(conn, F_SETFD, FD_CLOEXEC);
                ::fcntl(conn, F_SETFL, ::fcntl(conn, F_GETFL) | O_NONBLOCK);
                // NOLINTEND(cppcoreguidelines-pro-type-vararg)
                grpc::experimental::ExternalConnectionAcceptor::NewConnectionParameters params;
                params.listener_fd = listenFd;
                params.fd = conn;
                acceptor->HandleNewConnection(&params);
            }
        }).detach();
    }
}

} // namespace nixgrpc
