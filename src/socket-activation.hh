#pragma once
// systemd socket activation. gRPC cannot adopt a listening socket, but it
// accepts already-connected fds with any ServerCredentials, so we accept()
// ourselves.

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <grpcpp/server_builder.h>

#include <nix/util/environment-variables.hh>

namespace nixgrpc {

// sd_listen_fds(3) without libsystemd. Call before starting threads.
inline auto systemdListenFds() -> std::vector<int>
{
    constexpr int firstFd = 3;
    auto pidEnv = nix::getEnv("LISTEN_PID");
    auto fdsEnv = nix::getEnv("LISTEN_FDS");
    if (!pidEnv || !fdsEnv || std::stol(*pidEnv) != ::getpid()) {
        return {};
    }
    std::vector<int> fds;
    for (int sockFd = firstFd; sockFd < firstFd + std::stoi(*fdsEnv); ++sockFd) {
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
