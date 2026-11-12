#pragma once
// Asks the local nix-daemon whether it still takes connections. nix itself
// waits without limit for a nix-daemon that accepts and then says nothing,
// so this speaks the first step of the protocol by hand, against a deadline.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <span>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <nix/util/file-descriptor.hh>
#include <nix/util/unix-domain-socket.hh>

namespace nixgrpc {

namespace probe {
// The worker protocol speaks in 64-bit little-endian words.
constexpr std::size_t wordBytes = 8;
using Word = std::array<unsigned char, wordBytes>;
constexpr Word clientMagic{0x63, 0x78, 0x69, 0x6e, 0, 0, 0, 0};
constexpr Word daemonMagic{0x6f, 0x69, 0x78, 0x64, 0, 0, 0, 0};
// All a client says to open a connection, at protocol 1.35: its magic, its
// version, and two obsolete settings. With all of it sent the nix-daemon
// takes the end of the stream for a client that is done, and logs nothing.
constexpr std::array<unsigned char, 4 * sizeof(Word)> hello{
    0x63, 0x78, 0x69, 0x6e, 0, 0, 0, 0, 0x23, 0x01, 0, 0, 0, 0, 0, 0,
};

// Reads until `want` bytes arrived (true) or the stream ended or `deadline` passed.
inline auto readBy(int sock, std::span<unsigned char> want, std::chrono::steady_clock::time_point deadline) -> bool
{
    while (!want.empty()) {
        auto const left =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        pollfd waiting{.fd = sock, .events = POLLIN, .revents = 0};
        if (left.count() <= 0 || ::poll(&waiting, 1, static_cast<int>(left.count())) <= 0) {
            return false;
        }
        auto const count = ::read(sock, want.data(), want.size());
        if (count <= 0) {
            return false;
        }
        want = want.subspan(static_cast<std::size_t>(count));
    }
    return true;
}
} // namespace probe

// True if the nix-daemon listening at `socket` greets a new connection
// within `timeout`.
inline auto nixDaemonAnswers(const std::filesystem::path & socket, std::chrono::milliseconds timeout) -> bool
{
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    try {
        auto const sock = nix::createUnixDomainSocket();
        // Non-blocking, so a full accept queue fails the connect.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,bugprone-signed-bitwise): fcntl
        if (::fcntl(sock.get(), F_SETFL, ::fcntl(sock.get(), F_GETFL) | O_NONBLOCK) != 0) {
            return false;
        }
        nix::connect(sock.get(), socket);
        if (::write(sock.get(), probe::hello.data(), probe::hello.size()) != static_cast<ssize_t>(probe::hello.size())) {
            return false;
        }
        probe::Word answer{};
        if (!probe::readBy(sock.get(), answer, deadline) || answer != probe::daemonMagic) {
            return false;
        }
        // Leave politely: a connection closed with its answer unread is an
        // error in the nix-daemon's log.
        ::shutdown(sock.get(), SHUT_WR);
        std::array<unsigned char, sizeof probe::hello> rest{};
        while (probe::readBy(sock.get(), rest, deadline)) {
        }
        return true;
    } catch (const std::exception &) {
        return false;
    }
}

} // namespace nixgrpc
