// http::Call against a socket that counts the connections it accepts.

#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <nix/util/sync.hh>

#include "http.hh"

namespace {

// htonl() and ntohs() without their header, which differs between platforms.
template<typename T>
auto networkOrder(T value) -> T
{
    return std::endian::native == std::endian::little ? std::byteswap(value) : value;
}

// Answers every request with 200 and keeps the connection open.
class Server
{
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
    sockaddr * generic = reinterpret_cast<sockaddr *>(&addr);
    std::atomic<int> accepted = 0;
    std::atomic<bool> stopping = false;
    nix::Sync<std::vector<int>> open;
    std::vector<std::jthread> peers;
    std::jthread acceptor;

    static void serve(int peer)
    {
        constexpr std::string_view reply = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        std::string seen;
        constexpr size_t bufSize = 4096;
        std::array<char, bufSize> buf{};
        for (;;) {
            auto const got = read(peer, buf.data(), buf.size());
            if (got <= 0) {
                break;
            }
            seen.append(buf.data(), static_cast<size_t>(got));
            for (auto end = seen.find("\r\n\r\n"); end != std::string::npos; end = seen.find("\r\n\r\n")) {
                seen.erase(0, end + 4);
                if (write(peer, reply.data(), reply.size()) < 0) {
                    break;
                }
            }
        }
        close(peer);
    }

    std::string url_;

public:
    Server()
    {
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = networkOrder<uint32_t>(INADDR_LOOPBACK);
        unsigned int len = sizeof addr; // socklen_t
        constexpr int backlog = 16;
        assert(bind(listener, generic, len) == 0);
        assert(listen(listener, backlog) == 0);
        assert(getsockname(listener, generic, &len) == 0);
        url_ = "http://127.0.0.1:" + std::to_string(networkOrder<uint16_t>(addr.sin_port)) + "/";
        acceptor = std::jthread([this] -> void {
            for (;;) {
                auto const peer = accept(listener, nullptr, nullptr);
                if (peer < 0) {
                    return;
                }
                if (stopping) {
                    close(peer);
                    return;
                }
                accepted++;
                open.lock()->push_back(peer);
                peers.emplace_back(serve, peer);
            }
        });
    }

    ~Server()
    {
        // Closing or shutting down a listening socket does not wake accept()
        // everywhere. A connection does.
        stopping = true;
        auto const wake = socket(AF_INET, SOCK_STREAM, 0);
        if (wake < 0 || connect(wake, generic, sizeof addr) != 0) {
            std::terminate(); // rather than hang in join()
        }
        close(wake);
        acceptor.join();
        // The client keeps its connections for as long as its thread lives.
        for (auto const peer : *open.lock()) {
            shutdown(peer, SHUT_RDWR);
        }
        peers.clear();
        close(listener);
    }
    Server(const Server &) = delete;
    Server(Server &&) = delete;
    auto operator=(const Server &) -> Server & = delete;
    auto operator=(Server &&) -> Server & = delete;

    [[nodiscard]] auto url() const -> const std::string &
    {
        return url_;
    }

    [[nodiscard]] auto connections() const -> int
    {
        return accepted;
    }
};

void get(const std::string & url)
{
    nixgrpc::http::Call call(url, "", std::nullopt);
    assert(nixgrpc::http::ok(call.perform()));
    assert(call.body() == "ok");
}

// A scheduler asks niks3 once per batch of Wants. A handshake per request
// is what it would otherwise spend its time on.
void callsOfAThreadShareAConnection()
{
    Server const server;
    get(server.url());
    get(server.url());
    get(server.url());
    assert(server.connections() == 1);
    std::jthread([&] -> void { get(server.url()); }).join();
    assert(server.connections() == 2);
}

} // namespace

auto main() -> int
try {
    callsOfAThreadShareAConnection();
    return 0;
} catch (std::exception &) {
    return 1;
}
