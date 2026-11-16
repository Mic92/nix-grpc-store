#include "tunnel.hh"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef __APPLE__
#  include <sys/event.h>
#endif

#include <openssl/asn1.h>
#include <openssl/err.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/tls1.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>

#include "tunnel-protocol.hh"

// NOLINTBEGIN(misc-include-cleaner): which header declares a socket or OpenSSL name differs per platform and version.
namespace nixgrpc::tunnel {

namespace {

using Clock = std::chrono::steady_clock;

// Connecting, the TLS handshake and the balancer's answer together.
constexpr std::chrono::seconds openTimeout{30};

template<auto Release>
struct Free
{
    void operator()(auto * owned) const noexcept
    {
        (void) Release(owned); // NOLINT(cppcoreguidelines-owning-memory): fclose
    }
};
template<typename Owned, auto Release>
using Owner = std::unique_ptr<Owned, Free<Release>>;

auto sslError() -> std::string
{
    constexpr size_t maxText = 256;
    std::array<char, maxText> buf{};
    const auto code = ERR_get_error();
    ERR_clear_error();
    if (code == 0) {
        return "no detail";
    }
    ERR_error_string_n(code, buf.data(), buf.size());
    return buf.data();
}

void setFlag(int sock, int flag)
{
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg,bugprone-signed-bitwise): fcntl's interface.
    const int flags = fcntl(sock, F_GETFL);
    if (flags < 0 || fcntl(sock, F_SETFL, flags | flag) < 0) {
        throw nix::SysError("fcntl");
    }
    // NOLINTEND(cppcoreguidelines-pro-type-vararg,bugprone-signed-bitwise)
}

void setOpt(int sock, int level, int name, int value)
{
    if (setsockopt(sock, level, name, &value, sizeof value) != 0) {
        throw nix::SysError("setsockopt");
    }
}

enum class Ready : uint8_t {
    sock,
    stop,
    late,
};

// Waits for `events` on `sock` (none if negative), for `stop`, or until `deadline`.
auto wait(int sock, short events, const Wake & stop, std::optional<Clock::time_point> deadline) -> Ready
{
    for (;;) {
        int timeoutMs = -1;
        if (deadline) {
            const auto left = std::chrono::ceil<std::chrono::milliseconds>(*deadline - Clock::now()).count();
            if (left <= 0) {
                return Ready::late;
            }
            timeoutMs = static_cast<int>(left);
        }
        std::array<pollfd, 2> fds{{
            {.fd = sock, .events = events, .revents = 0},
            {.fd = stop.fd(), .events = POLLIN, .revents = 0},
        }};
        const int ready = poll(fds.data(), fds.size(), timeoutMs);
        if (ready < 0 && errno != EINTR) {
            throw nix::SysError("poll");
        }
        const auto & [onSock, onStop] = fds;
        if (ready > 0 && onStop.revents != 0) {
            return Ready::stop;
        }
        if (ready > 0 && onSock.revents != 0) {
            return Ready::sock;
        }
    }
}

// False when `stop` interrupted. Throws when `deadline` passes.
auto await(int sock, short events, const Wake & stop, std::optional<Clock::time_point> deadline) -> bool
{
    const auto ready = wait(sock, events, stop, deadline);
    if (ready == Ready::late) {
        throw nix::Error("timed out");
    }
    return ready == Ready::sock;
}

auto connectTo(const Target & target, const Wake & stop, Clock::time_point deadline) -> nix::AutoCloseFD
{
    sockaddr_storage addr{};
    socklen_t len = 0;
    if (auto * ipv4 = reinterpret_cast<sockaddr_in *>(&addr); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        inet_pton(AF_INET, target.address.c_str(), &ipv4->sin_addr) == 1) {
        ipv4->sin_family = AF_INET;
        ipv4->sin_port = htons(target.port);
        len = sizeof(sockaddr_in);
    } else if (auto * ipv6 = reinterpret_cast<sockaddr_in6 *>(&addr); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
               inet_pton(AF_INET6, target.address.c_str(), &ipv6->sin6_addr) == 1) {
        ipv6->sin6_family = AF_INET6;
        ipv6->sin6_port = htons(target.port);
        len = sizeof(sockaddr_in6);
    } else {
        throw nix::Error("'%s' is not an IP address", target.address);
    }
    nix::AutoCloseFD sock{socket(addr.ss_family, SOCK_STREAM, 0)};
    if (!sock) {
        throw nix::SysError("socket");
    }
    setFlag(sock.get(), O_NONBLOCK);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    if (fcntl(sock.get(), F_SETFD, FD_CLOEXEC) < 0) {
        throw nix::SysError("fcntl");
    }
#ifdef SO_NOSIGPIPE
    setOpt(sock.get(), SOL_SOCKET, SO_NOSIGPIPE, 1);
#endif
    // The only way to notice a balancer that vanished without closing.
    constexpr int idleSecs = 30;
    constexpr int probeSecs = 10;
    constexpr int probes = 3;
    setOpt(sock.get(), SOL_SOCKET, SO_KEEPALIVE, 1);
#ifdef __APPLE__
    setOpt(sock.get(), IPPROTO_TCP, TCP_KEEPALIVE, idleSecs);
#else
    setOpt(sock.get(), IPPROTO_TCP, TCP_KEEPIDLE, idleSecs);
#endif
    setOpt(sock.get(), IPPROTO_TCP, TCP_KEEPINTVL, probeSecs);
    setOpt(sock.get(), IPPROTO_TCP, TCP_KEEPCNT, probes);
    setOpt(sock.get(), IPPROTO_TCP, TCP_NODELAY, 1);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (connect(sock.get(), reinterpret_cast<sockaddr *>(&addr), len) != 0) {
        if (errno != EINPROGRESS) {
            throw nix::SysError("connect");
        }
        if (!await(sock.get(), POLLOUT, stop, deadline)) {
            return {};
        }
        int err = 0;
        socklen_t errLen = sizeof err;
        if (getsockopt(sock.get(), SOL_SOCKET, SO_ERROR, &err, &errLen) != 0 || err != 0) {
            errno = err;
            throw nix::SysError("connect");
        }
    }
    return sock;
}

struct Closed : nix::Error
{
    using nix::Error::Error;
};

enum class Io : uint8_t {
    done,
    stopped,
};

// Runs `step` (an SSL_* call) until it succeeds, waiting for the socket in between.
template<typename Step>
auto drive(SSL * ssl, int sock, const Wake & stop, Clock::time_point deadline, std::string_view what, Step step) -> Io
{
    for (;;) {
        ERR_clear_error();
        const int ret = step();
        if (ret > 0) {
            return Io::done;
        }
        short events = 0;
        switch (SSL_get_error(ssl, ret)) {
        case SSL_ERROR_WANT_READ:
            events = POLLIN;
            break;
        case SSL_ERROR_WANT_WRITE:
            events = POLLOUT;
            break;
        case SSL_ERROR_ZERO_RETURN:
            throw Closed("%s: the balancer closed the connection", what);
        default:
            if (const auto verify = SSL_get_verify_result(ssl); verify != X509_V_OK) {
                throw nix::Error("%s: certificate: %s", what, X509_verify_cert_error_string(verify));
            }
            // A reset shows as a system error, a plain close as this one.
            if (SSL_get_error(ssl, ret) == SSL_ERROR_SYSCALL || ERR_GET_REASON(ERR_peek_error()) == SSL_R_UNEXPECTED_EOF_WHILE_READING) {
                throw Closed("%s: the balancer closed the connection", what);
            }
            throw nix::Error("%s: %s", what, sslError());
        }
        if (!await(sock, events, stop, deadline)) {
            return Io::stopped;
        }
    }
}

auto handshake(int sock, const Target & target, const Tls & tls, const Identity & self, const Wake & stop, Clock::time_point deadline) -> Io
{
    const Owner<SSL_CTX, SSL_CTX_free> ctx{SSL_CTX_new(TLS_client_method())};
    if (!ctx) {
        throw nix::Error("TLS: %s", sslError());
    }
    if (SSL_CTX_set_min_proto_version(ctx.get(), TLS1_2_VERSION) != 1 || SSL_CTX_set_default_verify_paths(ctx.get()) != 1) {
        throw nix::Error("TLS: %s", sslError());
    }
    if (!tls.ca.empty() && SSL_CTX_load_verify_locations(ctx.get(), tls.ca.c_str(), nullptr) != 1) {
        throw nix::Error("reading '%s': %s", tls.ca, sslError());
    }
    if (SSL_CTX_use_certificate_chain_file(ctx.get(), tls.cert.c_str()) != 1) {
        throw nix::Error("reading '%s': %s", tls.cert, sslError());
    }
    if (SSL_CTX_use_PrivateKey_file(ctx.get(), tls.key.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw nix::Error("reading '%s': %s", tls.key, sslError());
    }
    SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_PEER, nullptr);

    const Owner<SSL, SSL_free> ssl{SSL_new(ctx.get())};
    if (!ssl || SSL_set_fd(ssl.get(), sock) != 1 || SSL_set1_host(ssl.get(), target.host.c_str()) != 1) {
        throw nix::Error("TLS: %s", sslError());
    }
    // The server name extension is for names only (RFC 6066).
    in6_addr scratch{};
    const bool isAddress =
        inet_pton(AF_INET, target.host.c_str(), &scratch) == 1 || inet_pton(AF_INET6, target.host.c_str(), &scratch) == 1;
    if (!isAddress && SSL_set_tlsext_host_name(ssl.get(), target.host.c_str()) != 1) {
        throw nix::Error("TLS: %s", sslError());
    }
    // On the wire: one length byte, then the name. validName() keeps it short enough.
    std::string alpn(1, static_cast<char>(target.protocol.size()));
    alpn += target.protocol;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (SSL_set_alpn_protos(ssl.get(), reinterpret_cast<const unsigned char *>(alpn.data()), static_cast<unsigned>(alpn.size())) != 0) {
        throw nix::Error("TLS: %s", sslError());
    }
    // The balancer goes on with the bare socket and never ends this session.
    SSL_set_quiet_shutdown(ssl.get(), 1);

    try {
        if (drive(ssl.get(), sock, stop, deadline, "TLS handshake", [&] -> int { return SSL_connect(ssl.get()); }) == Io::stopped) {
            return Io::stopped;
        }
    } catch (Closed &) {
        // What Envoy does on a port that has no listener for this ALPN name.
        throw nix::Error(
            "the balancer closed the connection during the TLS handshake; is %s its tunnel port?",
            std::format("{}:{}", target.address, target.port));
    }
    const auto req = request(self, target.host);
    // Without partial writes SSL_write succeeds only with all of it.
    if (drive(ssl.get(), sock, stop, deadline, "request", [&] -> int {
            return SSL_write(ssl.get(), req.data(), static_cast<int>(req.size()));
        }) == Io::stopped) {
        return Io::stopped;
    }
    constexpr size_t maxHead = 16384;
    std::string head;
    while (reply(head) == Reply::incomplete) {
        if (head.size() >= maxHead) {
            throw nix::Error("the balancer's answer is too long");
        }
        std::array<char, 1024> buf{};
        int got = 0;
        if (drive(ssl.get(), sock, stop, deadline, "answer", [&] -> int {
                got = SSL_read(ssl.get(), buf.data(), static_cast<int>(buf.size()));
                return got;
            }) == Io::stopped) {
            return Io::stopped;
        }
        head.append(buf.data(), static_cast<size_t>(got));
    }
    if (reply(head) == Reply::refused) {
        throw nix::Error("the balancer refused the tunnel: %s", head.substr(0, head.find('\r')));
    }
    return Io::done;
}

// The few bytes of a ping fit an idle socket's buffer, or the tunnel is broken.
void echo(int sock)
{
    int flags = 0;
#ifdef MSG_NOSIGNAL
    flags = MSG_NOSIGNAL;
#endif
    if (send(sock, ping.data(), ping.size(), flags) != static_cast<ssize_t>(ping.size())) {
        throw nix::Error("the tunnel does not take the answer to a ping");
    }
}

} // namespace

Wake::Wake()
{
    std::array<int, 2> fds{};
    if (pipe(fds.data()) != 0) {
        throw nix::SysError("pipe");
    }
    readSide = nix::AutoCloseFD{fds.front()};
    writeSide = nix::AutoCloseFD{fds.back()};
    for (const int end : fds) {
        setFlag(end, O_NONBLOCK);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
        if (fcntl(end, F_SETFD, FD_CLOEXEC) < 0) {
            throw nix::SysError("fcntl");
        }
    }
}

void Wake::set() const
{
    // A full pipe is as readable as one byte.
    (void) write(writeSide.get(), "x", 1);
}

void Wake::clear() const
{
    constexpr size_t chunk = 64;
    std::array<char, chunk> buf{};
    while (read(readSide.get(), buf.data(), buf.size()) > 0) {
    }
}

auto Wake::wait(std::optional<std::chrono::milliseconds> timeout) const -> bool
{
    return nixgrpc::tunnel::wait(-1, 0, *this, timeout ? std::optional{Clock::now() + *timeout} : std::nullopt) == Ready::stop;
}

auto open(const Target & target, const Tls & tls, const Identity & self, const Wake & stop) -> nix::AutoCloseFD
{
    if (!validName(self.node) || !validName(self.group) || !(validName(target.host) || validAddress6(target.host))
        || !validName(target.protocol)) {
        throw nix::Error("tunnel: invalid name");
    }
    const auto deadline = Clock::now() + openTimeout;
    auto sock = connectTo(target, stop, deadline);
    if (!sock || handshake(sock.get(), target, tls, self, stop, deadline) == Io::stopped) {
        return {};
    }
    // The start of a ping that came without its end.
    std::string pending;
    for (;;) {
        if (!await(sock.get(), POLLIN, stop, std::nullopt)) {
            return {};
        }
        std::array<char, ping.size()> head{};
        // What follows the pings stays in the socket for gRPC.
        const auto got = recv(sock.get(), head.data(), head.size() - pending.size(), pending.empty() ? MSG_PEEK : 0);
        if (got == 0) {
            throw nix::Error("the balancer closed the tunnel");
        }
        if (got < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                continue;
            }
            throw nix::SysError("reading from the tunnel");
        }
        const std::string_view bytes{head.data(), static_cast<size_t>(got)};
        if (pending.empty()) {
            if (front(bytes) == Front::data) {
                return sock;
            }
            (void) recv(sock.get(), head.data(), bytes.size(), 0);
        }
        pending += bytes;
        switch (front(pending)) {
        case Front::ping:
            echo(sock.get());
            pending.clear();
            break;
        case Front::partial:
            break;
        case Front::data:
            throw nix::Error("the balancer sent neither a ping nor a request");
        }
    }
}

#ifdef __APPLE__
// poll() on macOS misses a hang-up that happens while data is unread
// (FB17447257), kqueue does not.
auto waitClosed(int sock, const Wake & stop) -> End
{
    const nix::AutoCloseFD queue{kqueue()};
    if (!queue) {
        throw nix::SysError("kqueue");
    }
    std::array<struct kevent, 2> add{};
    EV_SET(&add.front(), sock, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    EV_SET(&add.back(), stop.fd(), EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    if (kevent(queue.get(), add.data(), static_cast<int>(add.size()), nullptr, 0, nullptr) < 0) {
        throw nix::SysError("kevent");
    }
    for (;;) {
        struct kevent event{};
        const int ready = kevent(queue.get(), nullptr, 0, &event, 1, nullptr);
        if (ready < 0 && errno != EINTR) {
            throw nix::SysError("kevent");
        }
        if (ready <= 0) {
            continue;
        }
        if (std::cmp_equal(event.ident, stop.fd())) {
            return End::stopped;
        }
        if ((event.flags & EV_EOF) != 0) {
            return End::closed;
        }
    }
}
#else
auto waitClosed(int sock, const Wake & stop) -> End
{
    return await(sock, POLLRDHUP | POLLHUP, stop, std::nullopt) ? End::closed : End::stopped;
}
#endif

auto certName(const std::string & file) -> std::string
{
    const Owner<FILE, std::fclose> pem{std::fopen(file.c_str(), "re")};
    if (!pem) {
        throw nix::SysError("opening '%s'", file);
    }
    const Owner<X509, X509_free> cert{PEM_read_X509(pem.get(), nullptr, nullptr, nullptr)};
    if (!cert) {
        throw nix::Error("reading '%s': %s", file, sslError());
    }
    // The balancer takes the name from the front of the subject as text,
    // which is the last entry; anywhere else it would not find it.
    const X509_NAME * subject = X509_get_subject_name(cert.get());
    const int last = X509_NAME_entry_count(subject) - 1;
    const X509_NAME_ENTRY * entry = last < 0 ? nullptr : X509_NAME_get_entry(subject, last);
    const bool isName = entry != nullptr && OBJ_obj2nid(X509_NAME_ENTRY_get_object(entry)) == NID_commonName;
    const bool alone = last < 1 || X509_NAME_ENTRY_set(entry) != X509_NAME_ENTRY_set(X509_NAME_get_entry(subject, last - 1));
    if (!isName || !alone) {
        throw nix::Error("the subject of the certificate '%s' must end in its common name, a builder behind a tunnel is known by it", file);
    }
    const ASN1_STRING * text = X509_NAME_ENTRY_get_data(entry);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    std::string found(reinterpret_cast<const char *>(ASN1_STRING_get0_data(text)), static_cast<size_t>(ASN1_STRING_length(text)));
    // "-" is what the balancer reads from a certificate without a name.
    if (!validName(found) || found == "-") {
        throw nix::Error("the common name of the certificate '%s' must consist of letters, digits and -._/", file);
    }
    return found;
}

} // namespace nixgrpc::tunnel
// NOLINTEND(misc-include-cleaner)
