// A builder's tunnel against a stand-in for the balancer: what it sends to
// open one, when it hands the connection over, and how it learns that the
// connection ended.

#include <array>
#include <chrono> // NOLINT(misc-include-cleaner): the literals.
#include <cstddef>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <format>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <print>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h> // NOLINT(misc-include-cleaner): timeval.
#include <unistd.h>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/tls1.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>

#include "tunnel-protocol.hh"
#include "tunnel.hh"
#include "tunnels.hh"

// NOLINTBEGIN(misc-include-cleaner): which header declares a socket or OpenSSL name differs per platform and version.
// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers): how long the stand-in balancer waits.
namespace {

using namespace std::chrono_literals;
using nixgrpc::tunnel::End;
using nixgrpc::tunnel::Wake;
using nixgrpc::tunnel::ping;

int failures = 0; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void check(bool cond, std::string_view what)
{
    if (!cond) {
        std::println(stderr, "FAIL: {}", what);
        failures++;
    }
}

struct Cert
{
    std::shared_ptr<X509> cert;
    std::shared_ptr<EVP_PKEY> key;
};

using Subject = std::vector<std::pair<std::string, std::string>>;

// A certificate for `names` ("DNS:a,DNS:b"), signed by `issuer` or by itself.
auto makeCert(const std::string & names, const Cert * issuer, const Subject & entries = {{"CN", "test"}}, int encoding = MBSTRING_ASC)
    -> Cert
{
    constexpr long hour = 3600;
    constexpr long year = 365L * 24 * hour;
    static long serial = 1;
    EVP_PKEY * key = EVP_EC_gen("P-256");
    X509 * cert = X509_new();
    X509_set_version(cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert), serial++);
    X509_gmtime_adj(X509_getm_notBefore(cert), -hour);
    X509_gmtime_adj(X509_getm_notAfter(cert), year);
    X509_set_pubkey(cert, key);
    X509_NAME * subject = X509_get_subject_name(cert);
    for (const auto & [field, value] : entries) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        const auto * bytes = reinterpret_cast<const unsigned char *>(value.data());
        X509_NAME_add_entry_by_txt(subject, field.c_str(), encoding, bytes, static_cast<int>(value.size()), -1, 0);
    }
    X509_set_issuer_name(cert, issuer == nullptr ? subject : X509_get_subject_name(issuer->cert.get()));
    X509V3_CTX ctx;
    X509V3_set_ctx(&ctx, issuer == nullptr ? cert : issuer->cert.get(), cert, nullptr, nullptr, 0);
    const auto add = [&](int nid, const std::string & value) -> void {
        X509_EXTENSION * ext = X509V3_EXT_conf_nid(nullptr, &ctx, nid, value.c_str());
        X509_add_ext(cert, ext, -1);
        X509_EXTENSION_free(ext);
    };
    if (issuer == nullptr) {
        add(NID_basic_constraints, "critical,CA:TRUE");
        add(NID_key_usage, "critical,keyCertSign,cRLSign");
    } else {
        add(NID_subject_alt_name, names);
    }
    X509_sign(cert, issuer == nullptr ? key : issuer->key.get(), EVP_sha256());
    return {.cert = {cert, X509_free}, .key = {key, EVP_PKEY_free}};
}

void save(const Cert & cert, const std::filesystem::path & base)
{
    BIO * out = BIO_new_file((base.string() + ".pem").c_str(), "w");
    PEM_write_bio_X509(out, cert.cert.get());
    BIO_free(out);
    out = BIO_new_file((base.string() + ".key").c_str(), "w");
    PEM_write_bio_PrivateKey(out, cert.key.get(), nullptr, nullptr, 0, nullptr, nullptr);
    BIO_free(out);
}

struct Listener
{
    nix::AutoCloseFD sock;
    uint16_t port = 0;
    explicit Listener(int family = AF_INET)
        : sock(socket(family, SOCK_STREAM, 0))
    {
        // Port and family sit at the same place in both address types.
        sockaddr_in6 addr{};
        addr.sin6_family = static_cast<sa_family_t>(family);
        unsigned len = sizeof(sockaddr_in);
        // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
        if (family == AF_INET6) {
            addr.sin6_addr = in6addr_loopback;
            len = sizeof addr;
        } else {
            inet_pton(AF_INET, "127.0.0.1", &reinterpret_cast<sockaddr_in *>(&addr)->sin_addr);
        }
        if (!sock || bind(sock.get(), reinterpret_cast<sockaddr *>(&addr), len) != 0 || listen(sock.get(), 4) != 0
            || getsockname(sock.get(), reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
            throw nix::SysError("listen");
        }
        // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
        port = ntohs(addr.sin6_port);
    }
};

struct Pki
{
    std::filesystem::path dir;
    Cert authority = makeCert("", nullptr, {{"CN", "test ca"}});
    Pki()
    {
        std::string tmpl = (std::filesystem::temp_directory_path() / "tunnel-test-XXXXXX").string();
        dir = mkdtemp(tmpl.data()); // NOLINT(misc-include-cleaner): <stdlib.h> on Linux, <unistd.h> on macOS.
        save(authority, dir / "ca");
        save(makeCert("DNS:lb.test", &authority), dir / "lb");
        save(makeCert("IP:::1", &authority), dir / "lb6");
        save(makeCert("DNS:other.test", &authority), dir / "other");
        save(makeCert("DNS:b1", &authority), dir / "b1");
        save(makeCert("DNS:a.test,DNS:b.test", &authority, {{"O", "farm"}, {"CN", "b1"}}), dir / "named");
        save(makeCert("DNS:b1", &authority, {{"CN", "b1"}, {"O", "farm"}}), dir / "name-first");
        save(makeCert("DNS:b1", &authority, {{"O", "farm"}}), dir / "nameless");
        save(makeCert("DNS:b1", &authority, {{"CN", "-"}}), dir / "dash");
        save(makeCert("DNS:b1", &authority, {{"CN", "b1/pod"}}), dir / "slash");
        // Two bytes per character.
        save(makeCert("DNS:b1", &authority, {{"CN", std::string("\0b\0" "1", 4)}}, V_ASN1_BMPSTRING), dir / "wide");
    }
    ~Pki()
    {
        std::filesystem::remove_all(dir);
    }
    Pki(const Pki &) = delete;
    auto operator=(const Pki &) -> Pki & = delete;
    Pki(Pki &&) = delete;
    auto operator=(Pki &&) -> Pki & = delete;
    [[nodiscard]] auto file(std::string_view name) const -> std::string
    {
        return (dir / name).string();
    }
};

struct Seen
{
    std::string request;
    std::string serverName;
    std::string protocol;
    std::string echoes;
    bool clientCert = false;
};

// The balancer's side of opening a tunnel on `conn`: TLS as `cert`, then `status`.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto greet(int conn, const Pki & pki, const std::string & cert, const std::string & status) -> Seen
{
        Seen seen;
        // A builder that stays silent fails the test instead of hanging it.
        const timeval patience{.tv_sec = 2, .tv_usec = 0};
        setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
        SSL_CTX * ctx = SSL_CTX_new(TLS_server_method());
        SSL_CTX_use_certificate_chain_file(ctx, pki.file(cert + ".pem").c_str());
        SSL_CTX_use_PrivateKey_file(ctx, pki.file(cert + ".key").c_str(), SSL_FILETYPE_PEM);
        SSL_CTX_load_verify_locations(ctx, pki.file("ca.pem").c_str(), nullptr);
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
        SSL_CTX_set_alpn_select_cb(
            ctx,
            [](SSL *, const unsigned char ** out, unsigned char * outLen, const unsigned char * offered, unsigned len, void * arg) -> int {
                // The client's list: one length byte, then the name.
                if (len > 1) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    static_cast<Seen *>(arg)->protocol.assign(reinterpret_cast<const char *>(offered) + 1, *offered);
                    *out = offered + 1; // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    *outLen = *offered;
                }
                return SSL_TLSEXT_ERR_OK;
            },
            &seen);
        SSL * ssl = SSL_new(ctx);
        SSL_set_fd(ssl, conn);
        if (SSL_accept(ssl) == 1) {
            X509 * peer = SSL_get1_peer_certificate(ssl);
            seen.clientCert = peer != nullptr;
            X509_free(peer);
            if (const char * name = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name)) {
                seen.serverName = name;
            }
            constexpr size_t chunk = 1024;
            std::array<char, chunk> buf{};
            while (!seen.request.contains("\r\n\r\n")) {
                const int got = SSL_read(ssl, buf.data(), static_cast<int>(buf.size()));
                if (got <= 0) {
                    break;
                }
                seen.request.append(buf.data(), static_cast<size_t>(got));
            }
            const std::string answer = "HTTP/1.1 " + status + "\r\ncontent-length: 0\r\n\r\n";
            SSL_write(ssl, answer.data(), static_cast<int>(answer.size()));
        }
        SSL_set_quiet_shutdown(ssl, 1);
        SSL_free(ssl);
        SSL_CTX_free(ctx);
        return seen;
}

// One tunnel: after `status` the balancer goes on in the clear with `after`.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto balancer(const Listener & lis, const Pki & pki, const std::string & cert, std::string status, std::function<void(int, Seen &)> after)
    -> std::future<Seen>
{
    // NOLINTNEXTLINE(bugprone-exception-escape): a throw reaches the test through the future.
    return std::async(std::launch::async, [&lis, &pki, cert, status = std::move(status), after = std::move(after)] -> Seen {
        const nix::AutoCloseFD conn{accept(lis.sock.get(), nullptr, nullptr)};
        Seen seen = greet(conn.get(), pki, cert, status);
        if (after) {
            after(conn.get(), seen);
        }
        return seen;
    });
}

void pingOnce(int conn, Seen & seen)
{
    std::array<char, ping.size()> echo{};
    (void) send(conn, ping.data(), ping.size(), 0);
    const auto got = recv(conn, echo.data(), echo.size(), MSG_WAITALL);
    seen.echoes.append(echo.data(), got > 0 ? static_cast<size_t>(got) : 0);
}

struct Fixture
{
    int family = AF_INET;
    Pki pki;
    Listener lis{family};
    Wake stop;
    nixgrpc::tunnel::Tls tls{.cert = pki.file("b1.pem"), .key = pki.file("b1.key"), .ca = pki.file("ca.pem")};
    nixgrpc::tunnel::Target target{.host = "lb.test", .address = "127.0.0.1", .port = lis.port, .protocol = std::string(nixgrpc::tunnel::anyProtocol)};
    nixgrpc::tunnel::Identity self{.node = "any/b1", .group = "any", .instance = "", .name = "b1"};

    [[nodiscard]] auto open() const -> nix::AutoCloseFD
    {
        return nixgrpc::tunnel::open(target, tls, self, stop);
    }
    [[nodiscard]] auto fails() const -> std::string
    {
        try {
            (void) open();
        } catch (nix::Error & err) {
            return err.info().msg.str();
        }
        return "";
    }
};

void a_tunnel_is_handed_over_when_the_balancer_starts_a_request()
{
    const Fixture fix;
    constexpr std::string_view hello = "\x16\x03\x01 client hello";
    auto peer = balancer(fix.lis, fix.pki, "lb", "200 OK", [&](int conn, Seen & seen) -> void {
        pingOnce(conn, seen);
        // A ping arriving in two pieces is still a ping.
        (void) send(conn, "RP", 2, 0);
        std::this_thread::sleep_for(50ms);
        (void) send(conn, "ING", 3, 0);
        std::array<char, ping.size()> echo{};
        (void) recv(conn, echo.data(), echo.size(), MSG_WAITALL);
        seen.echoes.append(echo.data(), echo.size());
        (void) send(conn, hello.data(), hello.size(), 0);
        std::array<char, 1> wait{};
        (void) recv(conn, wait.data(), 1, 0);
    });
    auto sock = fix.open();
    check(static_cast<bool>(sock), "open() returns the socket");
    std::array<char, hello.size() + 1> buf{};
    const auto got = sock ? recv(sock.get(), buf.data(), buf.size(), 0) : 0;
    check(got > 0 && std::string_view(buf.data(), static_cast<size_t>(got)) == hello, "the request's first bytes are left unread");
    sock.close();
    const auto seen = peer.get();
    check(seen.clientCert, "the builder presents its certificate");
    check(seen.serverName == "lb.test", "the TLS server name is the balancer's");
    check(seen.protocol == "nix-tunnel-any", "the ALPN name tells which set of tunnels to join");
    check(seen.request.contains("\r\nx-envoy-reverse-tunnel-node-id: any/b1\r\n"), "node id");
    check(seen.request.contains("\r\nx-envoy-reverse-tunnel-cluster-id: any\r\n"), "group");
    check(seen.echoes == "RPINGRPING", "pings are echoed");
}

void a_balancer_that_goes_away_inside_a_ping_is_noticed()
{
    const Fixture fix;
    auto peer = balancer(fix.lis, fix.pki, "lb", "200 OK", [](int conn, Seen &) -> void { (void) send(conn, "RPI", 3, 0); });
    auto opening = std::async(std::launch::async, [&] -> std::string { return fix.fails(); });
    const bool noticed = opening.wait_for(3s) == std::future_status::ready;
    fix.stop.set();
    check(noticed, "open() returns");
    check(opening.get().contains("closed the tunnel"), "the closed tunnel is reported");
    (void) peer.get();
}

void a_refused_tunnel_reports_the_balancers_answer()
{
    const Fixture fix;
    auto peer = balancer(fix.lis, fix.pki, "lb", "403 Forbidden", nullptr);
    const auto msg = fix.fails();
    check(msg.contains("403 Forbidden"), "refusal carries the status: " + msg);
    (void) peer.get();
}

void a_balancer_with_another_name_is_not_trusted()
{
    const Fixture fix;
    auto peer = balancer(fix.lis, fix.pki, "other", "200 OK", nullptr);
    const auto msg = fix.fails();
    check(msg.contains("certificate"), "wrong certificate is named as the cause: " + msg);
    (void) peer.get();
}

void a_port_that_does_not_take_tunnels_is_named_as_the_likely_cause()
{
    const Fixture fix;
    // Envoy without a listener for the tunnel's ALPN name hangs up like this.
    auto peer = std::async(std::launch::async, [&] -> void { const nix::AutoCloseFD conn{accept(fix.lis.sock.get(), nullptr, nullptr)}; });
    const auto msg = fix.fails();
    check(msg.contains("tunnel port"), "the error asks about the port: " + msg);
    peer.get();
}

void a_balancer_is_reached_by_its_ipv6_address()
{
    try {
        const Listener probe(AF_INET6);
    } catch (nix::SysError &) {
        std::println(stderr, "skipped: no IPv6 loopback here");
        return;
    }
    Fixture fix{.family = AF_INET6};
    const auto parsed = nixgrpc::tunnel::parseBalancer(std::format("[::1]:{}", fix.lis.port));
    fix.target.host = parsed.host;
    fix.target.address = parsed.host;
    fix.target.port = parsed.port;
    auto peer = balancer(fix.lis, fix.pki, "lb6", "200 OK", [](int conn, Seen &) -> void {
        (void) send(conn, "\x16", 1, 0);
        std::array<char, 1> wait{};
        (void) recv(conn, wait.data(), 1, 0);
    });
    check(static_cast<bool>(fix.open()), "the tunnel opens");
    const auto seen = peer.get();
    check(seen.request.contains("\r\nhost: [::1]\r\n"), "the address is in brackets in the host header: " + seen.request);
    check(seen.serverName.empty(), "an address is not sent as TLS server name: " + seen.serverName);
    for (const auto * bad : {"::1:9000", "[::1]", "[lb.test]:9000"}) {
        try {
            (void) nixgrpc::tunnel::parseBalancer(bad);
            check(false, std::string("accepted ") + bad);
        } catch (nix::Error &) { // NOLINT(bugprone-empty-catch): refusing is what is checked.
        }
    }
}

struct Pair
{
    nix::AutoCloseFD ours;
    nix::AutoCloseFD peer;
    Pair()
    {
        const Listener lis;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(lis.port);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        peer = nix::AutoCloseFD{socket(AF_INET, SOCK_STREAM, 0)};
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        if (connect(peer.get(), reinterpret_cast<sockaddr *>(&addr), sizeof addr) != 0) {
            throw nix::SysError("connect");
        }
        ours = nix::AutoCloseFD{accept(lis.sock.get(), nullptr, nullptr)};
    }
};

constexpr std::string_view unread = "unread";

// What the watcher of a connection reports when `act` happens while it waits.
auto watching(const std::function<void(Pair &, const Wake &)> & act) -> std::optional<End>
{
    Pair pair;
    const Wake stop;
    const nix::AutoCloseFD watch{dup(pair.ours.get())};
    auto result = std::async(std::launch::async, [&] -> End { return nixgrpc::tunnel::waitClosed(watch.get(), stop); });
    std::this_thread::sleep_for(100ms);
    act(pair, stop);
    if (result.wait_for(2s) != std::future_status::ready) {
        stop.set();
        (void) result.get();
        return std::nullopt;
    }
    return result.get();
}

void the_watcher_sees_every_way_a_connection_ends()
{
    check(watching([](Pair & pair, const Wake &) -> void { pair.peer.close(); }) == End::closed, "the peer closes");
    check(
        watching([](Pair & pair, const Wake &) -> void {
            shutdown(pair.ours.get(), SHUT_RDWR);
            pair.ours.close();
        }) == End::closed,
        "the owner shuts the socket down and closes its descriptor");
    // Where macOS's poll() loses the hang-up.
    check(
        watching([](Pair & pair, const Wake &) -> void {
            (void) send(pair.peer.get(), unread.data(), unread.size(), 0);
            std::this_thread::sleep_for(50ms);
            shutdown(pair.ours.get(), SHUT_RDWR);
            pair.ours.close();
        }) == End::closed,
        "the owner shuts down with data unread");
    check(
        watching([](Pair & pair, const Wake & stop) -> void {
            (void) send(pair.peer.get(), unread.data(), unread.size(), 0);
            std::this_thread::sleep_for(200ms);
            stop.set();
        }) == End::stopped,
        "data arriving is not an end");
}

struct Tunnel
{
    nix::AutoCloseFD conn;
    std::string protocol;
    std::string request;
    // The builder's end is gone, as opposed to merely silent.
    [[nodiscard]] auto ended() const -> bool
    {
        std::array<char, 1> byte{};
        // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): no lock is held.
        const auto got = recv(conn.get(), byte.data(), byte.size(), 0);
        return got == 0 || (got < 0 && errno == ECONNRESET);
    }
};

// A builder holding its tunnels to one balancer, one per set.
struct Held
{
    Pki pki;
    Listener lis;
    std::mutex mutex;
    std::vector<nix::AutoCloseFD> adopted;
    nixgrpc::tunnel::Tunnels tunnels{{
        .balancers = {{.host = "lb.test", .port = lis.port}},
        .tls = {.cert = pki.file("b1.pem"), .key = pki.file("b1.key"), .ca = pki.file("ca.pem")},
        .name = "b1",
        .instance = "pod-7",
        .perSet = 1,
        .adopt = [this](nix::AutoCloseFD sock) -> void {
            const std::scoped_lock lock(mutex);
            adopted.push_back(std::move(sock));
        },
        .resolve = [](const std::string &) -> std::vector<std::string> { return {"127.0.0.1"}; },
    }};

    auto taken() -> size_t
    {
        const std::scoped_lock lock(mutex);
        return adopted.size();
    }

    // The next tunnel the builder opens, with a request started on it.
    auto next(std::chrono::milliseconds patience = 3s) -> std::optional<Tunnel>
    {
        pollfd pfd{.fd = lis.sock.get(), .events = POLLIN, .revents = 0};
        if (poll(&pfd, 1, static_cast<int>(patience.count())) <= 0) {
            return std::nullopt;
        }
        Tunnel tunnel{.conn = nix::AutoCloseFD{accept(lis.sock.get(), nullptr, nullptr)}, .protocol = "", .request = ""};
        auto seen = greet(tunnel.conn.get(), pki, "lb", "200 OK");
        tunnel.protocol = std::move(seen.protocol);
        tunnel.request = std::move(seen.request);
        const auto before = taken();
        (void) send(tunnel.conn.get(), unread.data(), unread.size(), 0);
        constexpr int limit = 300;
        for (int tries = 0; taken() == before && tries < limit; tries++) {
            std::this_thread::sleep_for(10ms);
        }
        check(taken() > before, "the builder takes the tunnel into use");
        return tunnel;
    }

    // The first two tunnels: for this builder, and for any.
    auto both() -> std::pair<Tunnel, Tunnel>
    {
        auto first = next();
        auto second = next();
        if (!first || !second) {
            throw nix::Error("the builder opened no tunnels");
        }
        if (first->protocol == nixgrpc::tunnel::anyProtocol) {
            std::swap(first, second);
        }
        check(first->protocol == "nix-tunnel" && second->protocol == "nix-tunnel-any", "one tunnel per set");
        return {std::move(*first), std::move(*second)};
    }
};

// Envoy checks the first half against the certificate and the second
// against the header, so pods sharing a certificate get a tunnel each.
void builders_sharing_a_certificate_are_told_apart_by_instance()
{
    Held held;
    const auto [named, any] = held.both();
    check(named.request.contains("\r\nx-envoy-reverse-tunnel-node-id: b1/pod-7\r\n"), "own tunnels: " + named.request);
    check(named.request.contains("\r\nx-envoy-reverse-tunnel-cluster-id: b1/pod-7\r\n"), "a certificate's name alone finds nobody");
    check(any.request.contains("\r\nx-envoy-reverse-tunnel-node-id: any/b1/pod-7\r\n"), "shared tunnels: " + any.request);
    check(named.request.contains("\r\nx-nix-instance: pod-7\r\n") && any.request.contains("\r\nx-nix-instance: pod-7\r\n"), "instance header");
}

void a_tunnel_that_ended_is_opened_again()
{
    Held held;
    auto [named, any] = held.both();
    named.conn.close();
    const auto again = held.next();
    check(again && again->protocol == "nix-tunnel", "the same kind of tunnel comes back");
}

void a_builder_that_takes_no_work_only_keeps_the_tunnels_for_itself()
{
    Held held;
    const auto [named, any] = held.both();
    held.tunnels.setTakesWork(false);
    check(any.ended(), "the tunnel for any builder is cut although it was in use");
    check(!held.next(700ms), "and not opened again");
    std::array<char, 1> byte{};
    check(
        // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): no lock is held.
        recv(named.conn.get(), byte.data(), byte.size(), MSG_DONTWAIT) < 0 && errno == EAGAIN,
        "the tunnel for this builder stays");
    held.tunnels.setTakesWork(true);
    const auto back = held.next();
    check(back && back->protocol == "nix-tunnel-any", "taking work again opens it again");
}

void a_tunnelled_builder_is_known_by_its_certificates_common_name()
{
    using nixgrpc::tunnel::certName;
    const Pki pki;
    check(certName(pki.file("named.pem")) == "b1", "the common name is the builder's, whatever else the certificate holds");
    check(certName(pki.file("wide.pem")) == "b1", "also when the certificate stores it two bytes per character");
    // The balancer reads the name from the end of the subject, and "-" where there is none.
    // With a slash it could be taken for another builder's name and pod.
    for (const auto * file : {"name-first.pem", "nameless.pem", "dash.pem", "slash.pem"}) {
        try {
            (void) certName(pki.file(file));
            check(false, std::string(file) + " is refused");
        } catch (nix::Error & err) {
            check(err.info().msg.str().contains("common name"), "the error says what is needed");
        }
    }
}

} // namespace

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
// NOLINTEND(misc-include-cleaner)

auto main() -> int
try {
    // As in the daemon: a write to a closed connection is an error, not the end of the process.
    (void) std::signal(SIGPIPE, SIG_IGN); // NOLINT(misc-include-cleaner)
    try {
        a_tunnel_is_handed_over_when_the_balancer_starts_a_request();
        a_balancer_that_goes_away_inside_a_ping_is_noticed();
    a_refused_tunnel_reports_the_balancers_answer();
        a_balancer_with_another_name_is_not_trusted();
        a_port_that_does_not_take_tunnels_is_named_as_the_likely_cause();
        a_balancer_is_reached_by_its_ipv6_address();
        the_watcher_sees_every_way_a_connection_ends();
        a_tunnelled_builder_is_known_by_its_certificates_common_name();
        builders_sharing_a_certificate_are_told_apart_by_instance();
        a_tunnel_that_ended_is_opened_again();
        a_builder_that_takes_no_work_only_keeps_the_tunnels_for_itself();
    } catch (std::exception & err) {
        std::println(stderr, "FAIL: {}", err.what());
        return 1;
    }
    return failures == 0 ? 0 : 1;
} catch (...) {
    return 1;
}
