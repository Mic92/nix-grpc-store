// Applies a policy in a forked child and probes what it can still do.
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <stdlib.h> // NOLINT(modernize-deprecated-headers): mkdtemp, WIFEXITED are POSIX.

#include <nix/util/file-descriptor.hh>

#include "sandbox.hh"

namespace fs = std::filesystem;
using nixgrpc::sandbox::CanonicalPath;
using nixgrpc::sandbox::Policy;

namespace {

constexpr int skipped = 77;
// Ports nothing else listens on.
constexpr uint16_t allowedPort = 45871;
constexpr uint16_t deniedPort = 45872;

auto landlockAbi() -> int
{
    constexpr unsigned versionFlag = 1U;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): syscall(2) is vararg.
    return static_cast<int>(::syscall(SYS_landlock_create_ruleset, nullptr, 0, versionFlag));
}

auto canRead(const fs::path & path) -> bool { return std::ifstream(path).good(); }

auto canWrite(const fs::path & path) -> bool { return std::ofstream(path).good(); }

// sockaddr_in and friends are used through a sockaddr pointer by the C API.
template<typename Addr>
auto bindTo(const nix::AutoCloseFD & sock, const Addr & addr) -> bool
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): see above.
    return ::bind(sock.get(), reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == 0;
}

auto canListenUnix(const fs::path & path) -> bool
{
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    assert(path.native().size() < sizeof(addr.sun_path)); // no silent truncation
    std::ranges::copy(path.native(), static_cast<char *>(addr.sun_path));
    nix::AutoCloseFD const sock(::socket(AF_UNIX, SOCK_STREAM, 0));
    bool const bound = bindTo(sock, addr);
    ::unlink(path.c_str());
    return bound;
}

auto canBind6(uint16_t port) -> bool
{
    nix::AutoCloseFD const sock(::socket(AF_INET6, SOCK_STREAM, 0));
    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_loopback;
    addr.sin6_port = htons(port);
    return bindTo(sock, addr);
}

auto canBind(uint16_t port) -> bool
{
    nix::AutoCloseFD const sock(::socket(AF_INET, SOCK_STREAM, 0));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    return bindTo(sock, addr);
}

// Runs `body` in a child that is sandboxed with `policy`. Returns its exit
// code.
template<typename F>
auto sandboxed(const Policy & policy, F body) -> int
{
    auto const pid = ::fork();
    if (pid == 0) {
        try {
            nixgrpc::sandbox::apply(policy);
        } catch (const std::exception & err) {
            std::println(stderr, "sandbox: {}", err.what());
            ::_exit(skipped);
        }
        ::_exit(body() ? 0 : 1);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 2;
}

// A secret directory that is repointed at a new generation after the sandbox
// is up, like /run/secrets -> /run/secrets.d/N.
auto readsAcrossRotation(const fs::path & base) -> bool
{
    fs::create_directories(base / "gen1");
    fs::create_directories(base / "gen2");
    std::ofstream(base / "gen1" / "crt") << "1";
    std::ofstream(base / "gen2" / "crt") << "2";
    fs::create_directory_symlink("gen1", base / "current");

    Policy const policy;

    std::array<int, 2> ready{};
    std::array<int, 2> rotated{};
    assert(::pipe(ready.data()) == 0 && ::pipe(rotated.data()) == 0);
    auto const pid = ::fork();
    if (pid == 0) {
        try {
            nixgrpc::sandbox::apply(policy);
        } catch (const std::exception &) {
            ::_exit(skipped);
        }
        char byte = 0;
        bool const before = canRead(base / "current" / "crt");
        static_cast<void>(::write(ready.at(1), &byte, 1));
        static_cast<void>(::read(rotated.at(0), &byte, 1));
        ::_exit(before && canRead(base / "current" / "crt") ? 0 : 1);
    }
    char byte = 0;
    static_cast<void>(::read(ready.at(0), &byte, 1));
    fs::create_directory_symlink("gen2", base / "next");
    fs::rename(base / "next", base / "current");
    static_cast<void>(::write(rotated.at(1), &byte, 1));
    int status = 0;
    ::waitpid(pid, &status, 0);
    for (int const descriptor : {ready.at(0), ready.at(1), rotated.at(0), rotated.at(1)}) {
        ::close(descriptor);
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

} // namespace

auto main() -> int
try {
    // A bare file name has an empty parent directory.
    assert(fs::path("cert.pem").parent_path().empty());
    assert(CanonicalPath::from(fs::absolute(fs::path("cert.pem")).parent_path()).path() == fs::current_path());
    bool rejected = false;
    try {
        static_cast<void>(CanonicalPath::from(""));
    } catch (const std::invalid_argument &) {
        rejected = true;
    }
    assert(rejected);

    std::string tmpl = (fs::temp_directory_path() / "sandbox-XXXXXX").string();
    auto const base = fs::path(::mkdtemp(tmpl.data()));
    auto const readable = base / "ro";
    auto const writable = base / "rw";
    auto const hidden = base / "hidden";
    for (auto const & dir : {readable, writable, hidden}) {
        fs::create_directories(dir);
        std::ofstream(dir / "file") << "x";
    }

    Policy policy;
    policy.writePaths = {CanonicalPath::from(writable)};

    int const code = sandboxed(policy, [&] -> bool {
        // Reading works everywhere, writing only where the policy says.
        return canRead(readable / "file") && !canWrite(readable / "file") && canRead(writable / "file")
               && canWrite(writable / "new") && canRead(hidden / "file") && !canWrite(hidden / "new")
               && canRead("/etc/passwd") && canRead("/proc/self/status");
    });
    if (code == skipped) {
        std::puts("Landlock unavailable, skipping");
        fs::remove_all(base);
        return skipped;
    }
    assert(code == 0);
    assert(canWrite(hidden / "new")); // the parent is unaffected

    assert(readsAcrossRotation(base / "rotating"));

    // A unix listen socket may only be created where the policy says.
    Policy unixSock = policy;
    unixSock.listenSockets = {CanonicalPath::from(writable / "s.sock")};
    assert(
        sandboxed(
            unixSock, [&] -> bool { return canListenUnix(writable / "s.sock") && !canListenUnix(hidden / "s.sock"); })
        == 0);

    // Listening is limited to the allowed ports.
    Policy ports = policy;
    ports.listenPorts = std::vector<uint16_t>{allowedPort};
    int const bindCode = sandboxed(ports, [&] -> bool {
        // Port 0 stays allowed for gRPC's IPv6 probe.
        return !canBind(deniedPort) && !canBind6(deniedPort) && canBind(allowedPort) && canBind6(0);
    });
    // Network rules need ABI 4. Older kernels leave binding unrestricted.
    constexpr int netAbi = 4;
    if (landlockAbi() >= netAbi) {
        assert(bindCode == 0);
    } else {
        std::puts("kernel without Landlock network rules, bind restriction untested");
    }

    fs::remove_all(base);
    return 0;
} catch (const std::exception & err) {
    static_cast<void>(std::fputs(err.what(), stderr));
    return 1;
}
