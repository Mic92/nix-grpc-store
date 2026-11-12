// What nix throws when its daemon cannot be reached must count as a lost
// daemon. The errors come from nix itself, talking to sockets that are
// missing, refuse, or hang up, so a nix that words them differently fails
// here.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <print>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <signal.h> // NOLINT(modernize-deprecated-headers): SIGPIPE is POSIX, not in <csignal>.
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <nix/store/globals.hh>
#include <nix/store/path.hh>
#include <nix/util/error.hh>
#include <nix/util/terminal.hh>

#include "client-stream.hh"
#include "local-store.hh"
#include "nix_remote.pb.h"
#include "pump.hh"

namespace {

using nixgrpc::LocalStore;

auto listenAt(const std::filesystem::path & path) -> int
{
    int const sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    auto const name = path.string();
    std::ranges::copy(name, std::begin(addr.sun_path));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): the sockets API
    if (sock < 0 || ::bind(sock, reinterpret_cast<const sockaddr *>(&addr), sizeof addr) != 0 || ::listen(sock, SOMAXCONN) != 0) {
        std::println("cannot listen at {}", name);
        std::exit(EXIT_FAILURE); // NOLINT(concurrency-mt-unsafe)
    }
    return sock;
}

// The errors of two calls in a row: a store that failed to connect answers
// the second one differently.
auto errorsOf(const std::filesystem::path & socket) -> std::vector<std::string>
{
    std::vector<std::string> seen;
    LocalStore local("unix://" + socket.string());
    for (int call = 0; call < 2; call++) {
        try {
            local.shared()->isValidPath(nix::StorePath("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x"));
            seen.emplace_back("no error");
        } catch (const std::exception & err) {
            seen.push_back(LocalStore::lost(err) ? err.what() : std::string("not lost: ") + err.what());
        }
    }
    return seen;
}

// A client's stream that has ended, as the upload handler sees it.
struct EndedStream
{
    static auto Read(nix::remote::AddMultipleChunk * /*msg*/) -> bool { return false; }
};

auto endedUploadIsLost() -> bool
{
    EndedStream ended;
    nixgrpc::ClientStream stream(ended);
    nixgrpc::ZstdReaderSource<nixgrpc::ClientStream<EndedStream>, nix::remote::AddMultipleChunk> source(stream, "");
    try {
        std::array<char, 1> byte{};
        source.read(byte.data(), byte.size());
    } catch (const std::exception & err) {
        return LocalStore::lost(err);
    }
    return false;
}

auto run() -> int
{
    nix::initLibStore();
    // As the daemon does. NOLINTNEXTLINE(misc-include-cleaner): which header has SIGPIPE differs per platform.
    (void) ::signal(SIGPIPE, SIG_IGN);
    auto const dir = std::filesystem::temp_directory_path() / ("local-store-test-" + std::to_string(::getpid()));
    std::filesystem::create_directory(dir);

    // Bound and closed: the file is there, nobody listens.
    ::close(listenAt(dir / "refused"));
    int const hangup = listenAt(dir / "hangup");
    std::thread closer([hangup] -> void {
        for (;;) {
            int const conn = ::accept(hangup, nullptr, nullptr);
            if (conn < 0) {
                return;
            }
            ::close(conn);
        }
    });

    int status = EXIT_SUCCESS;
    std::set<std::string_view> used;
    for (const auto * name : {"missing", "refused", "hangup"}) {
        for (const auto & what : errorsOf(dir / name)) {
            if (what.starts_with("no") || what.starts_with("not lost")) {
                std::println("FAIL {}: {}", name, what);
                status = EXIT_FAILURE;
            }
            auto const plain = nix::filterANSIEscapes(what, true);
            for (auto const needle : LocalStore::cannotOpen) {
                if (plain.contains(needle)) {
                    used.insert(needle);
                }
            }
        }
    }
    for (auto const needle : LocalStore::cannotOpen) {
        if (!used.contains(needle)) {
            std::println("FAIL no error of nix says \"{}\"", needle);
            status = EXIT_FAILURE;
        }
    }
    if (LocalStore::lost(nix::SysError(ENOENT, "opening file '/nix/var/log/nix/drvs/x'"))) {
        std::println("FAIL a missing file counts as a lost nix-daemon");
        status = EXIT_FAILURE;
    }

    if (endedUploadIsLost()) {
        std::println("FAIL an upload that ends early counts as a lost nix-daemon");
        status = EXIT_FAILURE;
    }

    ::shutdown(hangup, SHUT_RDWR);
    ::close(hangup);
    closer.join();
    std::filesystem::remove_all(dir);
    return status;
}

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
