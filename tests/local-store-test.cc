// What nix throws when its daemon cannot be reached must count as a lost
// daemon. The errors come from nix itself, talking to sockets that are
// missing, refuse, or hang up, so a nix that words them differently fails
// here.

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
#include <sys/stat.h> // NOLINT(misc-include-cleaner): which header has S_IRWXU differs per platform.
#include <unistd.h>

#include <nix/store/globals.hh>
#include <nix/store/path.hh>
#include <nix/util/error.hh>
#include <nix/util/serialise.hh>
#include <nix/util/terminal.hh>
#include <nix/util/unix-domain-socket.hh>

#include "client-stream.hh"
#include "local-store.hh"
#include "nix_remote.pb.h"
#include "pump.hh"
#include "upload-spool.hh"

namespace {

using nixgrpc::LocalStore;

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

// nix parses the NAR it stores; one that goes on after its declared size
// makes it read past what was spooled.
auto longNarIsLost() -> bool
{
    nixgrpc::NarSpool spool;
    std::string const declared = "nar";
    nix::StringSource upload(declared);
    auto reader = spool.reader(spool.add(upload, declared.size()));
    try {
        std::array<char, 2> bytes{};
        while (true) {
            reader.read(bytes.data(), bytes.size());
        }
    } catch (const std::exception & err) {
        return LocalStore::lost(err);
    }
}

auto run() -> int
{
    nix::initLibStore();
    // As the daemon does. NOLINTNEXTLINE(misc-include-cleaner): which header has SIGPIPE differs per platform.
    (void) ::signal(SIGPIPE, SIG_IGN);
    auto const dir = std::filesystem::temp_directory_path() / ("local-store-test-" + std::to_string(::getpid()));
    std::filesystem::create_directory(dir);

    // Bound and closed: the file is there, nobody listens.
    // NOLINTNEXTLINE(misc-include-cleaner): which header has S_IRWXU differs per platform.
    nix::createUnixDomainSocket(dir / "refused", S_IRWXU).close();
    // NOLINTNEXTLINE(misc-include-cleaner)
    auto listener = nix::createUnixDomainSocket(dir / "hangup", S_IRWXU);
    std::thread closer([hangup = listener.get()] -> void {
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

    if (longNarIsLost()) {
        std::println("FAIL a NAR longer than its declared size counts as a lost nix-daemon");
        status = EXIT_FAILURE;
    }

    // Linux wakes the accept on shutdown, macOS on close.
    ::shutdown(listener.get(), SHUT_RDWR);
    listener.close();
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
