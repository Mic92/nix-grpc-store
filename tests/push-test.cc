#include <cassert>
#include <chrono>
#include <string>

#include <signal.h> // NOLINT(modernize-deprecated-headers): SIGPIPE is POSIX, not in <csignal>.

#include "push.hh"

namespace {

// A build log must not wait for a child that does not read its stdin.
void aLogDoesNotWaitForTheChild()
{
    constexpr int logs = 5000; // more than a pipe holds
    constexpr std::chrono::seconds prompt{1};
    nixgrpc::PushProcess push({"sleep", "2"});
    auto const before = std::chrono::steady_clock::now();
    for (int i = 0; i < logs; i++) {
        push.pushLog("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-" + std::to_string(i) + ".drv");
    }
    assert(std::chrono::steady_clock::now() - before < prompt);
}

} // namespace

// Nix's startProcess() never frees the logger it makes for the child.
extern "C" auto __lsan_default_suppressions() -> const char * // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
{
    return "leak:libnixutil";
}

auto main() -> int
try {
    // As in the daemon: a child that is gone fails the write.
    (void) ::signal(SIGPIPE, SIG_IGN); // NOLINT(misc-include-cleaner): its header differs per platform
    aLogDoesNotWaitForTheChild();
    return 0;
} catch (...) {
    return 1;
}
