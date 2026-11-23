// `nix-grpc-daemon --stdio`: serves one Nix worker-protocol client on
// stdin/stdout from a grpc:// store in this process, like `nix-daemon
// --stdio --store grpc://…` would with the plugin. Run as an sshd
// ForceCommand it makes the farm an ordinary ssh-ng:// builder.
//
// Nix's own daemon code enforces the client's trust: an untrusted client
// may only import signed or content-addressed paths, and may only build
// derivations whose closure it uploaded. The farm sees the gateway's own
// identity, which must have the trusted role so the inline derivations of
// the build hook are accepted.

#include "stdio.hh"

#include <csignal>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <variant>

#include <signal.h> // NOLINT(modernize-deprecated-headers): sigaction is POSIX, not in <csignal>
#include <unistd.h>

#include <nix/store/daemon.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-reference.hh>
#include <nix/util/error.hh>
#include <nix/util/ref.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signals.hh>
#include <nix/util/signals-impl.hh>
#include <nix/util/url.hh>

#include "store.hh"

// Named in Nix 2.35. Before, ReceiveInterrupts sent SIGUSR1 everywhere.
#ifndef NIX_SIG_MULTI_INT
#define NIX_SIG_MULTI_INT SIGUSR1
#endif

namespace nixgrpc {

namespace {

struct StdioOptions
{
    std::string farm;
    bool trusted = false;
};

auto parseStdioOptions(std::span<char *> args) -> StdioOptions
{
    StdioOptions options;
    for (size_t idx = 1; idx < args.size(); ++idx) {
        std::string_view const arg = args.subspan(idx).front();
        if (arg == "--stdio") {
            continue;
        }
        if (arg == "--farm") {
            if (++idx >= args.size()) {
                throw nix::Error("--farm requires an argument");
            }
            options.farm = args.subspan(idx).front();
        } else if (arg == "--trusted") {
            options.trusted = true;
        } else {
            throw nix::Error("--stdio: unknown flag '%s' (only --farm and --trusted)", arg);
        }
    }
    if (options.farm.empty()) {
        throw nix::Error("--stdio requires --farm grpc://host:port?…");
    }
    return options;
}

auto openFarm(const std::string & uri) -> nix::ref<nix::Store>
{
    auto ref = nix::StoreReference::parse(uri);
    auto const * specified = std::get_if<nix::StoreReference::Specified>(&ref.variant);
    if (specified == nullptr || specified->scheme != "grpc") {
        throw nix::Error("--farm '%s' is not a grpc:// URL", uri);
    }
    // No local store to read derivations from: the client uploads their
    // closure, or (the build hook, trusted only) sends them inline.
    ref.params.try_emplace("local-eval-store", "false");
    ref.params.try_emplace("inline-drvs", "true");
    auto config = nix::make_ref<nix::GrpcStoreConfig>(nix::ParsedURL::Authority::parse(specified->authority), ref.params);
    return config->openStore();
}

} // namespace

auto runStdio(std::span<char *> args) -> int
{
    auto const options = parseStdioOptions(args);

    // Nix's daemon loop interrupts itself with this signal when the client
    // hangs up. Without libmain nobody installed a handler, and the default
    // action would kill us.
    // NOLINTBEGIN(misc-include-cleaner): darwin's <signal.h> forwards these.
    struct sigaction act{};
    act.sa_handler = [](int) -> void {};
    sigaction(NIX_SIG_MULTI_INT, &act, nullptr);
    act.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &act, nullptr);
    // NOLINTEND(misc-include-cleaner)
    // SIGINT/SIGTERM/SIGHUP (sshd on disconnect) become nix interrupts, which
    // cancel in-flight gRPC calls. Before the store spawns its threads.
    nix::unix::startSignalHandlerThread();

    auto store = openFarm(options.farm);
    nix::daemon::processConnection(
        store,
        nix::FdSource(STDIN_FILENO),
        nix::FdSink(STDOUT_FILENO),
        options.trusted ? nix::Trusted : nix::NotTrusted,
        nix::daemon::RecursiveFlag::NotRecursive);
    return 0;
}

} // namespace nixgrpc
