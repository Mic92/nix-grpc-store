#include <cassert>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include "sandbox.hh"
#include "sbpl.hh"

using nixgrpc::sandbox::AnyPort;
using nixgrpc::sandbox::CanonicalPath;
using nixgrpc::sandbox::Policy;
namespace sbpl = nixgrpc::sandbox::sbpl;

constexpr uint16_t listenPort = 50051;

auto main() -> int
try {
    Policy policy;
    policy.writePaths = {CanonicalPath::from("/var/tmp/x\") (allow default) (\"")};
    policy.connectSockets = {CanonicalPath::from("/nix/var/nix/daemon-socket/socket")};
    policy.execPaths = {CanonicalPath::from("/nix/store")};
    policy.listenPorts = std::vector<uint16_t>{listenPort};

    auto const text = sbpl::profile(policy);
    assert(text.starts_with("(version 1)\n(deny default)\n"));
    assert(text.contains("(allow file-read*)\n"));
    assert(text.contains("(allow file-read* file-write* (subpath (param \"W0\")))"));
    assert(text.contains(
        "(allow network-outbound (remote unix-socket "
        "(path-literal (param \"S0\"))))"));
    assert(text.contains("(allow process-exec (subpath (param \"X0\")))"));
    assert(text.contains("(local tcp \"*:50051\")"));
    // Seatbelt rejects port 0 in an address, so gRPC's IPv6 probe stays denied.
    assert(!text.contains("(local tcp \"*:0\")"));
    assert(text.contains("(path-literal \"/private/var/run/mDNSResponder\")"));
    assert(!text.contains("*:*\"))\n(allow network-bind"));

    // Paths travel as parameters and never reach the profile text.
    assert(!text.contains("/var/tmp/x"));
    assert(!text.contains("allow default"));

    auto const params = sbpl::parameters(policy);
    assert(params.size() == 6);
    assert(params.at(0) == "W0" && params.at(1) == policy.writePaths.at(0).string());

    Policy sock;
    sock.listenSockets = {CanonicalPath::from("/run/nix-grpc/s.sock")};
    auto const sockText = sbpl::profile(sock);
    assert(sockText.contains(
        "(allow network-bind network-inbound (local "
        "unix-socket (path-literal (param \"L0\"))))"));
    assert(sockText.contains("(allow file-write-create file-write-unlink (literal (param \"L0\")))"));
    assert(sbpl::parameters(sock) == (std::vector<std::string>{"L0", sock.listenSockets.at(0).string()}));

    Policy open;
    open.listenPorts = AnyPort{};
    assert(sbpl::profile(open).contains("(local tcp \"*:*\")"));
    assert(sbpl::parameters(open).empty());
    // Default: no TCP listener at all.
    assert(!sbpl::profile(Policy{}).contains("local tcp"));
    return 0;
} catch (const std::exception & err) {
    static_cast<void>(std::fputs(err.what(), stderr));
    return 1;
}
