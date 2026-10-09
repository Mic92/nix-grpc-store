#include "sbpl.hh"

#include <cstddef>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "sandbox.hh"

namespace nixgrpc::sandbox::sbpl {

namespace {

// The prefix of the parameter names a rule refers to. profile() and
// parameters() both derive the names from here.
enum class Param : char {
    write = 'W',
    connect = 'S',
    listen = 'L',
    exec = 'X',
};

enum class Filter : uint8_t {
    subpath,
    literal,
    remoteUnixSocket,
    localUnixSocket,
};

auto paramName(Param param, size_t idx) -> std::string { return std::format("{}{}", static_cast<char>(param), idx); }

auto filterExpr(Filter filter, const std::string & name) -> std::string
{
    switch (filter) {
    case Filter::subpath:
        return std::format("(subpath (param \"{}\"))", name);
    case Filter::literal:
        return std::format("(literal (param \"{}\"))", name);
    case Filter::remoteUnixSocket:
        return std::format("(remote unix-socket (path-literal (param \"{}\")))", name);
    case Filter::localUnixSocket:
        return std::format("(local unix-socket (path-literal (param \"{}\")))", name);
    }
    return {};
}

struct Rule
{
    std::string_view action;
    Filter filter;
    Param param;
};

// One allow rule per path of the list that `rule.param` names.
[[nodiscard]] auto rules(const Rule & rule, size_t count) -> std::string
{
    std::string out;
    for (size_t idx = 0; idx < count; ++idx) {
        out += std::format("(allow {} {})\n", rule.action, filterExpr(rule.filter, paramName(rule.param, idx)));
    }
    return out;
}

void addParams(std::vector<std::string> & out, Param param, const std::vector<CanonicalPath> & paths)
{
    for (size_t idx = 0; idx < paths.size(); ++idx) {
        out.push_back(paramName(param, idx));
        out.push_back(paths.at(idx).string());
    }
}

} // namespace

auto profile(const Policy & policy) -> std::string
{
    std::string out =
        "(version 1)\n"
        "(deny default)\n"
        "(import \"system.sb\")\n"
        // Reading is unrestricted, see Policy.
        "(allow file-read*)\n"
        "(allow file-write* (literal \"/dev/null\") (literal \"/dev/dtracehelper\"))\n"
        "(allow sysctl-read)\n"
        "(allow signal (target self))\n"
        "(allow process-fork)\n"
        // DNS and certificate trust are Mach services (mDNSResponder, trustd).
        "(allow mach-lookup)\n"
        // Hosts cannot be filtered, only ports. Outbound TCP covers workers,
        // the scheduler, OIDC discovery and the niks3 server.
        "(allow network-outbound (remote tcp \"*:*\"))\n"
        "(allow network-outbound (remote udp \"*:53\"))\n"
        // getaddrinfo, which Go and most clients use, asks mDNSResponder.
        "(allow network-outbound (remote unix-socket (path-literal \"/private/var/run/mDNSResponder\")))\n";
    out += rules(
        {.action = "file-read* file-write*", .filter = Filter::subpath, .param = Param::write},
        policy.writePaths.size());
    out += rules(
        {.action = "network-outbound", .filter = Filter::remoteUnixSocket, .param = Param::connect},
        policy.connectSockets.size());
    out += rules(
        {.action = "network-bind network-inbound", .filter = Filter::localUnixSocket, .param = Param::listen},
        policy.listenSockets.size());
    out += rules(
        {.action = "file-write-create file-write-unlink", .filter = Filter::literal, .param = Param::listen},
        policy.listenSockets.size());
    out += rules({.action = "process-exec", .filter = Filter::subpath, .param = Param::exec}, policy.execPaths.size());
    if (auto const * ports = std::get_if<std::vector<uint16_t>>(&policy.listenPorts)) {
        for (auto const port : *ports) {
            out += std::format("(allow network-bind network-inbound (local tcp \"*:{}\"))\n", port);
        }
    } else {
        out += "(allow network-bind network-inbound (local tcp \"*:*\"))\n";
    }
    return out;
}

auto parameters(const Policy & policy) -> std::vector<std::string>
{
    std::vector<std::string> out;
    addParams(out, Param::write, policy.writePaths);
    addParams(out, Param::connect, policy.connectSockets);
    addParams(out, Param::listen, policy.listenSockets);
    addParams(out, Param::exec, policy.execPaths);
    return out;
}

} // namespace nixgrpc::sandbox::sbpl
