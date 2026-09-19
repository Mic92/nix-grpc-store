#pragma once
// Command line of nix-grpc-daemon.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/security/server_credentials.h>

#include "acl.hh"
#include "cache.hh"
#include "logfmt.hh"
#include "xfcc.hh"

namespace nixgrpc {

struct Options
{
    std::string listen = "0.0.0.0:50051";
    std::optional<std::chrono::seconds> idleTimeout;
    std::string socketPath = "/nix/var/nix/daemon-socket/socket";
    // Store URI for the native bulk RPCs. Defaults to the proxy socket.
    std::string storeUri;
    std::string tlsCert;
    std::string tlsKey;
    std::string clientCA;
    std::string metricsListen;
    std::string workerName;
    LogLevel logLevel = LogLevel::info;
    Acl acl;
    xfcc::TrustedProxies proxies;
    std::string oidcConfig;
    Niks3Config niks3;

    bool builder = true;
    bool scheduler = true;
    // Empty: in-process scheduler. Else the balancer URL (host:port) to dial.
    std::string schedulerAddr;
    std::string schedulerCA; // server CA for schedulerAddr; client cert = tlsCert/tlsKey
    // Nodes with the scheduler role that outrank this one, in order. While any
    // of them serves, this one stays passive. Same TLS as schedulerAddr.
    std::vector<std::string> yieldTo;
    // How the balancer reaches this worker; goes into Assigned.worker_addr.
    std::string advertise;
    unsigned maxJobs = 1;
    uint64_t minFree = 0;
    std::string storeDir;
};

auto parseOptions(const std::vector<std::string_view> & args) -> Options;
auto makeServerCredentials(const Options & options) -> std::shared_ptr<grpc::ServerCredentials>;

} // namespace nixgrpc
