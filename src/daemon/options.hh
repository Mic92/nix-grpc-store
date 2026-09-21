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
#include "niks3-client.hh"
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
    std::string schedulerTokenFile; // bearer towards a remote scheduler, optional
    // How the balancer reaches this worker. Goes into Assigned.worker_addr.
    std::string advertise;
    unsigned maxJobs = 0; // 0 = take nix's max-jobs
    uint64_t minFree = 0;
    std::string storeDir;
};

auto parseOptions(const std::vector<std::string_view> & args) -> Options;

// --scheduler http://host:port is dialed without TLS.
constexpr std::string_view plaintextScheme = "http://";
constexpr unsigned certRefreshSeconds = 10;

auto makeServerCredentials(const Options & options) -> std::shared_ptr<grpc::ServerCredentials>;

} // namespace nixgrpc
