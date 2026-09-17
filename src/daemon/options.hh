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
#include "farm.hh"
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
    LogLevel logLevel = LogLevel::info;
    Acl acl;
    xfcc::TrustedProxies proxies;
    FarmConfig farm; // active when niks3Url is set
};

auto parseOptions(const std::vector<std::string_view> & args) -> Options;
auto makeServerCredentials(const Options & options) -> std::shared_ptr<grpc::ServerCredentials>;

} // namespace nixgrpc
