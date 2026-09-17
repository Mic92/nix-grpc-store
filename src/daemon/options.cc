// Command line of nix-grpc-daemon.

#include "options.hh"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/security/server_credentials.h>

#include <nix/util/error.hh>
#include <nix/util/file-system.hh>

#include "acl.hh"
#include "logfmt.hh"
#include "parse-int.hh"

namespace nixgrpc {

namespace {
auto parseLogLevel(std::string_view value) -> LogLevel
{
    if (value == "debug") {
        return LogLevel::debug;
    }
    if (value != "info") {
        throw nix::Error("--log-level must be 'info' or 'debug'");
    }
    return LogLevel::info;
}
} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity): flat flag list.
auto parseOptions(const std::vector<std::string_view> & args) -> Options
{
    Options options;
    for (size_t idx = 1; idx < args.size(); ++idx) {
        std::string_view const arg = args.at(idx);
        auto next = [&]() -> std::string_view {
            if (++idx >= args.size()) {
                throw nix::Error("flag '%s' requires an argument", arg);
            }
            return args.at(idx);
        };
        if (arg == "--listen") {
            options.listen = next();
        } else if (arg == "--proxy-socket") {
            options.socketPath = next();
        } else if (arg == "--proxy-store") {
            options.storeUri = next();
        } else if (arg == "--tls-cert") {
            options.tlsCert = next();
        } else if (arg == "--tls-key") {
            options.tlsKey = next();
        } else if (arg == "--client-ca") {
            options.clientCA = next();
        } else if (arg == "--allow") {
            options.acl.addRule(next());
        } else if (arg == "--allow-anonymous") {
            options.acl.allowAnonymous(parseRole(next()));
        } else if (arg == "--metrics-listen") {
            options.metricsListen = next();
        } else if (arg == "--idle-timeout") {
            auto secs = parseInt<unsigned>(next());
            if (!secs) {
                throw nix::Error("--idle-timeout expects a non-negative integer");
            }
            options.idleTimeout = std::chrono::seconds(*secs);
        } else if (arg == "--log-level") {
            options.logLevel = parseLogLevel(next());
        } else {
            throw nix::Error("unknown flag '%s'", arg);
        }
    }
    if ((options.acl.active() || options.acl.anonymousRole()) && options.clientCA.empty()) {
        // Without mTLS every client's CN is "-".
        throw nix::Error("--allow/--allow-anonymous requires --client-ca");
    }
    if (!options.clientCA.empty() && !options.acl.anonymousRole()) {
        options.acl.requireCertificate();
    }
    return options;
}

auto makeServerCredentials(const Options & options) -> std::shared_ptr<grpc::ServerCredentials>
{
    if (options.tlsCert.empty()) {
        if (!options.clientCA.empty()) {
            throw nix::Error("--client-ca requires --tls-cert/--tls-key");
        }
        return grpc::InsecureServerCredentials();
    }
    // Cert-less clients pass the handshake and are denied by the ACL with a
    // readable UNAUTHENTICATED instead of an opaque "Socket closed".
    auto const clientCertRequest = options.clientCA.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
                                                             : GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;
    grpc::SslServerCredentialsOptions ssl(clientCertRequest);
    ssl.pem_key_cert_pairs.push_back(
        {.private_key = nix::readFile(options.tlsKey), .cert_chain = nix::readFile(options.tlsCert)});
    if (!options.clientCA.empty()) {
        ssl.pem_root_certs = nix::readFile(options.clientCA);
    }
    return grpc::SslServerCredentials(ssl);
}

} // namespace nixgrpc
