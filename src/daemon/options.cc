// Command line of nix-grpc-daemon.

#include "options.hh"
#include "nix-compat.hh"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <string_view>
#include <vector>

#include <unistd.h>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/security/server_credentials.h>

#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/strings.hh>
#include <nix/util/util.hh>
#include <nix/util/environment-variables.hh>

#include "acl.hh"
#include "logfmt.hh"
#include "parse-int.hh"
#include "scheduler.hh"
#include "xfcc.hh"

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

auto localHostName() -> std::string
{
    constexpr size_t maxLen = 256;
    std::array<char, maxLen> buf{};
    if (::gethostname(buf.data(), buf.size() - 1) != 0) {
        return "?";
    }
    return buf.data();
}
} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity): flat flag list.
auto parseOptions(const std::vector<std::string_view> & args) -> Options
{
    Options options;
    for (size_t idx = 1; idx < args.size(); ++idx) {
        // Also `--flag=value`, one list item in a container spec.
        std::string_view arg = args.at(idx);
        std::optional<std::string_view> inlineValue;
        if (auto sep = arg.find('='); arg.starts_with("--") && sep != std::string_view::npos) {
            inlineValue = arg.substr(sep + 1);
            arg = arg.substr(0, sep);
        }
        auto next = [&]() -> std::string_view {
            if (inlineValue) {
                return *std::exchange(inlineValue, std::nullopt);
            }
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
        } else if (arg == "--trusted-proxy") {
            options.proxies.add(next());
        } else if (arg == "--allow-anonymous") {
            options.acl.allowAnonymous(parseRole(next()));
        } else if (arg == "--worker-name") {
            options.workerName = next();
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
        } else if (arg == "--oidc-config") {
            options.oidcConfig = next();
        } else if (arg == "--niks3") {
            options.niks3.url = next();
        } else if (arg == "--niks3-token-file") {
            options.niks3.tokenFile = next();
        } else if (arg == "--niks3-push") {
            // The program plus extra flags, e.g. "niks3 push --max-concurrent-uploads 8".
            auto words = nix::shellSplitString(next());
            options.niks3.pushArgv = {words.begin(), words.end()};
        } else if (arg == "--role") {
            options.builder = options.scheduler = false;
            for (auto & role : nix::tokenizeString<std::vector<std::string>>(next(), ",")) {
                if (role == "builder") {
                    options.builder = true;
                } else if (role == "scheduler") {
                    options.scheduler = true;
                } else {
                    throw nix::Error("--role: unknown role '%s' (builder, scheduler)", role);
                }
            }
        } else if (arg == "--scheduler") {
            options.schedulerAddr = next();
        } else if (arg == "--advertise") {
            options.advertise = next();
        } else if (arg == "--max-jobs") {
            auto jobs = parseInt<unsigned>(next());
            if (!jobs || *jobs < 1 || *jobs > static_cast<unsigned>(sched::FreeIndex::maxSlots)) {
                throw nix::Error("--max-jobs expects an integer in 1..%d", sched::FreeIndex::maxSlots);
            }
            options.maxJobs = *jobs;
        } else if (arg == "--min-free") {
            options.minFree = nix::string2IntWithUnitPrefix<uint64_t>(next());
        } else {
            throw nix::Error("unknown flag '%s'", arg);
        }
    }
    if (options.workerName.empty()) {
        options.workerName = localHostName();
    }
    if (options.niks3.enabled()) {
        if (options.niks3.tokenFile.empty()) {
            throw nix::Error("--niks3 needs --niks3-token-file");
        }
        auto & argv = options.niks3.pushArgv;
        if (argv.empty()) {
            argv = {"niks3", "push"};
        }
        argv.insert(
            argv.end(), {"--stdin", "--server-url", options.niks3.url, "--auth-token-path", options.niks3.tokenFile});
    }
    if (!options.builder && !options.scheduler) {
        throw nix::Error("--role: need at least one of builder, scheduler");
    }
    if (!options.builder) {
        options.maxJobs = 0;
    } else if (options.maxJobs == 0) {
        options.maxJobs = std::clamp(nixcompat::maxBuildJobs(), 1U, static_cast<unsigned>(sched::FreeIndex::maxSlots));
    }
    if (options.schedulerAddr.empty() && !options.scheduler) {
        throw nix::Error("--role builder without --scheduler has nobody to take work from");
    }
    if (options.advertise.empty()) {
        options.advertise = options.listen;
    }
    if (options.storeDir.empty()) {
        options.storeDir = nix::getEnv("NIX_STORE_DIR").value_or("/nix/store");
    }
    if ((options.acl.active() || options.acl.anonymousRole()) && options.clientCA.empty() && options.oidcConfig.empty()) {
        throw nix::Error("--allow/--allow-anonymous requires --client-ca or --oidc-config");
    }
    if (!options.oidcConfig.empty() && options.tlsCert.empty()) {
        // Bearer tokens in clear text are replayable by anyone on the path.
        logLine(LogLevel::info, {{"event", "warning"}, {"msg", "--oidc-config without --tls-cert sends bearer tokens in clear"}});
    }
    if (!options.proxies.empty() && options.clientCA.empty()) {
        throw nix::Error("--trusted-proxy requires --client-ca");
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
