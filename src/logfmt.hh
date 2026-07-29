#pragma once
// Minimal logfmt emitter for the daemon's access log. One key=value line per
// event on stderr, so journald/Loki can parse it and journalctl stays readable.

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>

namespace nixgrpc {

// Quote a value if it contains characters that would break key=value parsing.
inline auto logfmtValue(std::string_view value) -> std::string
{
    bool needsQuotes = value.empty();
    for (char const chr : value) {
        if (chr == ' ' || chr == '"' || chr == '=' || chr == '\\' || chr == '\n') {
            needsQuotes = true;
            break;
        }
    }
    if (!needsQuotes) {
        return std::string(value);
    }
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (char const chr : value) {
        if (chr == '"' || chr == '\\') {
            out.push_back('\\');
            out.push_back(chr);
        } else if (chr == '\n') {
            out += "\\n";
        } else {
            out.push_back(chr);
        }
    }
    out.push_back('"');
    return out;
}

enum class LogLevel : std::uint8_t { info, debug };

// Emit one logfmt line: ts=… level=… key=value …
inline void logLine(LogLevel level, std::initializer_list<std::pair<std::string_view, std::string>> fields)
{
    auto const now = std::chrono::system_clock::now();
    auto const secs = std::chrono::system_clock::to_time_t(now);
    std::tm utc{};
    gmtime_r(&secs, &utc);
    std::array<char, sizeof("2000-01-01T00:00:00Z")> timestamp{};
    static_cast<void>(std::strftime(timestamp.data(), timestamp.size(), "%Y-%m-%dT%H:%M:%SZ", &utc));

    std::string line = "ts=";
    line += timestamp.data();
    line += level == LogLevel::debug ? " level=debug" : " level=info";
    for (const auto & [key, value] : fields) {
        line += ' ';
        line += key;
        line += '=';
        line += logfmtValue(value);
    }
    line += '\n';
    // Single write keeps lines from interleaving across handler threads.
    static_cast<void>(std::fputs(line.c_str(), stderr));
}

// TLS client identity (x509 CN) or "-" when the client did not present a
// certificate (no --client-ca configured).
inline auto clientCommonName(const grpc::ServerContext & context) -> std::string
{
    auto auth = context.auth_context();
    if (auth) {
        auto values = auth->FindPropertyValues(GRPC_X509_CN_PROPERTY_NAME);
        if (!values.empty()) {
            return {values.front().data(), values.front().size()};
        }
    }
    return "-";
}

} // namespace nixgrpc
