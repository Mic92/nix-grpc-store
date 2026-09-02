#pragma once
// Parser for the worker-protocol stderr stream the backend nix-daemon sends
// around each operation. Shared between the daemon's build RPCs and the fuzzer.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/logging.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>

namespace nixgrpc {

// An error reported by the backend daemon, as opposed to a malformed stream.
class BackendError final : public nix::CloneableError<BackendError, nix::Error>
{
    void anchor() override {}

public:
    using CloneableError<BackendError, nix::Error>::CloneableError;
};

// nix::readString() preallocates the announced length.
constexpr size_t kMaxLogString = 1UL << 20;

namespace detail {

inline auto readLogFields(nix::Source & source) -> std::vector<std::string>
{
    std::vector<std::string> fields;
    auto count = nix::readNum<uint64_t>(source);
    for (uint64_t idx = 0; idx < count; ++idx) {
        auto type = nix::readNum<uint64_t>(source);
        if (type == 0) {
            nix::readNum<uint64_t>(source);
            fields.emplace_back();
        } else if (type == 1) {
            fields.push_back(nix::readString(source, kMaxLogString));
        } else {
            throw nix::Error("unknown logger field type %d from backend", type);
        }
    }
    return fields;
}

} // namespace detail

// Consumes the stream up to STDERR_LAST, forwarding plain build log lines.
// Throws BackendError on STDERR_ERROR.
inline void relayBuildLog(nix::Source & source, const std::function<void(std::string)> & sendLogLine)
{
    while (true) {
        auto msg = nix::readNum<uint64_t>(source);
        if (msg == STDERR_NEXT) {
            sendLogLine(nix::chomp(nix::readString(source, kMaxLogString)));
        } else if (msg == STDERR_START_ACTIVITY) {
            nix::readNum<uint64_t>(source); // id
            nix::readNum<uint64_t>(source); // level
            nix::readNum<uint64_t>(source); // type
            nix::readString(source, kMaxLogString); // text
            detail::readLogFields(source);
            nix::readNum<uint64_t>(source); // parent
        } else if (msg == STDERR_STOP_ACTIVITY) {
            nix::readNum<uint64_t>(source);
        } else if (msg == STDERR_RESULT) {
            nix::readNum<uint64_t>(source); // id
            auto type = nix::readNum<uint64_t>(source);
            auto fields = detail::readLogFields(source);
            if (type == nix::resBuildLogLine && !fields.empty()) {
                sendLogLine(std::move(fields.front()));
            }
        } else if (msg == STDERR_ERROR) {
            throw BackendError(nix::readError(source).info());
        } else if (msg == STDERR_LAST) {
            break;
        } else {
            // STDERR_READ/WRITE need a tunnel we do not provide.
            throw nix::Error("unexpected worker protocol message 0x%x from backend", msg);
        }
    }
}

} // namespace nixgrpc
