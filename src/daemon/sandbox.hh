#pragma once
// Process sandbox, applied once after startup. Implemented per platform in
// sandbox-<os>.cc: Seatbelt on macOS, Landlock on Linux.

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>
#include <variant>
#include <vector>

namespace nixgrpc::sandbox {

// The kernel checks resolved paths (/var is /private/var on macOS), so the
// policy only accepts paths that went through canonicalization.
class CanonicalPath
{
public:
    // The path need not exist yet: a socket or a cache dir may be created later.
    [[nodiscard]] static auto from(const std::filesystem::path & path) -> CanonicalPath
    {
        if (path.empty()) {
            throw std::invalid_argument("sandbox: empty path");
        }
        std::error_code error;
        // A bare "cert.pem" has an empty parent, which would resolve to nothing.
        auto resolved = std::filesystem::weakly_canonical(std::filesystem::absolute(path), error);
        if (error) {
            throw std::system_error(error, "canonicalizing '" + path.string() + "'");
        }
        return CanonicalPath(std::move(resolved));
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path & { return path_; }

    [[nodiscard]] auto string() const -> std::string { return path_.string(); }

    [[nodiscard]] auto parent() const -> CanonicalPath { return CanonicalPath(path_.parent_path()); }

private:
    explicit CanonicalPath(std::filesystem::path path)
        : path_(std::move(path))
    {
    }

    std::filesystem::path path_;
};

struct AnyPort
{};

// Listening on any TCP port, or exactly these (possibly none).
using ListenPorts = std::variant<AnyPort, std::vector<uint16_t>>;

struct Policy
{
    // Everything is readable: certificates and tokens are re-read in place and
    // rotation repoints symlinks, so no path list stays valid. File permissions
    // still apply. The policy restricts writes, exec, sockets and TCP ports.
    std::vector<CanonicalPath> writePaths;
    // Unix sockets the daemon may connect to.
    std::vector<CanonicalPath> connectSockets;
    // Unix sockets the daemon may create and listen on.
    std::vector<CanonicalPath> listenSockets;
    // Subtrees the daemon may execute from.
    std::vector<CanonicalPath> execPaths;
    ListenPorts listenPorts = std::vector<uint16_t>{};
};

// Irrevocable. Throws where no backend exists or the kernel refuses. Call it
// before starting threads: Landlock only covers threads created afterwards.
void apply(const Policy & policy);

} // namespace nixgrpc::sandbox
