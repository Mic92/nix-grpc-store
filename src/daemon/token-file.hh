#pragma once
// Bearer token from a file that may be swapped at runtime (Kubernetes
// projected service account tokens rotate hourly).

#include <string>
#include <utility>

#include <sys/stat.h>

#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/sync.hh>
#include <nix/util/util.hh>

namespace nixgrpc {

class TokenFile
{
    std::string path;

    struct Cached
    {
        std::string token;
        struct timespec mtime{};
        ino_t ino = 0;
    };
    nix::Sync<Cached> cached;

public:
    explicit TokenFile(std::string path_)
        : path(std::move(path_))
    {
        (void) get();
    }

    // Re-reads on mtime or inode change (projected volumes swap a symlink).
    auto get() -> std::string
    {
        struct stat info{};
        if (::stat(path.c_str(), &info) != 0) {
            throw nix::SysError("token file '%s'", path);
        }
        auto cur(cached.lock());
        if (cur->token.empty() || info.st_ino != cur->ino || info.st_mtim.tv_sec != cur->mtime.tv_sec
            || info.st_mtim.tv_nsec != cur->mtime.tv_nsec) {
            auto token = nix::chomp(nix::readFile(path));
            if (token.empty()) {
                throw nix::Error("token file '%s' is empty", path);
            }
            *cur = {.token = std::move(token), .mtime = info.st_mtim, .ino = info.st_ino};
        }
        return cur->token;
    }
};

} // namespace nixgrpc
