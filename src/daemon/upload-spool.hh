#pragma once
// A NAR read completely off the wire into an unlinked temp file, so the store
// import that follows reads from disk. Importing straight from the stream lets
// a path lock held by one upload stall its reader, and an unread HTTP/2
// stream can fill a window that other uploads share.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <cstdint>
#include <span>
#include <string>

#include <nix/util/error.hh>
#include <nix/util/serialise.hh>
#include <unistd.h>

#include "log-spill.hh"

namespace nixgrpc {

class SpooledNar
{
public:
    // Reads exactly `size` bytes from `source`. Keeps reading after a failed
    // write so the client's stream is drained either way, then throws.
    static auto spool(nix::Source & source, std::uint64_t size) -> SpooledNar
    {
        auto opened = openTempFile();
        if (!opened) {
            drain(source, size);
            throw nix::Error("cannot create a spool file: %s", opened.error().message());
        }
        UniqueFd file = std::move(*opened);
        std::array<char, kChunk> buf{};
        std::uint64_t offset = 0;
        bool writable = true;
        int writeErrno = 0;
        while (offset < size) {
            auto const want = static_cast<std::size_t>(std::min<std::uint64_t>(buf.size(), size - offset));
            source(buf.data(), want);
            if (writable) {
                writable = writeAll(file.get(), std::span<const char>(buf.data(), want), offset);
                if (!writable) {
                    writeErrno = errno;
                }
            }
            offset += want;
        }
        if (!writable) {
            throw nix::Error("cannot write the spool file: %s", std::strerror(writeErrno));
        }
        return SpooledNar(std::move(file), size);
    }

    static void drain(nix::Source & source, std::uint64_t size)
    {
        std::array<char, kChunk> buf{};
        while (size > 0) {
            auto const want = static_cast<std::size_t>(std::min<std::uint64_t>(buf.size(), size));
            source(buf.data(), want);
            size -= want;
        }
    }

    // A Source over the spooled bytes, from the start.
    class Reader : public nix::Source
    {
    public:
        explicit Reader(const SpooledNar & nar) : fd_(nar.file_.get()), left_(nar.size_) {}
        auto read(char * data, size_t len) -> size_t override
        {
            if (left_ == 0) {
                throw nix::EndOfFile("end of spooled NAR");
            }
            auto const want = static_cast<size_t>(std::min<std::uint64_t>(len, left_));
            for (;;) {
                auto const count = ::pread(fd_, data, want, static_cast<off_t>(offset_));
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                if (count <= 0) {
                    throw nix::Error("cannot read the spool file: %s", count == 0 ? "short file" : std::strerror(errno));
                }
                offset_ += static_cast<std::uint64_t>(count);
                left_ -= static_cast<std::uint64_t>(count);
                return static_cast<size_t>(count);
            }
        }

    private:
        int fd_;
        std::uint64_t left_;
        std::uint64_t offset_ = 0;
    };

    [[nodiscard]] auto reader() const -> Reader { return Reader(*this); }

private:
    static constexpr std::size_t kChunk = 1U << 20U;

    SpooledNar(UniqueFd file, std::uint64_t size) : file_(std::move(file)), size_(size) {}

    static auto writeAll(int fd, std::span<const char> data, std::uint64_t offset) -> bool
    {
        while (!data.empty()) {
            auto const count = ::pwrite(fd, data.data(), data.size(), static_cast<off_t>(offset));
            if (count < 0 && errno == EINTR) {
                continue;
            }
            if (count <= 0) {
                return false;
            }
            data = data.subspan(static_cast<std::size_t>(count));
            offset += static_cast<std::uint64_t>(count);
        }
        return true;
    }

    UniqueFd file_;
    std::uint64_t size_;
};

} // namespace nixgrpc
