#pragma once
// NARs read off the wire into an unlinked temp file.

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <unistd.h>

#include "log-spill.hh"

namespace nixgrpc {

struct SpoolRange
{
    std::uint64_t offset = 0;
    std::uint64_t size = 0;
};

// pread keeps the offset the spool appends at.
class RangeSource : public nix::Source
{
public:
    RangeSource(int descriptor, SpoolRange range) : descriptor_(descriptor), pos_(range.offset), end_(range.offset + range.size) {}

    auto read(char * data, std::size_t len) -> std::size_t override
    {
        if (pos_ == end_) {
            throw nix::EndOfFile("end of spooled NAR");
        }
        auto const want = static_cast<std::size_t>(std::min<std::uint64_t>(len, end_ - pos_));
        ssize_t got = ::pread(descriptor_, data, want, static_cast<off_t>(pos_));
        while (got < 0 && errno == EINTR) {
            got = ::pread(descriptor_, data, want, static_cast<off_t>(pos_));
        }
        if (got < 0) {
            throw nix::SysError("reading a spooled NAR");
        }
        if (got == 0) {
            throw nix::EndOfFile("spool file is shorter than expected");
        }
        pos_ += static_cast<std::uint64_t>(got);
        return static_cast<std::size_t>(got);
    }

private:
    int descriptor_;
    std::uint64_t pos_;
    std::uint64_t end_;
};

// One file per request: a descriptor per NAR ran out on big closures.
class NarSpool
{
public:
    NarSpool()
    {
        auto opened = openTempFile();
        if (!opened) {
            throw nix::Error("cannot create a spool file: %s", opened.error().message());
        }
        file_ = std::move(*opened);
    }

    auto add(nix::Source & source, std::uint64_t size) -> SpoolRange
    {
        SpoolRange const range{.offset = tail_, .size = size};
        copy(source, size, [&](std::string_view chunk) -> void { nix::writeFull(file_.get(), chunk); });
        tail_ += size;
        return range;
    }

    static void discard(nix::Source & source, std::uint64_t size)
    {
        copy(source, size, [](std::string_view) -> void {});
    }

    [[nodiscard]] auto reader(SpoolRange range) const -> RangeSource { return {file_.get(), range}; }

private:
    static constexpr std::size_t kChunk = 1U << 20U;

    static void copy(nix::Source & source, std::uint64_t size, const auto & sink)
    {
        std::vector<char> buf(kChunk);
        while (size > 0) {
            auto const want = static_cast<std::size_t>(std::min<std::uint64_t>(buf.size(), size));
            source(buf.data(), want);
            sink(std::string_view(buf.data(), want));
            size -= want;
        }
    }

    UniqueFd file_;
    std::uint64_t tail_ = 0;
};

} // namespace nixgrpc
