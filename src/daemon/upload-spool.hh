#pragma once
// A NAR read off the wire into an unlinked temp file.

#include <algorithm>
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

class SpooledNar
{
public:
    static auto spool(nix::Source & source, std::uint64_t size) -> SpooledNar
    {
        auto opened = openTempFile();
        if (!opened) {
            throw nix::Error("cannot create a spool file: %s", opened.error().message());
        }
        SpooledNar nar(std::move(*opened));
        copy(source, size, [&](std::string_view chunk) -> void { nix::writeFull(nar.file_.get(), chunk); });
        ::lseek(nar.file_.get(), 0, SEEK_SET);
        return nar;
    }

    static void discard(nix::Source & source, std::uint64_t size)
    {
        copy(source, size, [](std::string_view) -> void {});
    }

    [[nodiscard]] auto reader() const -> nix::FdSource { return {file_.get()}; }

private:
    static constexpr std::size_t kChunk = 1U << 20U;

    explicit SpooledNar(UniqueFd file) : file_(std::move(file)) {}

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
};

} // namespace nixgrpc
