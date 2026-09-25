#pragma once
// Append-only build log kept in an unlinked temp file, so a build that many
// callers attach to costs constant memory however chatty it is. One writer
// (the build's owner) appends; any number of readers replay from their own
// offset while it grows. Errors are values: a spill that cannot be written
// just stops recording, it never fails the build.

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace nixgrpc {

// Same values as BuildEvent::Kind (build-log.hh), which main.cc asserts.
enum class SpillKind : std::uint8_t { line, phase };

class UniqueFd
{
public:
    UniqueFd() = default;
    explicit UniqueFd(int descriptor) noexcept : fd_(descriptor) {}
    UniqueFd(UniqueFd && other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    auto operator=(UniqueFd && other) noexcept -> UniqueFd &
    {
        if (this != &other) {
            reset();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    UniqueFd(const UniqueFd &) = delete;
    auto operator=(const UniqueFd &) -> UniqueFd & = delete;
    ~UniqueFd() { reset(); }

    [[nodiscard]] auto get() const noexcept -> int { return fd_; }
    explicit operator bool() const noexcept { return fd_ >= 0; }

private:
    void reset() noexcept
    {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = -1;
    }

    int fd_ = -1;
};

class LogSpill
{
public:
    // Past this the log stops growing and ends in a "truncated" record. The
    // record itself may overshoot by its own few bytes.
    static constexpr std::size_t kDefaultCap = std::size_t{64} << 20;
    static constexpr std::size_t kMaxText = std::size_t{1} << 20;
    static constexpr std::string_view kTruncated = "[log truncated by the worker]";

    explicit LogSpill(std::size_t cap = kDefaultCap) noexcept : cap_(cap) {}

    // Owner only.
    void append(SpillKind kind, std::string_view text)
    {
        if (ended_) {
            return;
        }
        text = text.substr(0, kMaxText);
        auto const tail = written_.load(std::memory_order_relaxed);
        if (tail + kHeader + text.size() > cap_) {
            ended_ = true; // the marker is the last record
            kind = SpillKind::line;
            text = kTruncated;
        }
        if (!fd_) {
            auto opened = openTemp();
            if (!opened) {
                ended_ = true;
                return;
            }
            fd_ = std::move(*opened);
        }
        auto const len = std::bit_cast<std::array<std::byte, sizeof(std::uint32_t)>>(static_cast<std::uint32_t>(text.size()));
        std::array<std::byte, kHeader> header{};
        header.front() = static_cast<std::byte>(kind);
        std::ranges::copy(len, std::next(header.begin()));
        if (!writeAt(header, tail) || !writeAt(std::as_bytes(std::span(text)), tail + kHeader)) {
            ended_ = true;
            return;
        }
        // Publishes fd_ and the bytes to readers.
        written_.store(tail + kHeader + text.size(), std::memory_order_release);
    }

    // Bytes readable so far. A reader that has drained up to here is caught up.
    [[nodiscard]] auto written() const noexcept -> std::size_t { return written_.load(std::memory_order_acquire); }

    // Feeds every complete record at or after `offset` to emit(SpillKind,
    // string_view) and returns the offset to resume from. A record that cannot
    // be read is skipped along with everything after it.
    template<typename Emit>
    [[nodiscard]] auto drain(std::size_t offset, const Emit & emit) const -> std::size_t
    {
        auto const end = written();
        std::string text;
        while (offset < end) {
            std::array<std::byte, kHeader> header{};
            if (!readAt(header, offset)) {
                return end;
            }
            std::array<std::byte, sizeof(std::uint32_t)> raw{};
            std::ranges::copy(std::span(header).subspan<1>(), raw.begin());
            auto const len = std::bit_cast<std::uint32_t>(raw);
            auto const kind = std::to_integer<std::uint8_t>(header.front());
            if (len > kMaxText || kind > static_cast<std::uint8_t>(SpillKind::phase)) {
                return end;
            }
            text.resize(len);
            if (!readAt(std::as_writable_bytes(std::span(text)), offset + kHeader)) {
                return end;
            }
            emit(static_cast<SpillKind>(kind), std::string_view(text));
            offset += kHeader + len;
        }
        return offset;
    }

private:
    static constexpr std::size_t kHeader = 1 + sizeof(std::uint32_t);

    using Opened = std::expected<UniqueFd, std::error_code>;

    // The directory follows TMPDIR: /tmp is a size-limited tmpfs on NixOS.
    // O_TMPFILE is Linux-only, and not every Linux filesystem has it, so fall
    // back to create-then-unlink.
    static auto openTemp() -> Opened
    {
        std::error_code error;
        auto const dir = std::filesystem::temp_directory_path(error);
        if (error) {
            return std::unexpected(error);
        }
        UniqueFd file;
#ifdef O_TMPFILE
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,bugprone-signed-bitwise): C API takes int flags.
        file = UniqueFd(::open(dir.c_str(), O_TMPFILE | O_RDWR | O_CLOEXEC, S_IRUSR | S_IWUSR));
#endif
        if (!file) {
            auto name = (dir / "nix-grpc-log-XXXXXX").string();
            file = UniqueFd(::mkostemp(name.data(), O_CLOEXEC));
            if (!file) {
                return std::unexpected(std::error_code(errno, std::generic_category()));
            }
            ::unlink(name.c_str());
        }
        return {std::move(file)};
    }

    [[nodiscard]] auto writeAt(std::span<const std::byte> data, std::size_t offset) const -> bool
    {
        while (!data.empty()) {
            auto const count = ::pwrite(fd_.get(), data.data(), data.size(), static_cast<off_t>(offset));
            if (count < 0 && errno == EINTR) {
                continue;
            }
            if (count <= 0) {
                return false;
            }
            data = data.subspan(static_cast<std::size_t>(count));
            offset += static_cast<std::size_t>(count);
        }
        return true;
    }

    [[nodiscard]] auto readAt(std::span<std::byte> out, std::size_t offset) const -> bool
    {
        while (!out.empty()) {
            auto const count = ::pread(fd_.get(), out.data(), out.size(), static_cast<off_t>(offset));
            if (count < 0 && errno == EINTR) {
                continue;
            }
            if (count <= 0) {
                return false;
            }
            out = out.subspan(static_cast<std::size_t>(count));
            offset += static_cast<std::size_t>(count);
        }
        return true;
    }

    std::size_t cap_;
    UniqueFd fd_; // owner writes it before the first release store of written_
    std::atomic<std::size_t> written_{0};
    bool ended_ = false; // owner only: truncated, or the temp file failed
};

// A reader's side of the protocol: replay `spill` to emit(SpillKind, text) and
// keep following it until the owner sets `finished` and everything is drained.
// `finished` is guarded by `mutex`, and its writer must notify `wakeup` under that
// lock after the last append. False if `cancelled()` ended the wait first.
template<typename Emit, typename Cancelled>
auto followSpill(
    const LogSpill & spill,
    std::mutex & mutex,
    std::condition_variable & wakeup,
    const bool & finished,
    std::chrono::milliseconds poll,
    const Cancelled & cancelled,
    const Emit & emit) -> bool
{
    std::size_t offset = 0;
    std::unique_lock lock(mutex);
    while (true) {
        lock.unlock();
        offset = spill.drain(offset, emit);
        lock.lock();
        if (finished && offset >= spill.written()) {
            return true;
        }
        if (cancelled()) {
            return false;
        }
        wakeup.wait_for(lock, poll, [&] -> bool { return finished || spill.written() > offset; });
    }
}

} // namespace nixgrpc
