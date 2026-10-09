#include "sandbox.hh"

#include <array>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <format>
#include <iterator>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <linux/prctl.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>

// The syscalls and constants are declared here instead of <linux/landlock.h>
// because distro headers lag behind the kernel, and the ABI is stable.
namespace nixgrpc::sandbox {

namespace {

// Each kind of right is its own type, so a net right cannot be passed where a
// filesystem right is expected (netBindTcp and scopeAbstractUnix are both bit
// 0).
enum class [[clang::flag_enum]] Fs : uint32_t {
    none = 0,
    execute = 0x1,
    writeFile = 0x2,
    readFile = 0x4,
    readDir = 0x8,
    removeDir = 0x10,
    removeFile = 0x20,
    makeChar = 0x40,
    makeDir = 0x80,
    makeReg = 0x100,
    makeSock = 0x200,
    makeFifo = 0x400,
    makeBlock = 0x800,
    makeSym = 0x1000,
    refer = 0x2000,        // ABI 2
    truncate = 0x4000,     // ABI 3
    resolveUnix = 0x10000, // ABI 9
};

enum class [[clang::flag_enum]] Net : uint8_t {
    none = 0,
    bindTcp = 0x1, // ABI 4
};

enum class [[clang::flag_enum]] Scope : uint8_t {
    none = 0,
    abstractUnixSocket = 0x1, // ABI 6
    signal = 0x2,
};

template<typename T>
concept Right = std::same_as<T, Fs> || std::same_as<T, Net> || std::same_as<T, Scope>;

template<Right T>
using Bits = std::make_unsigned_t<std::underlying_type_t<T>>;

template<Right T>
constexpr auto bits(T val) -> Bits<T>
{
    return static_cast<Bits<T>>(std::to_underlying(val));
}

template<Right T>
constexpr auto operator|(T lhs, T rhs) -> T
{
    return static_cast<T>(static_cast<Bits<T>>(bits(lhs) | bits(rhs)));
}

template<Right T>
constexpr auto operator&(T lhs, T rhs) -> T
{
    return static_cast<T>(static_cast<Bits<T>>(bits(lhs) & bits(rhs)));
}

template<Right T>
constexpr auto operator~(T val) -> T
{
    return static_cast<T>(static_cast<Bits<T>>(~bits(val)));
}

template<Right T>
constexpr auto operator|=(T & lhs, T rhs) -> T &
{
    return lhs = lhs | rhs;
}

template<Right T>
constexpr auto operator&=(T & lhs, T rhs) -> T &
{
    return lhs = lhs & rhs;
}

constexpr uint32_t createRulesetVersion = 0x1;
enum class RuleType : uint8_t {
    pathBeneath = 1,
    netPort = 2,
};

// Layouts of the kernel's structs, passed by pointer.
struct RulesetAttr
{
    uint64_t handledAccessFs;
    uint64_t handledAccessNet;
    uint64_t scoped;
};

struct [[gnu::packed]] PathBeneathAttr
{
    uint64_t allowedAccess;
    int32_t parentFd;
};

struct NetPortAttr
{
    uint64_t allowedAccess;
    uint64_t port;
};

constexpr size_t word = sizeof(uint64_t);
static_assert(sizeof(RulesetAttr) == 3 * word);
static_assert(sizeof(PathBeneathAttr) == word + sizeof(int32_t));
static_assert(sizeof(NetPortAttr) == 2 * word);

namespace landlock {

// Arguments are passed as full registers, as the kernel reads them.
template<typename... Args>
auto call(long number, Args... args) -> std::expected<long, std::error_code>
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): syscall(2) is vararg.
    auto const res = ::syscall(number, args...);
    if (res < 0) {
        return std::unexpected(std::error_code(errno, std::generic_category()));
    }
    return res;
}

// 0 when the kernel has no Landlock.
auto abiVersion() -> int
{
    return static_cast<int>(call(SYS_landlock_create_ruleset, nullptr, 0, createRulesetVersion).value_or(0));
}

auto createRuleset(const RulesetAttr & attr) -> std::expected<nix::AutoCloseFD, std::error_code>
{
    return call(SYS_landlock_create_ruleset, &attr, sizeof(attr), 0U).transform([](long ruleset) -> auto {
        return nix::AutoCloseFD(static_cast<int>(ruleset));
    });
}

auto addPath(int ruleset, Fs access, int parentFd) -> std::expected<void, std::error_code>
{
    PathBeneathAttr const attr{.allowedAccess = std::to_underlying(access), .parentFd = parentFd};
    return call(SYS_landlock_add_rule, ruleset, RuleType::pathBeneath, &attr, 0U).transform([](long) -> void {});
}

auto addPort(int ruleset, Net access, uint16_t port) -> std::expected<void, std::error_code>
{
    NetPortAttr const attr{.allowedAccess = std::to_underlying(access), .port = port};
    return call(SYS_landlock_add_rule, ruleset, RuleType::netPort, &attr, 0U).transform([](long) -> void {});
}

auto restrictSelf(int ruleset) -> std::expected<void, std::error_code>
{
    return call(SYS_landlock_restrict_self, ruleset, 0U).transform([](long) -> void {});
}

} // namespace landlock

template<typename T>
auto orThrow(std::expected<T, std::error_code> result, const std::string & what) -> T
{
    if (!result) {
        throw std::system_error(result.error(), what);
    }
    if constexpr (!std::is_void_v<T>) {
        return std::move(*result);
    }
}

constexpr Fs readRights = Fs::readFile | Fs::readDir;
constexpr Fs execRights = Fs::execute | readRights;
// Spool files are created and unlinked, the cache dir may need subdirectories.
constexpr Fs writeRights =
    readRights | Fs::writeFile | Fs::removeFile | Fs::removeDir | Fs::makeReg | Fs::makeDir | Fs::truncate | Fs::refer;
// The kernel rejects these on anything but a directory.
constexpr Fs dirOnlyRights = Fs::readDir | Fs::removeDir | Fs::removeFile | Fs::makeChar | Fs::makeDir | Fs::makeReg
                             | Fs::makeSock | Fs::makeFifo | Fs::makeBlock | Fs::makeSym | Fs::refer;

// Directories of the unix sockets every service talks to (sd_notify, journald,
// nscd). Granted by directory because a restarted peer recreates its socket
// file, and a rule on the old inode would no longer match. Enforced from ABI 9.
constexpr auto baseSocketDirs = std::to_array<const char *>({"/run/systemd", "/var/run/nscd"});

// Platform paths may not exist, a path the user configured must.
enum class Presence : bool {
    required,
    optional,
};

class Ruleset
{
public:
    Ruleset(nix::AutoCloseFD ruleset, Fs handledFs)
        : fd_(std::move(ruleset))
        , handledFs_(handledFs)
    {
    }

    void allow(const std::filesystem::path & path, Fs rights, Presence presence = Presence::required) const
    {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): open(2) is vararg.
        nix::AutoCloseFD const target(::open(path.c_str(), O_PATH | O_CLOEXEC));
        if (!target) {
            if (presence == Presence::optional && (errno == ENOENT || errno == ENOTDIR)) {
                return; // e.g. no /run/systemd/resolve without systemd-resolved
            }
            throw nix::SysError("sandbox: opening '%s'", path.string());
        }
        struct stat info{};
        if (::fstat(target.get(), &info) != 0) {
            throw nix::SysError("sandbox: stat '%s'", path.string());
        }
        // Drop what this kernel does not know, so one policy runs on any ABI.
        auto allowed = rights & handledFs_;
        if (!S_ISDIR(info.st_mode)) {
            allowed &= ~dirOnlyRights;
        }
        if (allowed == Fs::none) {
            return;
        }
        orThrow(landlock::addPath(fd_.get(), allowed, target.get()), "sandbox: allowing '" + path.string() + "'");
    }

    void allow(const CanonicalPath & path, Fs rights, Presence presence = Presence::required) const
    {
        allow(path.path(), rights, presence);
    }

    void allowBind(uint16_t port) const
    {
        orThrow(
            landlock::addPort(fd_.get(), Net::bindTcp, port), std::format("sandbox: allowing bind to port {}", port));
    }

    [[nodiscard]] auto ruleset() const -> int { return fd_.get(); }

private:
    nix::AutoCloseFD fd_;
    Fs handledFs_;
};

constexpr int abiRefer = 2;
constexpr int abiTruncate = 3;
constexpr int abiNet = 4;
constexpr int abiScope = 6;
constexpr int abiResolveUnix = 9;

auto handledFor(int abi) -> Fs
{
    // Not handled: ioctl, since terminals and journal sockets use it freely.
    auto handled = Fs::execute | Fs::writeFile | Fs::readFile | Fs::readDir | Fs::removeDir | Fs::removeFile
                   | Fs::makeChar | Fs::makeDir | Fs::makeReg | Fs::makeSock | Fs::makeFifo | Fs::makeBlock
                   | Fs::makeSym;
    if (abi >= abiRefer) {
        handled |= Fs::refer;
    }
    if (abi >= abiTruncate) {
        handled |= Fs::truncate;
    }
    if (abi >= abiResolveUnix) {
        handled |= Fs::resolveUnix;
    }
    return handled;
}

} // namespace

void apply(const Policy & policy)
{
    int const abi = landlock::abiVersion();
    if (abi < 1) {
        throw nix::Error("--sandbox needs Landlock, which this kernel lacks or has disabled");
    }
    // Landlock restricts the calling thread and the ones it creates later.
    std::filesystem::directory_iterator const tasks("/proc/self/task");
    if (auto const threads = std::ranges::distance(tasks, std::filesystem::directory_iterator()); threads != 1) {
        throw nix::Error("sandbox: %d threads already run and would escape the sandbox", threads);
    }

    auto const * const ports = std::get_if<std::vector<uint16_t>>(&policy.listenPorts);
    auto const handledNet = abi >= abiNet && ports != nullptr ? Net::bindTcp : Net::none;
    auto const scoped = abi >= abiScope ? Scope::abstractUnixSocket | Scope::signal : Scope::none;
    auto const handledFs = handledFor(abi);

    Ruleset const ruleset(
        orThrow(
            landlock::createRuleset(
                {.handledAccessFs = std::to_underlying(handledFs),
                 .handledAccessNet = std::to_underlying(handledNet),
                 .scoped = std::to_underlying(scoped)}),
            "sandbox: landlock_create_ruleset"),
        handledFs);

    ruleset.allow(std::filesystem::path("/"), readRights);
    for (auto const & path : policy.execPaths) {
        ruleset.allow(path, execRights);
    }
    for (auto const & path : policy.writePaths) {
        ruleset.allow(path, writeRights);
    }
    // bind() creates the socket file, and a stale one is unlinked first.
    for (auto const & path : policy.listenSockets) {
        ruleset.allow(path.parent(), Fs::makeSock | Fs::removeFile);
    }
    // The peer may not be up yet (socket activation), its directory is.
    for (auto const & path : policy.connectSockets) {
        ruleset.allow(path.parent(), Fs::resolveUnix);
    }
    for (auto const * dir : baseSocketDirs) {
        ruleset.allow(std::filesystem::path(dir), Fs::resolveUnix, Presence::optional);
    }
    if (auto const notify = nix::getEnv("NOTIFY_SOCKET"); notify && notify->starts_with('/')) {
        ruleset.allow(std::filesystem::path(*notify).parent_path(), Fs::resolveUnix, Presence::optional);
    }
    if (handledNet != Net::none) {
        // gRPC probes IPv6 by binding [::1]:0. Without this it concludes there
        // is no IPv6 and a [::] listener silently becomes IPv4-only.
        ruleset.allowBind(0);
        for (auto const port : *ports) {
            ruleset.allowBind(port);
        }
    }

    // Required to restrict without CAP_SYS_ADMIN, and keeps setuid binaries
    // from shedding the sandbox.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
    if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
        throw nix::SysError("sandbox: PR_SET_NO_NEW_PRIVS");
    }
    orThrow(landlock::restrictSelf(ruleset.ruleset()), "sandbox: landlock_restrict_self");
}

} // namespace nixgrpc::sandbox
