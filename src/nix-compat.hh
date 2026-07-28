#pragma once
// Compatibility helpers for building against multiple Nix versions.
// NIX_COMPAT_VERSION_{MAJOR,MINOR} come from meson (version of the
// `nix-store` pkg-config dependency).

#include <concepts>
#include <cstdint>

#include <nix/store/worker-protocol.hh>
#include <nix/util/serialise.hh>

// Must be a macro: it is evaluated in #if directives below and in the sources.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define NIX_COMPAT_AT_LEAST(major, minor)                                     \
  (NIX_COMPAT_VERSION_MAJOR > (major) ||                                      \
   (NIX_COMPAT_VERSION_MAJOR == (major) && NIX_COMPAT_VERSION_MINOR >= (minor)))

// Nix 2.35 added a FilePathType argument to the StoreConfig /
// RemoteStoreConfig constructors.
// Must be a macro: it expands inside constructor member-initializer lists.
#if NIX_COMPAT_AT_LEAST(2, 35)
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define NIX_COMPAT_STORE_CONFIG_ARGS(params) params, FilePathType::Unix
#else
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define NIX_COMPAT_STORE_CONFIG_ARGS(params) params
#endif

namespace nixcompat {

// Nix 2.34 turned WorkerProto::Version from a packed unsigned int into a
// struct; detect the representation instead of checking version numbers.
template <typename V = nix::WorkerProto::Version>
constexpr auto makeProtocolVersion(unsigned int major, uint8_t minor) -> V {
  if constexpr (std::integral<V>) {
    constexpr unsigned int minorBits = 8; // wire format: (major << 8) | minor
    return (major << minorBits) | minor;
  } else {
    return V{.number = {.major = major, .minor = minor}};
  }
}

// Worker-protocol version 1.16: the ValidPathInfo framing used by the bulk
// RPCs.
inline auto infoProtocolVersion() -> nix::WorkerProto::Version {
  constexpr unsigned int major = 1;
  constexpr uint8_t minor = 16;
  return makeProtocolVersion(major, minor);
}

// Nix 2.35 added EnsureRead, which turns a short NAR read into an error
// instead of silently truncating. Older versions read the NAR unguarded.
#if NIX_COMPAT_AT_LEAST(2, 35)
using EnsureRead = nix::EnsureRead;
#else
class EnsureRead : public nix::Source {
  nix::Source &inner;

public:
  EnsureRead(nix::Source &source, uint64_t /*bytesExpected*/) : inner(source) {}
  auto read(char *data, size_t len) -> size_t override {
    return inner.read(data, len);
  }
  void finish() {}
};
#endif

} // namespace nixcompat
