#pragma once
// NOLINTBEGIN(cppcoreguidelines-macro-usage) -- version detection needs macros
// Compatibility helpers for building against multiple Nix versions.
// NIX_COMPAT_VERSION_{MAJOR,MINOR} come from meson (version of the
// `nix-store` pkg-config dependency).

#include <concepts>
#include <cstdint>

// Must be a macro: it is evaluated in #if directives below and in the sources.
#define NIX_COMPAT_AT_LEAST(major, minor)                                     \
  (NIX_COMPAT_VERSION_MAJOR > (major) ||                                      \
   (NIX_COMPAT_VERSION_MAJOR == (major) && NIX_COMPAT_VERSION_MINOR >= (minor)))

#include <string_view>

#include <nix/store/derivations.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/serialise.hh>

// Late 2.36 moved the build operations from Store into a Builder interface.
#if __has_include(<nix/store/build.hh>)
#include <nix/store/build.hh>
#define NIX_COMPAT_HAS_BUILDER 1
#else
#define NIX_COMPAT_HAS_BUILDER 0
#endif

#if __has_include(<nix/store/derivation/aterm.hh>)
#include <nix/store/derivation/aterm.hh>
#define NIX_COMPAT_HAS_DERIVATION_ATERM 1
#else
#define NIX_COMPAT_HAS_DERIVATION_ATERM 0
#endif

// Nix 2.35 added a FilePathType argument to the StoreConfig /
// RemoteStoreConfig constructors.
// Must be a macro: it expands inside constructor member-initializer lists.
#if NIX_COMPAT_AT_LEAST(2, 35)
#define NIX_COMPAT_STORE_CONFIG_ARGS(params) params, FilePathType::Unix
#else
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

// Worker-protocol version 1.37: BasicDerivation, BuildResult and the stderr
// stream framing used by the native BuildDerivation RPC.
constexpr uint32_t kBuildProtocolWire = (1U << 8U) | 37U;

template <typename V = nix::WorkerProto::Version>
constexpr auto protocolWire(const V &version) -> uint32_t {
  if constexpr (std::integral<V>) {
    return version;
  } else {
    constexpr unsigned int minorBits = 8;
    return (version.number.major << minorBits) | version.number.minor;
  }
}

// Nix 2.34 folded the feature set into WorkerProto::Version.
template <typename C>
inline auto handshakeCompat(C &conn, const nix::WorkerProto::Version &version)
    -> nix::WorkerProto::Version {
  if constexpr (std::integral<nix::WorkerProto::Version>) {
    auto [negotiated, features] = C::handshake(conn.to, conn.from, version, {});
    conn.features = features;
    return negotiated;
  } else {
    return C::handshake(conn.to, conn.from, version);
  }
}

// Late 2.36 turned BuildResult into a success/failure variant.
// Non-const: the old success() accessor is not const-qualified.
template <typename R, typename F>
inline void forBuiltOutputs(R &res, const F &fun) {
  if constexpr (requires { res.tryGetSuccess(); }) {
    if (const auto *success = res.tryGetSuccess()) {
      for (const auto &[name, realisation] : success->builtOutputs) {
        fun(realisation.outPath);
      }
    }
  } else {
    if (res.success()) {
      for (const auto &[name, realisation] : res.builtOutputs) {
        fun(realisation.outPath);
      }
    }
  }
}

// Same variant split as forBuiltOutputs.
// Non-const because BuildResult::success() is non-const before Nix 2.32.
template <typename R>
inline auto buildFailureMsg(R &res) -> std::optional<std::string> {
  if constexpr (requires { res.tryGetFailure(); }) {
    if (const auto *failure = res.tryGetFailure()) {
      if constexpr (requires { failure->errorMsg; }) {
        return failure->errorMsg;
      } else {
        return std::string(failure->what());
      }
    }
    return std::nullopt;
  } else {
    if (res.success()) {
      return std::nullopt;
    }
    return res.errorMsg;
  }
}

// Same variant split as forBuiltOutputs.
template <typename R> inline void setAlreadyValid(R &res) {
  if constexpr (requires { res.tryGetSuccess(); }) {
    res.inner = typename R::Success{.status = R::Success::AlreadyValid};
  } else {
    res.status = R::AlreadyValid;
  }
}

inline auto buildProtocolVersion() -> nix::WorkerProto::Version {
  constexpr unsigned int major = 1;
  constexpr uint8_t minor = 37;
  return makeProtocolVersion(major, minor);
}

// Late 2.36 moved the drv wire serialisation into namespace derivation.
#if NIX_COMPAT_HAS_DERIVATION_ATERM
inline void writeDrv(nix::Sink &sink, const nix::StoreDirConfig &store,
                     const nix::BasicDerivation &drv) {
  nix::derivation::write(sink, store, drv);
}
inline void readDrv(nix::Source &source, const nix::StoreDirConfig &store,
                    nix::BasicDerivation &drv, std::string_view name) {
  nix::derivation::read(source, store, drv, name);
}
#else
inline void writeDrv(nix::Sink &sink, const nix::StoreDirConfig &store,
                     const nix::BasicDerivation &drv) {
  nix::writeDerivation(sink, store, drv);
}
inline void readDrv(nix::Source &source, const nix::StoreDirConfig &store,
                    nix::BasicDerivation &drv, std::string_view name) {
  nix::readDerivation(source, store, drv, name);
}
#endif

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
// NOLINTEND(cppcoreguidelines-macro-usage)
