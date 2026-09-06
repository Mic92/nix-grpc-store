#pragma once
// NOLINTBEGIN(cppcoreguidelines-macro-usage) -- version detection needs macros
// Compatibility helpers for building against multiple Nix versions.
// NIX_COMPAT_VERSION_{MAJOR,MINOR} come from meson (version of the
// `nix-store` pkg-config dependency).

#include <concepts>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <variant>

// Must be a macro: it is evaluated in #if directives below and in the sources.
#define NIX_COMPAT_AT_LEAST(major, minor)                                     \
  (NIX_COMPAT_VERSION_MAJOR > (major) ||                                      \
   (NIX_COMPAT_VERSION_MAJOR == (major) && NIX_COMPAT_VERSION_MINOR >= (minor)))

#include <string_view>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/realisation.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/serialise.hh>

// Late 2.36 moved the build operations from Store into a Builder interface.
#if __has_include(<nix/store/build.hh>)
#include <nix/store/build.hh>
#define NIX_COMPAT_HAS_BUILDER 1
#else
#define NIX_COMPAT_HAS_BUILDER 0
#endif

// Late 2.36 templated Derivation over its inputs: `inputs` replaces
// `inputSrcs`/`inputDrvs` and tryResolve became a free function.
#if __has_include(<nix/store/derivation/resolution.hh>)
#define NIX_COMPAT_TEMPLATED_DRV 1
#else
#define NIX_COMPAT_TEMPLATED_DRV 0
#endif
#include <nix/store/derivation-options.hh>

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

// BuildResult: flat struct with `status` before 2.32, variant of
// Success/Failure(BuildError) since.
// Non-const because BuildResult::success() is non-const before Nix 2.32.
template <typename R = nix::BuildResult>
inline auto succeeded(R &res) -> bool {
  if constexpr (requires { res.tryGetSuccess(); }) {
    return res.tryGetSuccess() != nullptr;
  } else {
    return res.success();
  }
}

#if NIX_COMPAT_AT_LEAST(2, 32)
using FailureStatus = nix::BuildResult::Failure::Status;
#else
using FailureStatus = nix::BuildResult::Status;
#endif

inline auto failed(FailureStatus status, const std::string &msg) -> nix::BuildResult {
  nix::BuildResult res;
#if NIX_COMPAT_AT_LEAST(2, 32)
  nix::BuildResult::Failure failure(status, "%s", msg);
  res.inner = std::move(failure);
#else
  res.status = status;
  res.errorMsg = msg;
#endif
  return res;
}

// AlreadyValid with builtOutputs filled in, which `nix build` reads to map
// results to paths. Only outPath matters to it. The DrvOutput key changed
// from a hash to the drv path in 2.35.
inline auto alreadyValid(const nix::StorePath &drvPath,
                         std::map<std::string, nix::StorePath> outputs) -> nix::BuildResult {
  nix::BuildResult res;
  setAlreadyValid(res);
#if NIX_COMPAT_AT_LEAST(2, 32)
  auto &built = res.tryGetSuccess()->builtOutputs;
#else
  auto &built = res.builtOutputs;
#endif
  for (auto &[name, path] : outputs) {
#if NIX_COMPAT_AT_LEAST(2, 35)
    nix::DrvOutput key{.drvPath = drvPath, .outputName = name};
#else
    (void)drvPath;
    nix::DrvOutput key{.drvHash = nix::Hash(nix::HashAlgorithm::SHA256), .outputName = name};
#endif
#if NIX_COMPAT_AT_LEAST(2, 34)
    built.emplace(name, nix::Realisation{{.outPath = std::move(path)}, std::move(key)});
#else
    built.emplace(name, nix::Realisation{.id = std::move(key), .outPath = std::move(path)});
#endif
  }
  return res;
}

// Failure kinds worth telling other workers' waiters about.
inline auto deterministicFailureKind(nix::BuildResult &res) -> std::string {
  std::optional<FailureStatus> status;
#if NIX_COMPAT_AT_LEAST(2, 32)
  if (const auto *failure = res.tryGetFailure()) {
    status = failure->status;
  }
#else
  if (!succeeded(res)) {
    status = res.status;
  }
#endif
  if (!status) {
    return "";
  }
  switch (*status) {
  case FailureStatus::PermanentFailure:
    return "PermanentFailure";
  case FailureStatus::OutputRejected:
    return "OutputRejected";
  case FailureStatus::TimedOut:
    return "TimedOut";
  case FailureStatus::LogLimitExceeded:
    return "LogLimitExceeded";
  default:
    return "";
  }
}

// Plain store path inputs of a BasicDerivation.
inline auto drvInputs(const nix::BasicDerivation &drv) -> const nix::StorePathSet & {
#if NIX_COMPAT_TEMPLATED_DRV
  return drv.inputs;
#else
  return drv.inputSrcs;
#endif
}

// What nix's DerivationGoal hands a builder: the BasicDerivation with each
// input derivation output replaced by its store path. `outputOf(drvPath,
// outputName)` supplies those. Returns nullopt for dynamic derivation inputs.
template <typename OutputOf>
auto toBasicDrv(const nix::Derivation &drv, const OutputOf &outputOf)
    -> std::optional<nix::BasicDerivation> {
  bool dynamic = false;
#if NIX_COMPAT_TEMPLATED_DRV
  auto basic = drv.mapInputs([&](const std::set<nix::SingleDerivedPath> &inputs) -> nix::StorePathSet {
    nix::StorePathSet srcs;
    for (const auto &input : inputs) {
      if (const auto *opaque = std::get_if<nix::SingleDerivedPath::Opaque>(&input.raw())) {
        srcs.insert(opaque->path);
        continue;
      }
      const auto &built = std::get<nix::SingleDerivedPath::Built>(input.raw());
      const auto *dep = std::get_if<nix::SingleDerivedPath::Opaque>(&built.drvPath->raw());
      if (dep == nullptr) {
        dynamic = true;
        continue;
      }
      srcs.insert(outputOf(dep->path, built.output));
    }
    return srcs;
  });
#else
  nix::BasicDerivation basic(static_cast<const nix::BasicDerivation &>(drv));
  for (const auto &[depPath, node] : drv.inputDrvs.map) {
    if (!node.childMap.empty()) {
      dynamic = true;
      continue;
    }
    for (const auto &output : node.value) {
      basic.inputSrcs.insert(outputOf(depPath, output));
    }
  }
#endif
  if (dynamic) {
    return std::nullopt;
  }
  return basic;
}

// Every input derivation path (static ones only, see toBasicDrv).
template <typename F> void forInputDrvs(const nix::Derivation &drv, const F &visit) {
#if NIX_COMPAT_TEMPLATED_DRV
  for (const auto &input : drv.inputs) {
    if (const auto *built = std::get_if<nix::SingleDerivedPath::Built>(&input.raw())) {
      visit(built->drvPath->getBaseStorePath());
    }
  }
#else
  for (const auto &[drvPath, node] : drv.inputDrvs.map) {
    (void)node;
    visit(drvPath);
  }
#endif
}

inline auto requiredSystemFeatures(const nix::StoreDirConfig &store,
                                   const nix::BasicDerivation &drv) -> nix::StringSet {
  const auto *attrs = drv.structuredAttrs ? &*drv.structuredAttrs : nullptr;
#if NIX_COMPAT_AT_LEAST(2, 34)
  auto options = nix::derivationOptionsFromStructuredAttrs(store, drv.env, attrs, /*shouldWarn=*/false);
#else
  (void)store;
  auto options = nix::DerivationOptions::fromStructuredAttrs(drv.env, attrs, /*shouldWarn=*/false);
#endif
  return options.getRequiredSystemFeatures(drv);
}

// Substitute `path` if it is not valid.
inline void ensurePath(nix::Store &store, const nix::StorePath &path) {
#if NIX_COMPAT_HAS_BUILDER
  store.getBuilder()->ensurePath(path);
#else
  store.ensurePath(path);
#endif
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
// NOLINTEND(cppcoreguidelines-macro-usage)
