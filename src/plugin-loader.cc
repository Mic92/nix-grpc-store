// Dispatcher plugin loaded via nix.conf `plugin-files`. It dlopen()s the
// nix-grpc-store build from ../nix-grpc-store-versions/<soversion>/ whose
// libnixstore is the one already mapped into this process. If no build
// matches, it warns and leaves grpc:// stores unregistered.
//
// Loading a build for any other SONAME would pull a second copy of the
// Nix libraries into the process, which appears to work and then
// double-frees their globals at exit. It must not use any Nix API.

#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <string>
#include <system_error>

#ifdef NIX_GRPC_BLOCKING_SHUTDOWN
#include <grpc/grpc.h>
#endif

namespace {

auto nixStoreSoname(const std::string &soversion) -> std::string {
#ifdef __APPLE__
  return "libnixstore." + soversion + ".dylib";
#else
  return "libnixstore.so." + soversion;
#endif
}

auto hostHas(const std::string &soname) -> bool {
  void *handle = dlopen(soname.c_str(), RTLD_LAZY | RTLD_NOLOAD);
  if (handle == nullptr) {
    return false;
  }
  dlclose(handle);
  return true;
}

#ifdef NIX_GRPC_BLOCKING_SHUTDOWN
class GrpcRuntimeGuard
{
public:
    GrpcRuntimeGuard() { grpc_init(); }
    ~GrpcRuntimeGuard() { grpc_shutdown_blocking(); }

    GrpcRuntimeGuard(const GrpcRuntimeGuard &) = delete;
    GrpcRuntimeGuard(GrpcRuntimeGuard &&) = delete;
    auto operator=(const GrpcRuntimeGuard &) -> GrpcRuntimeGuard & = delete;
    auto operator=(GrpcRuntimeGuard &&) -> GrpcRuntimeGuard & = delete;
};
#endif

// nix::warn() would drag in more of the Nix ABI, so use plain stderr.
void warn(const std::string &message) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  static_cast<void>(std::fprintf(
      stderr, "warning: nix-grpc-store: %s. grpc:// stores are unavailable\n",
      message.c_str()));
}

auto versionsDir() -> std::filesystem::path {
  Dl_info info{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dladdr needs a data pointer
  if (dladdr(reinterpret_cast<void *>(&versionsDir), &info) == 0 ||
      info.dli_fname == nullptr) {
    return {};
  }
  return std::filesystem::path(info.dli_fname).parent_path().parent_path() /
         "nix-grpc-store-versions";
}

auto pluginForRunningNix() -> std::filesystem::path {
  std::error_code ec;
  for (const auto &entry :
       std::filesystem::directory_iterator(versionsDir(), ec)) {
    if (hostHas(nixStoreSoname(entry.path().filename().string()))) {
      return entry.path() / ("nix-grpc-store." NIX_GRPC_MODULE_SUFFIX);
    }
  }
  return {};
}

} // namespace

extern "C" void nix_plugin_entry() {
  std::filesystem::path plugin;
  // NOLINTNEXTLINE(concurrency-mt-unsafe): plugins are loaded before Nix spawns threads
  if (const char *override = std::getenv("NIX_GRPC_PLUGIN_OVERRIDE")) {
    plugin = override;
  } else {
    plugin = pluginForRunningNix();
  }

  if (plugin.empty() || !std::filesystem::exists(plugin)) {
    warn("no plugin build matching the loaded libnixstore in '" +
         versionsDir().string() + "'");
    return;
  }

#ifdef NIX_GRPC_BLOCKING_SHUTDOWN
  // gRPC shutdown is asynchronous by default. Keep one process-wide reference
  // so its final shutdown can block before OpenSSL's process-exit cleanup.
  static const GrpcRuntimeGuard grpcRuntime;
#endif

  // Leaked on purpose: the plugin stays registered for the process lifetime.
  void *handle = dlopen(plugin.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (handle == nullptr) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe): plugins are loaded before Nix spawns threads
    warn("could not load '" + plugin.string() + "': " + dlerror());
    return;
  }
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dlsym returns void*
  auto entry = reinterpret_cast<void (*)()>(dlsym(handle, "nix_plugin_entry"));
  if (entry != nullptr) {
    entry();
  }
}
