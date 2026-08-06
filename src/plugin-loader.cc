// Dispatcher plugin loaded via nix.conf `plugin-files`. It dlopen()s the
// nix-grpc-store build matching the running Nix version from
// ../nix-grpc-store-versions/<major.minor>/ next to itself. If no build
// matches, it warns and leaves grpc:// stores unregistered.
//
// It must not use any Nix API beyond the version string.

#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <string>
#include <string_view>

namespace nix {
// Resolved from the host `nix` process at dlopen() time.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern std::string nixVersion;
} // namespace nix

namespace {

// "2.31.5" / "2.35pre20260619_f8bb823a" -> "2.31" / "2.35"
auto majorMinor(std::string_view version) -> std::string_view {
  auto firstDot = version.find('.');
  auto end = version.find_first_not_of("0123456789", firstDot + 1);
  return version.substr(0, end);
}

// nix::warn() would drag in more of the Nix ABI, so use plain stderr.
void warn(const std::string &message) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  static_cast<void>(std::fprintf(
      stderr, "warning: nix-grpc-store: %s. grpc:// stores are unavailable\n",
      message.c_str()));
}

// <this module's directory>/../nix-grpc-store-versions/<major.minor>/nix-grpc-store.<module suffix>
auto pluginForRunningNix() -> std::filesystem::path {
  Dl_info info{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dladdr needs a data pointer
  if (dladdr(reinterpret_cast<void *>(&pluginForRunningNix), &info) == 0 ||
      info.dli_fname == nullptr) {
    return {};
  }
  return std::filesystem::path(info.dli_fname).parent_path().parent_path() /
         "nix-grpc-store-versions" / majorMinor(nix::nixVersion) /
         ("nix-grpc-store." NIX_GRPC_MODULE_SUFFIX);
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
    warn("no plugin build for Nix " + nix::nixVersion);
    return;
  }

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
