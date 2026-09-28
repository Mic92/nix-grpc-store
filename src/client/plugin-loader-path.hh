#pragma once

#include <string_view>

namespace nixgrpc {

struct DarwinNixStoreMatch {
  std::string_view outputName;
  std::string_view soversion;
};

inline auto darwinNixStoreOutputMatches(DarwinNixStoreMatch match) noexcept
    -> bool {
  constexpr std::string_view marker = "-nix-store-";
  const auto versionStart = match.outputName.rfind(marker);
  if (versionStart == std::string_view::npos) {
    return false;
  }
  auto version = match.outputName;
  version.remove_prefix(versionStart + marker.size());
  if (!version.starts_with(match.soversion)) {
    return false;
  }
  auto suffix = version;
  suffix.remove_prefix(match.soversion.size());
  return suffix.empty() || suffix.starts_with('+') || suffix.starts_with("pre");
}

} // namespace nixgrpc
