#pragma once

#include <string>
#include <string_view>

namespace nixgrpc {

inline auto darwinNixStoreOutputMatches(std::string_view outputName,
                                        std::string_view soversion) -> bool {
  const auto marker = "-nix-store-" + std::string(soversion);
  const auto versionStart = outputName.rfind(marker);
  if (versionStart == std::string_view::npos) {
    return false;
  }
  const auto suffix = outputName.substr(versionStart + marker.size());
  return suffix.empty() || suffix.starts_with('+') || suffix.starts_with("pre");
}

} // namespace nixgrpc
