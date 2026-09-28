#include "client/plugin-loader-path.hh"

#include <cassert>

using nixgrpc::darwinNixStoreOutputMatches;

auto main() noexcept -> int {
  assert(darwinNixStoreOutputMatches(
      {.outputName = "hash-nix-store-2.31.5", .soversion = "2.31.5"}));
  assert(darwinNixStoreOutputMatches(
      {.outputName = "hash-nix-store-2.31.5+1", .soversion = "2.31.5"}));
  assert(darwinNixStoreOutputMatches(
      {.outputName = "hash-nix-store-2.35pre20260927_c621c2b3",
       .soversion = "2.35"}));
  assert(!darwinNixStoreOutputMatches(
      {.outputName = "hash-nix-store-2.31.50", .soversion = "2.31.5"}));
  assert(!darwinNixStoreOutputMatches(
      {.outputName = "hash-nix-store-2.31.5-debug", .soversion = "2.31.5"}));
}
