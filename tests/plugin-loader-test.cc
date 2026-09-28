#include "client/plugin-loader-path.hh"

#include <cassert>

int main() {
  using nixgrpc::darwinNixStoreOutputMatches;

  assert(darwinNixStoreOutputMatches("hash-nix-store-2.31.5", "2.31.5"));
  assert(darwinNixStoreOutputMatches("hash-nix-store-2.31.5+1", "2.31.5"));
  assert(darwinNixStoreOutputMatches("hash-nix-store-2.35pre20260927_c621c2b3",
                                     "2.35"));
  assert(!darwinNixStoreOutputMatches("hash-nix-store-2.31.50", "2.31.5"));
  assert(!darwinNixStoreOutputMatches("hash-nix-store-2.31.5-debug", "2.31.5"));
}
