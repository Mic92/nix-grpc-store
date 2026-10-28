// Helpers that mirror what nix itself does.

#include <cassert>

#include <nix/store/store-api.hh>

#include "nix-compat.hh"

namespace {

void inputsAreBuiltNormallyUnlessRepairing()
{
    assert(nixcompat::inputBuildMode(nix::bmNormal) == nix::bmNormal);
    assert(nixcompat::inputBuildMode(nix::bmCheck) == nix::bmNormal);
    assert(nixcompat::inputBuildMode(nix::bmRepair) == nix::bmRepair);
}

} // namespace

auto main() -> int
{
    inputsAreBuiltNormallyUnlessRepairing();
    return 0;
}
