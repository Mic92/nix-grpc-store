# harmonia-gc for the worker image, pinned in harmonia-gc.json
# (scripts/update-harmonia-gc.sh).
{
  lib,
  fetchFromGitHub,
  rustPlatform,
}:
let
  pin = lib.importJSON ./harmonia-gc.json;
in
rustPlatform.buildRustPackage {
  pname = "harmonia-gc";
  version = "0-unstable-${builtins.substring 0 7 pin.rev}";
  src = fetchFromGitHub {
    owner = "nix-community";
    repo = "harmonia";
    inherit (pin) rev hash;
  };
  inherit (pin) cargoHash;
  cargoBuildFlags = [ "-p" "harmonia-store-gc" ];
  cargoTestFlags = [ "-p" "harmonia-store-gc" ];
  # Tests want a writable store and user namespaces.
  doCheck = false;
  meta = {
    description = "Fast Nix garbage collector with --ensure-free";
    homepage = "https://github.com/nix-community/harmonia";
    license = lib.licenses.mit;
    mainProgram = "harmonia-gc";
    platforms = lib.platforms.linux;
  };
}
