# `nix` with the grpc:// plugin built against it and loaded, for images and
# hosts where nix.conf is not under our control.
{
  lib,
  callPackage,
  symlinkJoin,
  makeBinaryWrapper,
}:
nix:
let
  plugin = callPackage ./nix-grpc-store.nix {
    inherit (nix.libs) nix-store nix-util;
    components = "plugin";
  };
in
symlinkJoin {
  pname = "nix-with-grpc-store";
  inherit (nix) version;
  paths = [ nix ];
  nativeBuildInputs = [ makeBinaryWrapper ];
  passthru = {
    inherit plugin nix;
    inherit (nix) libs;
  };
  # The legacy commands are symlinks to nix and dispatch on argv[0].
  # NIX_CONFIG rather than --add-flags: the `nix __build-remote` hook nix
  # spawns for grpc:// builders inherits the environment, not our flags.
  postBuild = ''
    for f in "$out"/bin/*; do
      name=$(basename "$f")
      rm "$f"
      makeBinaryWrapper ${lib.getExe' nix "nix"} "$f" --argv0 "$name" \
        --suffix NIX_CONFIG $'\n' "plugin-files = ${plugin}/lib/nix/plugins"
    done
  '';
  meta = removeAttrs nix.meta [ "outputsToInstall" ] // {
    mainProgram = "nix";
  };
}
