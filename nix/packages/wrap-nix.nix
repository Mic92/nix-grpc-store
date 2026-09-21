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
  plugin = callPackage ./plugin.nix { inherit (nix.libs) nix-store nix-util; };
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
  postBuild = ''
    for f in "$out"/bin/*; do
      name=$(basename "$f")
      rm "$f"
      makeBinaryWrapper ${lib.getExe' nix "nix"} "$f" --argv0 "$name" \
        --add-flags "--option plugin-files ${plugin}/lib/nix/plugins"
    done
  '';
  meta = removeAttrs nix.meta [ "outputsToInstall" ] // {
    mainProgram = "nix";
  };
}
