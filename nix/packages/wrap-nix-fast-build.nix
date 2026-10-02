# nix-fast-build with the grpc:// plugin loaded in every nix it spawns,
# including nix-eval-jobs, which does not go through the `nix` binary.
{
  lib,
  symlinkJoin,
  makeBinaryWrapper,
  nix-fast-build,
  plugin,
}:
symlinkJoin {
  pname = "nix-fast-build-with-grpc-store";
  inherit (nix-fast-build) version;
  paths = [ nix-fast-build ];
  nativeBuildInputs = [ makeBinaryWrapper ];
  passthru = { inherit plugin; };
  # NIX_CONFIG rather than --option: it reaches nix-eval-jobs and, unlike
  # flags, the `nix __build-remote` hook nix spawns for grpc:// builders.
  postBuild = ''
    rm "$out/bin/nix-fast-build"
    makeBinaryWrapper ${lib.getExe nix-fast-build} "$out/bin/nix-fast-build" \
      --suffix NIX_CONFIG $'\n' "plugin-files = ${plugin}/lib/nix/plugins"
  '';
  meta = nix-fast-build.meta // {
    mainProgram = "nix-fast-build";
  };
}
