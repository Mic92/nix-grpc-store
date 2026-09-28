# nix-fast-build with the grpc:// plugin loaded in every nix it spawns,
# including nix-eval-jobs, which does not go through the `nix` binary.
# nix-fast-build >= 2.0.4 forwards --option to all of them.
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
  postBuild = ''
    rm "$out/bin/nix-fast-build"
    makeBinaryWrapper ${lib.getExe nix-fast-build} "$out/bin/nix-fast-build" \
      --add-flags "--option plugin-files ${plugin}/lib/nix/plugins"
  '';
  meta = nix-fast-build.meta // {
    mainProgram = "nix-fast-build";
  };
}
