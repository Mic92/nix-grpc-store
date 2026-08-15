{
  pkgs,
  packages,
  nixPackages,
  nixosModule,
}:
let
  inherit (pkgs) lib;
in
# Every per-version plugin package doubles as a compile check.
lib.filterAttrs (name: _: lib.hasPrefix "plugin-" name) packages
// {
  clang-tidy = packages.default.overrideAttrs (old: {
    pname = "nix-grpc-store-clang-tidy";
    nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.llvmPackages_latest.clang-tools ];
    # Meson generates a clang-tidy target from .clang-tidy. The generated
    # protobuf headers must exist before it runs.
    buildPhase = ''
      ninja nix_remote.pb.h nix_remote.grpc.pb.h
      ninja clang-tidy
    '';
    installPhase = "touch $out";
    doCheck = false;
    dontFixup = true;
  });
}
// lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
  sanitize-smoke = import ./tests/sanitize-smoke.nix {
    inherit pkgs;
    nix = nixPackages.nix-everything;
    package = packages.default.overrideAttrs (old: {
      pname = "nix-grpc-store-asan";
      mesonFlags = (old.mesonFlags or [ ]) ++ [
        "-Db_sanitize=address,undefined"
        "-Db_lundef=false"
      ];
      hardeningDisable = [ "fortify" ];
    });
  };

  # Quick libFuzzer run over the zstd chunk decoder (peer-controlled input).
  fuzz = (packages.default.override { stdenv = pkgs.clangStdenv; }).overrideAttrs (old: {
    pname = "nix-grpc-store-fuzz";
    mesonFlags = (old.mesonFlags or [ ]) ++ [
      "-Dfuzzers=true"
      "-Db_sanitize=address,undefined"
      "-Db_lundef=false"
    ];
    hardeningDisable = [ "fortify" ];
    nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.zstd ];
    installPhase = ''
      mkdir corpus
      printf 'seed' | zstd -o corpus/valid.zst
      ASAN_OPTIONS=detect_leaks=0 ./fuzz-zstd-reader -max_total_time=30 corpus
      touch $out
    '';
    doCheck = false;
    dontFixup = true;
  });

  vm = import ./tests/nixos-test.nix {
    inherit pkgs;
    nixPkgs = nixPackages;
    module = nixosModule;
  };
}
