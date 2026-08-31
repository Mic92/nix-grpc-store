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
  exit-stress = import ./tests/exit-stress.nix {
    inherit pkgs;
    nix = nixPackages.nix-everything;
    package = packages.default;
  };
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

  # Smoke run so the fuzz targets keep compiling and do not crash on an
  # empty input; real campaigns run locally via scripts/fuzz.sh.
  fuzz = pkgs.runCommand "nix-grpc-store-fuzz-smoke" { } ''
    for f in ${packages.fuzzers}/bin/fuzz-*; do
      "$f" -runs=200 2>&1 | tail -n2
    done
    touch $out
  '';

  # Exercises the README ACME/step-ca substituter example.
  acme-vm = import ./tests/acme-substituter-test.nix {
    inherit pkgs;
    nixPkgs = nixPackages;
    module = nixosModule;
  };

  vm = import ./tests/nixos-test.nix {
    inherit pkgs;
    nixPkgs = nixPackages;
    module = nixosModule;
  };
}
