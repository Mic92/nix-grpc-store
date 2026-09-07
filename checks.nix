{
  pkgs,
  packages,
  nixPackages,
  nixosModule,
  niks3,
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
  # Model of the worker/niks3 claim protocol, see spec/claims.qnt.
  claims-spec = pkgs.runCommand "nix-grpc-store-claims-spec" { nativeBuildInputs = [ pkgs.quint ]; } ''
    cd ${./spec}
    export HOME=$TMPDIR
    quint typecheck claims.qnt
    quint run claims.qnt --invariant=safety --max-steps=30 --max-samples=20000
    quint run claims.qnt --step=stepBlips --invariant=oneBuilder --max-steps=30 --max-samples=20000
    touch $out
  '';
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

  farm = import ./tests/farm.nix {
    inherit pkgs niks3;
    nixPkgs = nixPackages;
    module = nixosModule;
  };
}
