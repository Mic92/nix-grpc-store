{
  pkgs,
  packages,
  nixPackages,
  nixosModule,
  niks3,
}:
let
  inherit (pkgs) lib;
  k3s = import ../tests/k3s.nix {
    inherit pkgs niks3;
    chart = ../deploy/helm/nix-grpc-farm;
    dockerWorker = packages.docker;
    dockerLb = packages.docker-lb;
    dockerClient = packages.docker-client;
    nixPkgs = nixPackages;
    clientModule = nixosModule;
  };
  # Drop once NixOS/nixpkgs#563394 reaches nixos-unstable.
  clangTools = pkgs.llvmPackages_22.clang-tools.overrideAttrs (old: {
    buildInputs = (old.buildInputs or [ ]) ++ [ pkgs.bash ];
  });
in
# Every per-version plugin package doubles as a compile check.
lib.filterAttrs (name: _: lib.hasPrefix "plugin-" name) packages
// {
  # A grpc:// URL is only understood when the plugin registered the scheme.
  nix-with-plugin = pkgs.runCommand "nix-with-plugin-check" { } ''
    HOME=$TMPDIR ${packages.nix-with-plugin}/bin/nix --extra-experimental-features nix-command \
      store info --store 'grpc://127.0.0.1:1?insecure=1' 2>log || true
    grep -q "gRPC StoreInfo" log
    touch $out
  '';
  # LLVM 23 is not in the darwin cache yet.
  # Same clang as clang-tidy so compile_commands carry flags it understands.
  # No PCH: the cc-wrapper's hardening flags are not in compile_commands, so
  # clang-tidy could not load it.
  clang-tidy = (packages.default.override { stdenv = pkgs.llvmPackages_22.stdenv; }).overrideAttrs (old: {
    pname = "nix-grpc-store-clang-tidy";
    separateDebugInfo = false;
    nativeBuildInputs = old.nativeBuildInputs ++ [ clangTools ];
    mesonFlags = (old.mesonFlags or [ ]) ++ [ "-Db_pch=false" ];
    # Meson generates a clang-tidy target from .clang-tidy. The generated
    # protobuf headers must exist before it runs.
    buildPhase = ''
      ninja nix_remote.pb.h nix_remote.grpc.pb.h eds.pb.h eds.grpc.pb.h
      ninja clang-tidy
    '';
    installPhase = "touch $out";
    doCheck = false;
    dontFixup = true;
  });
  spec = pkgs.runCommand "nix-grpc-store-spec" { nativeBuildInputs = [ pkgs.quint ]; } ''
    cd ${../spec}
    export HOME=$TMPDIR
    quint typecheck scheduler.qnt
    quint typecheck push.qnt
    quint run scheduler.qnt --main schedFixed --invariant=safety --max-steps=30 --max-samples=30000
    ! quint run scheduler.qnt --main schedNoRevoke --invariant=safety --max-steps=30 --max-samples=30000
    ! quint run scheduler.qnt --main schedNoFence --invariant=safety --max-steps=30 --max-samples=30000
    ! quint run scheduler.qnt --main schedNoPush --invariant=safety --max-steps=30 --max-samples=30000
    ! quint run scheduler.qnt --main schedNoReport --invariant=safety --max-steps=30 --max-samples=30000
    ! quint run scheduler.qnt --main schedTerminal --invariant=safety --max-steps=30 --max-samples=30000
    quint run push.qnt --main pushFixed --invariant=safety --max-steps=12 --max-samples=20000
    touch $out
  '';
  exit-stress = import ../tests/exit-stress.nix {
    inherit pkgs;
    nix = nixPackages.nix-everything;
    package = packages.default;
  };
}
// lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
  sanitize-smoke = import ../tests/sanitize-smoke.nix {
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
  # empty input. Real campaigns run locally via scripts/fuzz.sh.
  fuzz = pkgs.runCommand "nix-grpc-store-fuzz-smoke" { } ''
    for f in ${packages.fuzzers}/bin/fuzz-*; do
      "$f" -runs=200 2>&1 | tail -n2
    done
    touch $out
  '';

  # Exercises the README ACME/step-ca substituter example.
  acme-vm = import ../tests/acme-substituter-test.nix {
    inherit pkgs;
    nixPkgs = nixPackages;
    module = nixosModule;
  };

  vm = import ../tests/nixos-test.nix {
    mockOidc = niks3.packages.${pkgs.stdenv.hostPlatform.system}.mock-oidc-server;
    inherit pkgs;
    nixPkgs = nixPackages;
    module = nixosModule;
  };

  helm = import ../tests/helm.nix {
    inherit pkgs;
    chart = ../deploy/helm/nix-grpc-farm;
    lbModule = ../nixos/lb.nix;
    extraValues = [ k3s.chartValues ];
  };

  farm = import ../tests/farm.nix {
    inherit pkgs niks3;
    mockOidc = niks3.packages.${pkgs.stdenv.hostPlatform.system}.mock-oidc-server;
    nixPkgs = nixPackages;
    module = nixosModule;
  };
}
// lib.optionalAttrs (pkgs.stdenv.hostPlatform.system == "x86_64-linux") { inherit k3s; }
