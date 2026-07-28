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
  vm = import ./tests/nixos-test.nix {
    inherit pkgs;
    nixPkgs = nixPackages;
    module = nixosModule;
  };
}
