{
  description = "Nix store plugin + daemon that tunnel the worker protocol over gRPC";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    nix.url = "github:Mic92/nix-1";
    nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      nix,
    }:
    let
      lib = nixpkgs.lib;
      # Nix versions from nixpkgs that expose split component libraries
      # (`.libs.nix-store`, `.libs.nix-util`) which the plugin builds against.
      supportedNixVersions = [
        "nix_2_31"
        "nix_2_34"
        "nix_2_35"
        "git"
      ];
      forAllSystems = lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.callPackage ./package.nix {
            inherit (nix.packages.${system}) nix-store nix-util;
          };
        }
        # One plugin per supported Nix version; the plugin is ABI-coupled to
        # the Nix that dlopen()s it.
        // lib.listToAttrs (
          map (
            version:
            lib.nameValuePair "plugin-${version}" (
              pkgs.callPackage ./package.nix {
                inherit (pkgs.nixVersions.${version}.libs) nix-store nix-util;
              }
            )
          ) supportedNixVersions
        )
      );

      nixosModules = {
        server = ./nixos/server.nix;
        client = ./nixos/client.nix;
        default.imports = [
          self.nixosModules.server
          self.nixosModules.client
        ];
      };

      checks = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        # Every per-version plugin package doubles as a compile check.
        lib.filterAttrs (name: _: lib.hasPrefix "plugin-" name) self.packages.${system}
        // {
          clang-tidy = self.packages.${system}.default.overrideAttrs (old: {
            pname = "nix-grpc-store-clang-tidy";
            nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.llvmPackages_latest.clang-tools ];
            # Meson generates a clang-tidy target from .clang-tidy; the
            # generated protobuf headers must exist before it runs.
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
            nixPkgs = nix.packages.${system};
            module = self.nixosModules.default;
          };
        }
      );

      devShells = forAllSystems (system: {
        default = nixpkgs.legacyPackages.${system}.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          packages = [ nixpkgs.legacyPackages.${system}.llvmPackages_latest.clang-tools ];
        };
      });
    };
}
