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
      forAllSystems = nixpkgs.lib.genAttrs [
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
        {
          clang-tidy = self.packages.${system}.default.overrideAttrs (old: {
            pname = "nix-grpc-store-clang-tidy";
            nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.clang-tools ];
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
        // nixpkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
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
          packages = [ nixpkgs.legacyPackages.${system}.clang-tools ];
        };
      });
    };
}
