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
          scope = nixpkgs.legacyPackages.${system}.callPackage ./packages.nix {
            nixPackages = nix.packages.${system};
          };
        in
        {
          inherit (scope) default plugin-dispatcher;
        }
        // scope.versionPlugins
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
        import ./checks.nix {
          pkgs = nixpkgs.legacyPackages.${system};
          packages = self.packages.${system};
          nixPackages = nix.packages.${system};
          nixosModule = self.nixosModules.default;
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
