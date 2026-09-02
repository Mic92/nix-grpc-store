{
  description = "Nix store plugin + daemon that tunnel the worker protocol over gRPC";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    {
      self,
      nixpkgs,
    }:
    let
      lib = nixpkgs.lib;
      # The default build and the VM tests track NixOS/nix master. nixpkgs'
      # nixVersions.git lags behind, so pin master in nix-git.json (bumped
      # weekly by the update-nix-git effect) to see API breakage here first.
      nixGitPin = lib.importJSON ./nix-git.json;
      nixGitFor =
        pkgs:
        ((pkgs.nixVersions.nixComponents_git.overrideSource (
          pkgs.fetchFromGitHub {
            inherit (nixGitPin)
              owner
              repo
              rev
              hash
              ;
          }
        )).overrideScope
          (
            _final: prev: {
              inherit (nixGitPin) version;
              # NixOS/nix#16399, drop once merged.
              patches = prev.patches ++ [ ./patches/nix-readerror-level.patch ];
              # The eval cache moved into libexpr. nixpkgs' packaging lags.
              nix-expr = prev.nix-expr.overrideAttrs (old: {
                buildInputs = old.buildInputs ++ [ pkgs.sqlite ];
              });
            }
          )
        ).nix-everything;
      nixPackagesFor =
        pkgs:
        let
          nix-everything = nixGitFor pkgs;
        in
        {
          inherit (nix-everything.libs) nix-store nix-util;
          inherit nix-everything;
        };
      packageSetFor =
        pkgs:
        pkgs.callPackage ./packages.nix {
          nixPackages = nixPackagesFor pkgs;
        };
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
          scope = packageSetFor nixpkgs.legacyPackages.${system};
        in
        {
          inherit (scope)
            default
            plugin-dispatcher
            fuzzers
            fuzzers-coverage
            ;
        }
        // scope.versionPlugins
        // lib.optionalAttrs (lib.hasSuffix "-linux" system) {
          # Benchmarks, intentionally not in `checks` so CI skips them.
          bench-closure = nixpkgs.legacyPackages.${system}.callPackage ./tests/bench-closure.nix { };
          bench-latency = import ./tests/latency-test.nix {
            pkgs = nixpkgs.legacyPackages.${system};
            nixPkgs = nixPackagesFor nixpkgs.legacyPackages.${system};
            module = self.nixosModules.default;
          };
        }
      );

      nixosModules = {
        server = ./nixos/server.nix;
        # Reuse the flake's package set so hosts get the same derivations as
        # `nix build` instead of rebuilding the plugins per machine.
        client =
          { pkgs, ... }:
          {
            imports = [ ./nixos/client.nix ];
            programs.nix-grpc-store.packageSet = lib.mkDefault (packageSetFor pkgs);
          };
        default.imports = [
          self.nixosModules.server
          self.nixosModules.client
        ];
      };

      herculesCI = import ./effects.nix { inherit nixpkgs; };

      checks = forAllSystems (
        system:
        import ./checks.nix {
          pkgs = nixpkgs.legacyPackages.${system};
          packages = self.packages.${system};
          nixPackages = nixPackagesFor nixpkgs.legacyPackages.${system};
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
