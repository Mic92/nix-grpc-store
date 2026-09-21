{
  description = "Nix store plugin + daemon that tunnel the worker protocol over gRPC";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Build farm mode (PLAN.md). Only the NixOS test uses it.
    niks3.url = "github:Mic92/niks3";
    niks3.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      niks3,
    }:
    let
      lib = nixpkgs.lib;
      # The default build and the VM tests track NixOS/nix master. nixpkgs'
      # nixVersions.git lags behind, so pin master in nix-git.json (bumped
      # weekly by the update-nix-git effect) to see API breakage here first.
      nixGitPin = lib.importJSON ./nix-git.json;
      nixGitFor =
        pkgs:
        (
          (pkgs.nixVersions.nixComponents_git.overrideSource (
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
        pkgs.callPackage ./nix/packages {
          nixPackages = nixPackagesFor pkgs;
          niks3 = niks3.packages.${pkgs.stdenv.hostPlatform.system}.niks3 or null;
          # For the multi-arch image merge.
          scopeFor = system: packageSetFor nixpkgs.legacyPackages.${system};
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
            nix-with-plugin
            jwt-cpp
            plugin-dispatcher
            fuzzers
            fuzzers-coverage
            ;
          # The nix the default plugin is built against, for manual testing.
          nix = nixGitFor nixpkgs.legacyPackages.${system};
        }
        // scope.versionPlugins
        // lib.optionalAttrs (lib.hasSuffix "-linux" system) {
          inherit (scope)
            harmonia-gc
            docker
            docker-lb
            docker-client
            docker-multiarch
            docker-lb-multiarch
            docker-client-multiarch
            ;
          # Benchmarks, intentionally not in `checks` so CI skips them.
          bench-closure = nixpkgs.legacyPackages.${system}.callPackage ./tests/bench-closure.nix { };
          bench-latency = import ./tests/latency-test.nix {
            pkgs = nixpkgs.legacyPackages.${system};
            nixPkgs = nixPackagesFor nixpkgs.legacyPackages.${system};
            module = self.nixosModules.default;
          };
        }
      );

      dashboards.farm = ./deploy/helm/nix-grpc-farm/files/farm.json;

      # nix-grpc-store.lib.wrapNix pkgs pkgs.nix: that nix with the plugin loaded.
      lib.wrapNix = pkgs: (packageSetFor pkgs).wrapNix;

      nixosModules = {
        server = ./nixos/server.nix;
        lb = ./nixos/lb.nix;
        client = ./nixos/client.nix;
        default.imports = [
          self.nixosModules.server
          self.nixosModules.client
          self.nixosModules.lb
        ];
      };

      herculesCI = import ./nix/effects.nix { inherit nixpkgs; };

      checks = forAllSystems (
        system:
        import ./nix/checks.nix {
          pkgs = nixpkgs.legacyPackages.${system};
          packages = self.packages.${system};
          nixPackages = nixPackagesFor nixpkgs.legacyPackages.${system};
          nixosModule = self.nixosModules.default;
          inherit niks3;
        }
      );

      devShells = forAllSystems (system: {
        default = nixpkgs.legacyPackages.${system}.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          packages = [
            nixpkgs.legacyPackages.${system}.llvmPackages_latest.clang-tools
            (nixGitFor nixpkgs.legacyPackages.${system})
          ];
        };
      });
    };
}
