# Package set with one plugin per supported Nix version plus the dispatcher bundle.
{
  lib,
  newScope,
  nixVersions,
  clangStdenv,
  symlinkJoin,
  # Nix package set the default plugin builds against.
  nixPackages,
  # Nix the images ship: nixpkgs' release, not the master pin the tests track.
  nix,
  nix-eval-jobs,
  nix-fast-build,
  niks3 ? null,
  scopeFor ? null,
}:
# Clang builds this with half the memory of GCC, whose PCH step alone needs
# 1.3 GB.
lib.makeScope (extra: newScope ({ stdenv = clangStdenv; } // extra)) (
  self:
  let
    linuxSystems = [
      "x86_64-linux"
      "aarch64-linux"
    ];
    # Nix releases from nixpkgs that ship split component libraries.
    # 2.31 is the oldest release with ParsedURL::Authority and
    # StoreConfig::getReference(), which the plugin relies on.
    supportedNixVersions = builtins.filter (
      name:
      builtins.match "nix_[0-9]+_[0-9]+" name != null
      && (builtins.tryEval (
        lib.versionAtLeast nixVersions.${name}.version "2.31" && (nixVersions.${name}.libs or { }) ? nix-store
      )).value or false
    ) (builtins.attrNames nixVersions);
  in
  {
    jwt-cpp = self.callPackage ./jwt-cpp.nix { };
    # The store plugin alone, built against the nix that will dlopen() it.
    pluginFor =
      nix':
      self.callPackage ./nix-grpc-store.nix {
        inherit (nix'.libs) nix-store nix-util;
        components = "plugin";
      };
    wrapNix = self.callPackage ./wrap-nix.nix { };
    nix-with-plugin = self.wrapNix nix;
    harmonia-gc = self.callPackage ./harmonia-gc.nix { };

    # Kubernetes images, see deploy/helm. Built against `nix`, not nixPackages.
    imagePackage = self.callPackage ./nix-grpc-store.nix { inherit (nix.libs) nix-store nix-util; };
    docker = self.callPackage ./docker.nix {
      inherit nix niks3;
      nix-grpc-store = self.imagePackage;
    };
    docker-lb = self.callPackage ./docker-lb.nix { tag = self.imagePackage.version; };
    # nix, plugin and nix-eval-jobs must share one libnixstore.
    clientNix = nix-eval-jobs.passthru.nix // {
      libs = {
        inherit (nix-eval-jobs.passthru.nixComponents) nix-store nix-util;
      };
    };
    # 2.0.3 adds --store/--no-download, 2.0.4 forwards --option to every nix
    # call. Drop once nixpkgs has it.
    clientFastBuild =
      if lib.versionAtLeast nix-fast-build.version "2.0.4" then
        nix-fast-build
      else
        nix-fast-build.overrideAttrs (old: rec {
          version = "2.0.4";
          src = old.src.override {
            tag = version;
            hash = "sha256-sc/NZIHkRhgyAzK8Xn6G++vGrl/Uf7QHh+J5fnZ/o4s=";
          };
        });
    clientPlugin = self.pluginFor self.clientNix;
    client-nix-with-plugin = self.wrapNix self.clientNix;
    nix-fast-build-with-plugin = self.callPackage ./wrap-nix-fast-build.nix {
      nix-fast-build = self.clientFastBuild;
      plugin = self.clientPlugin;
    };
    client = symlinkJoin {
      name = "nix-grpc-store-client";
      paths = [
        self.client-nix-with-plugin
        nix-eval-jobs
        self.nix-fast-build-with-plugin
      ];
      passthru = {
        nix = self.client-nix-with-plugin;
        inherit nix-eval-jobs;
        nix-fast-build = self.nix-fast-build-with-plugin;
        plugin = self.clientPlugin;
      };
    };
    docker-client = self.callPackage ./docker.nix {
      variant = "client";
      nix = self.clientNix;
      inherit nix-eval-jobs;
      nix-fast-build = self.clientFastBuild;
      plugin = self.clientPlugin;
    };
    docker-multiarch = self.callPackage ./docker-multiarch.nix {
      name = "nix-grpc-farm-docker";
      imageName = "nix-grpc-farm:latest";
      perArch = lib.genAttrs linuxSystems (s: (scopeFor s).docker);
    };
    docker-lb-multiarch = self.callPackage ./docker-multiarch.nix {
      name = "nix-grpc-farm-lb-docker";
      imageName = "nix-grpc-farm-lb:latest";
      perArch = lib.genAttrs linuxSystems (s: (scopeFor s).docker-lb);
    };
    docker-client-multiarch = self.callPackage ./docker-multiarch.nix {
      name = "nix-grpc-farm-client-docker";
      imageName = "nix-grpc-farm-client:latest";
      perArch = lib.genAttrs linuxSystems (s: (scopeFor s).docker-client);
    };

    default = self.callPackage ./nix-grpc-store.nix {
      inherit (nixPackages) nix-store nix-util;
    };

    # libFuzzer + ASan/UBSan builds of fuzz/*.cc, installed as bin/fuzz-*.
    fuzzers = self.default.overrideAttrs (old: {
      pname = "nix-grpc-store-fuzzers";
      separateDebugInfo = false;
      mesonFlags = (old.mesonFlags or [ ]) ++ [
        "-Dfuzzers=true"
        "-Db_sanitize=address,undefined"
        "-Db_lundef=false"
      ];
      hardeningDisable = [ "fortify" ];
      installPhase = ''
        mkdir -p $out/bin
        find . -maxdepth 1 -type f -name 'fuzz-*' -exec install -m755 -t $out/bin/ {} +
      '';
      doCheck = false;
      dontFixup = true;
    });

    # Same targets with source coverage, for scripts/fuzz.sh -c.
    fuzzers-coverage = self.fuzzers.overrideAttrs (old: {
      pname = "nix-grpc-store-fuzzers-coverage";
      env = old.env // {
        NIX_CFLAGS_COMPILE = old.env.NIX_CFLAGS_COMPILE + " -fprofile-instr-generate -fcoverage-mapping";
        NIX_CFLAGS_LINK = "-fprofile-instr-generate";
      };
    });

    # One plugin per supported Nix release. nixVersions.git is `default`.
    versionPlugins = lib.listToAttrs (
      map (version: lib.nameValuePair "plugin-${version}" (self.pluginFor nixVersions.${version})) supportedNixVersions
    );

    # The loader picks the one matching the running Nix, see meson.build.
    plugin-dispatcher = symlinkJoin {
      name = "nix-grpc-store-dispatcher";
      paths = [
        self.default
      ]
      ++ map (version: self.versionPlugins."plugin-${version}") supportedNixVersions;
      # Copy the loader: dladdr() resolves symlinks, so a symlinked loader
      # would find version plugins in self.default, not this dispatcher.
      postBuild = ''
        rm -f "$out"/lib/nix/plugins/nix-grpc-store-loader.*
        cp -L ${self.default}/lib/nix/plugins/nix-grpc-store-loader.* "$out"/lib/nix/plugins/
      '';
      meta.mainProgram = "nix-grpc-daemon";
    };
  }
)
