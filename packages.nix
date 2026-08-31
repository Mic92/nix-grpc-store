# Package set with one plugin per supported Nix version plus the dispatcher bundle.
{
  lib,
  newScope,
  nixVersions,
  clangStdenv,
  symlinkJoin,
  # Nix package set the default plugin builds against.
  nixPackages,
}:
lib.makeScope newScope (
  self:
  let
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
    default = self.callPackage ./package.nix {
      inherit (nixPackages) nix-store nix-util;
    };

    # libFuzzer + ASan/UBSan builds of fuzz/*.cc, installed as bin/fuzz-*.
    fuzzers = (self.default.override { stdenv = clangStdenv; }).overrideAttrs (old: {
      pname = "nix-grpc-store-fuzzers";
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
      map (
        version:
        lib.nameValuePair "plugin-${version}" (
          self.callPackage ./package.nix {
            inherit (nixVersions.${version}.libs) nix-store nix-util;
          }
        )
      ) supportedNixVersions
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
