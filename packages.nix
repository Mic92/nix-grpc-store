# Package set with one plugin per supported Nix version plus the dispatcher bundle.
{
  lib,
  newScope,
  nixVersions,
  symlinkJoin,
  # Nix package set the default plugin builds against.
  nixPackages,
}:
lib.makeScope newScope (
  self:
  let
    # Nix releases from nixpkgs that ship split component libraries.
    supportedNixVersions = builtins.filter (
      name:
      builtins.match "nix_[0-9]+_[0-9]+" name != null
      && (builtins.tryEval ((nixVersions.${name}.libs or { }) ? nix-store)).value or false
    ) (builtins.attrNames nixVersions);
  in
  {
    default = self.callPackage ./package.nix {
      inherit (nixPackages) nix-store nix-util;
    };

    # One plugin per supported Nix version.
    # "git" is only a compile check for the next release.
    versionPlugins = lib.listToAttrs (
      map (
        version:
        lib.nameValuePair "plugin-${version}" (
          self.callPackage ./package.nix {
            inherit (nixVersions.${version}.libs) nix-store nix-util;
          }
        )
      ) (supportedNixVersions ++ [ "git" ])
    );

    # The loader picks the one matching the running Nix, see meson.build.
    plugin-dispatcher = symlinkJoin {
      name = "nix-grpc-store-dispatcher";
      paths = [
        self.default
      ]
      ++ map (version: self.versionPlugins."plugin-${version}") supportedNixVersions;
      meta.mainProgram = "nix-grpc-daemon";
    };
  }
)
