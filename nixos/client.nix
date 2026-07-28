{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.programs.nix-grpc-store;
in
{
  options.programs.nix-grpc-store = {
    enable = lib.mkEnableOption "the grpc:// Nix store plugin";

    packageSet = lib.mkOption {
      type = lib.types.raw;
      default = pkgs.callPackage ../packages.nix {
        nixPackages = config.nix.package.libs;
      };
      defaultText = lib.literalExpression ''
        pkgs.callPackage ./packages.nix { nixPackages = config.nix.package.libs; }
      '';
      description = "Package set from packages.nix providing the per-version plugins.";
    };

    package = lib.mkOption {
      type = lib.types.package;
      default = cfg.packageSet.plugin-dispatcher;
      defaultText = lib.literalExpression "config.programs.nix-grpc-store.packageSet.plugin-dispatcher";
      description = "Package providing the plugin loader under `lib/nix/plugins`.";
    };
  };

  config = lib.mkIf cfg.enable {
    # The loader warns and disables grpc:// stores on a version mismatch
    # instead of crashing, so it is safe to load in every nix invocation.
    nix.settings.plugin-files = [ "${cfg.package}/lib/nix/plugins" ];
  };
}
