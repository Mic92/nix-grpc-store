{
  config,
  options,
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
      default = pkgs.callPackage ../nix/packages {
        nixPackages = config.nix.package.libs;
      };
      defaultText = lib.literalExpression ''
        pkgs.callPackage ./nix/packages { nixPackages = config.nix.package.libs; }
      '';
      description = "Package set from nix/packages providing the per-version plugins.";
    };

    package = lib.mkOption {
      type = lib.types.package;
      default = cfg.packageSet.plugin-dispatcher;
      defaultText = lib.literalExpression "config.programs.nix-grpc-store.packageSet.plugin-dispatcher";
      description = "Package providing the plugin loader under `lib/nix/plugins`.";
    };

    daemonEgressPorts = lib.mkOption {
      type = lib.types.listOf lib.types.port;
      default = [ 50051 ];
      description = ''
        Ports the build hook may connect to when `nix.firewall` restricts
        nix-daemon egress. The hook runs inside `nix-daemon.service`, so
        without this a `grpc://` entry in `nix.buildMachines` cannot connect.
      '';
    };
  };

  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      {
        # The loader warns and disables grpc:// stores on a version mismatch
        # instead of crashing, so it is safe to load in every nix invocation.
        nix.settings.plugin-files = [ "${cfg.package}/lib/nix/plugins" ];
      }
      (lib.optionalAttrs (options ? nix.firewall) {
        nix.firewall.extraNftablesRules = lib.mkIf (cfg.daemonEgressPorts != [ ]) [
          "tcp dport { ${lib.concatMapStringsSep ", " toString cfg.daemonEgressPorts} } accept"
        ];
      })
    ]
  );
}
