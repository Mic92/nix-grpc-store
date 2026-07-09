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

    package = lib.mkOption {
      type = lib.types.package;
      # Build against the component libraries of the system's Nix so the
      # plugin's C++ ABI matches the `nix` binary that will dlopen() it.
      default = pkgs.callPackage ../package.nix {
        inherit (config.nix.package.libs) nix-store nix-util;
      };
      defaultText = lib.literalExpression ''
        pkgs.callPackage ./package.nix {
          inherit (config.nix.package.libs) nix-store nix-util;
        }
      '';
      description = "Package providing `lib/nix/plugins/nix-grpc-store.so`.";
    };
  };

  config = lib.mkIf cfg.enable {
    nix.settings.plugin-files = [ "${cfg.package}/lib/nix/plugins/nix-grpc-store.so" ];
  };
}
