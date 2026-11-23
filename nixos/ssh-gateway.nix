# An ssh-ng:// endpoint in front of a farm: sshd runs `nix-grpc-daemon
# --stdio` for every connection, so clients use the farm as an ordinary
# remote builder without the grpc:// plugin.
{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-ssh-gateway;

  command =
    trusted:
    lib.escapeShellArgs (
      [
        (lib.getExe cfg.package)
        "--stdio"
        "--farm"
        cfg.farm
      ]
      ++ lib.optional trusted "--trusted"
    );

  keyLine =
    trusted: key:
    ''restrict,command="${lib.replaceStrings [ ''"'' ] [ ''\"'' ] (command trusted)}" ${key}'';
in
{
  options.services.nix-grpc-ssh-gateway = {
    enable = lib.mkEnableOption "an ssh-ng:// remote builder that forwards to a nix-grpc-store farm";

    package = lib.mkOption {
      type = lib.types.package;
      default = config.services.nix-grpc-daemon.package;
      defaultText = lib.literalExpression "config.services.nix-grpc-daemon.package";
      description = "Package providing {command}`nix-grpc-daemon`.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "nix-farm";
      description = ''
        Account clients log in as: `ssh-ng://nix-farm@host`. It has no
        shell, every key is forced to run the gateway.
      '';
    };

    farm = lib.mkOption {
      type = lib.types.str;
      example = "grpc://farm.example.com:50051?client-cert=/run/keys/gw.crt&client-key=/run/keys/gw.key";
      description = ''
        `grpc://` URL of the farm, with the credentials the gateway uses.
        They must grant the `trusted` role: the build hook of a trusted
        client sends derivations whose `.drv` is in no store.
        The files must be readable by {option}`user`.
      '';
    };

    trustedKeys = lib.mkOption {
      type = lib.types.listOf lib.types.singleLineStr;
      default = [ ];
      description = ''
        SSH public keys of trusted clients, like `trusted-users` on an
        ordinary remote builder: they may upload unsigned paths and build
        input-addressed derivations sent by the build hook.
      '';
    };

    keys = lib.mkOption {
      type = lib.types.listOf lib.types.singleLineStr;
      default = [ ];
      description = ''
        SSH public keys of untrusted clients. They may only upload paths
        signed by a key in the farm's `trusted-public-keys`, and only
        build derivations whose closure they uploaded, like a plain
        nix-daemon user.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = lib.hasPrefix "grpc://" cfg.farm;
        message = "services.nix-grpc-ssh-gateway.farm must be a grpc:// URL";
      }
    ];

    services.openssh.enable = true;

    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.user;
      # sshd needs a valid shell even with a forced command.
      useDefaultShell = true;
      home = "/var/empty";
      openssh.authorizedKeys.keys = map (keyLine true) cfg.trustedKeys ++ map (keyLine false) cfg.keys;
    };
    users.groups.${cfg.user} = { };
  };
}
