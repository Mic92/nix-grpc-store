{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-daemon;
  logDir = "/var/log/nix-grpc-daemon";
  logFile = "${logDir}/daemon.log";
in
{
  imports = [ ./common.nix ];

  options.services.nix-grpc-daemon = {
    uid = lib.mkOption {
      type = lib.types.int;
      default = 450;
      description = "UID of the daemon user.";
    };
    gid = lib.mkOption {
      type = lib.types.int;
      default = 450;
      description = "GID of the daemon group.";
    };
  };

  config = lib.mkIf cfg.enable {
    services.nix-grpc-daemon.idleTimeout = lib.mkDefault null;

    assertions = [
      {
        assertion = cfg.idleTimeout == null;
        message = "services.nix-grpc-daemon.idleTimeout needs socket activation, which launchd does not provide";
      }
    ];

    users.knownGroups = [ "nix-grpc-daemon" ];
    users.knownUsers = [ "nix-grpc-daemon" ];
    users.groups.nix-grpc-daemon.gid = cfg.gid;
    users.users.nix-grpc-daemon = {
      inherit (cfg) uid gid;
      home = "/var/empty";
      shell = "/usr/bin/false";
      isHidden = true;
    };

    launchd.daemons.nix-grpc-daemon = {
      path = lib.optional (cfg.niks3 != null) config.nix.package;
      serviceConfig = {
        ProgramArguments = map toString (import ./daemon-args.nix { inherit cfg lib pkgs; });
        UserName = "nix-grpc-daemon";
        GroupName = "nix-grpc-daemon";
        RunAtLoad = true;
        KeepAlive = true;
        StandardOutPath = logFile;
        StandardErrorPath = logFile;
      };
    };

    # launchd opens the log as the job's user, so /var/log itself is not writable.
    system.activationScripts.postActivation.text = ''
      install -d -o nix-grpc-daemon -g nix-grpc-daemon -m 0750 ${logDir}
    '';
    environment.etc."newsyslog.d/nix-grpc-daemon.conf".text = ''
      ${logFile} nix-grpc-daemon:nix-grpc-daemon 640 5 10240 * J
    '';
  };
}
