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
      {
        # Nix opens the store with a SQLite cache under the user's home, which is /var/empty here.
        assertion = cfg.buildLogStore == null;
        message = "services.nix-grpc-daemon.buildLogStore is not supported on nix-darwin yet";
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
      # `command` rather than ProgramArguments: nix-darwin prefixes it with
      # `/bin/wait4path /nix/store`. At boot launchd starts daemons before the
      # Nix store volume is mounted, finds no executable, and gives up on the
      # job for good (exit 78, despite KeepAlive).
      command = lib.escapeShellArgs (map toString (import ./daemon-args.nix { inherit cfg lib pkgs; }));
      serviceConfig = {
        UserName = "nix-grpc-daemon";
        GroupName = "nix-grpc-daemon";
        RunAtLoad = true;
        KeepAlive = true;
        # launchd's default soft limit is 256, which a busy worker exhausts
        # with one socket per balancer connection plus the NAR spool files.
        SoftResourceLimits.NumberOfFiles = lib.mkDefault 65536;
        HardResourceLimits.NumberOfFiles = lib.mkDefault 65536;
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
