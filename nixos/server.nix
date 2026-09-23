{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-daemon;
  builder = lib.elem "builder" cfg.roles;
  # A socket unit is not restarted when its listen address changes, and an
  # idle node would never register with the scheduler.
  socketActivated = cfg.idleTimeout != null;
in
{
  imports = [ ./common.nix ];

  config = lib.mkIf cfg.enable {
    # gRPC clients inherit the store privileges of this uid via the proxied
    # nix-daemon connection, so default to a dedicated unprivileged user.
    users.users.nix-grpc-daemon = {
      isSystemUser = true;
      group = "nix-grpc-daemon";
    };
    users.groups.nix-grpc-daemon = { };

    systemd.sockets.nix-grpc-daemon = lib.mkIf socketActivated {
      description = "Nix worker-protocol over gRPC";
      wantedBy = [ "sockets.target" ];
      socketConfig.ListenStream = lib.removePrefix "unix:" cfg.listen;
    };

    systemd.services.nix-grpc-daemon = {
      description = "Nix worker-protocol over gRPC";
      # niks3 push shells out to `nix path-info`.
      path = lib.optional (cfg.niks3 != null) config.nix.package;
      wantedBy = lib.mkIf (!socketActivated) [ "multi-user.target" ];
      requires = lib.optional socketActivated "nix-grpc-daemon.socket";
      # nix-daemon is socket-activated; ordering after the socket is enough,
      # the first proxied connection will start it.
      after = lib.optional socketActivated "nix-grpc-daemon.socket" ++ [ "nix-daemon.socket" ];
      wants = [ "nix-daemon.socket" ];
      # Reload = drain: leave the balancer, finish and publish running
      # builds, then exit. The socket or Restart=always starts the new
      # generation.
      reloadIfChanged = builder;
      serviceConfig = {
        Type = "notify";
        WatchdogSec = 30;
        # Keep RPCs responsive next to builds in nix-daemon.service.
        CPUWeight = lib.mkDefault 1000;
        User = "nix-grpc-daemon";
        Group = "nix-grpc-daemon";
        Restart = if socketActivated then "on-failure" else "always";
        ExecStart = lib.escapeShellArgs (import ./daemon-args.nix { inherit cfg lib pkgs; });
        # Builds run in nix-daemon. This only bounds the proxy.
        MemoryMax = lib.mkDefault "2G";
      }
      // lib.optionalAttrs builder {
        ExecReload = "${pkgs.coreutils}/bin/kill -TERM $MAINPID";
        # stop drains too, and niks3 push must live on to publish.
        KillMode = "mixed";
        TimeoutStopSec = lib.mkDefault "1h";
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
      };
    };
  };
}
