# systemd units of nix-grpc-daemon, shared by the NixOS module and the
# flakelet (nix/flakelet-daemon.nix).
{
  cfg,
  lib,
  pkgs,
  nixPackage,
  name ? "nix-grpc-daemon",
}:
let
  builder = lib.elem "builder" cfg.roles;
  # A socket unit is not restarted when its listen address changes, and an
  # idle node would never register with the scheduler.
  socketActivated = cfg.idleTimeout != null;
in
{
  inherit socketActivated;

  assertions = [
    {
      assertion = cfg.scheduler == null || cfg.idleTimeout == null;
      message = "services.nix-grpc-daemon.idleTimeout must be null with scheduler: an idle builder would stop taking work";
    }
  ];

  socket = {
    description = "Nix worker-protocol over gRPC";
    wantedBy = [ "sockets.target" ];
    socketConfig.ListenStream = lib.removePrefix "unix:" cfg.listen;
  };

  service = {
    description = "Nix worker-protocol over gRPC";
    # niks3 push shells out to `nix path-info`.
    path = lib.optional (cfg.niks3 != null) nixPackage;
    wantedBy = lib.optional (!socketActivated) "multi-user.target";
    requires = lib.optional socketActivated "${name}.socket";
    # nix-daemon is socket-activated; ordering after the socket is enough,
    # the first proxied connection will start it.
    after = lib.optional socketActivated "${name}.socket" ++ [ "nix-daemon.socket" ];
    wants = [ "nix-daemon.socket" ];
    # /tmp is a size-limited tmpfs.
    environment.TMPDIR = "/var/cache/${name}";
    serviceConfig = {
      CacheDirectory = name;
      Type = "notify";
      WatchdogSec = 30;
      User = "nix-grpc-daemon";
      Group = "nix-grpc-daemon";
      Restart = if socketActivated then "on-failure" else "always";
      ExecStart = lib.escapeShellArgs (import ./daemon-args.nix { inherit cfg lib pkgs; });
    }
    // lib.optionalAttrs (!socketActivated) {
      # Binds listen itself, which may be a port below 1024.
      AmbientCapabilities = [ "CAP_NET_BIND_SERVICE" ];
      CapabilityBoundingSet = [ "CAP_NET_BIND_SERVICE" ];
    }
    // lib.optionalAttrs builder {
      ExecReload = "${pkgs.coreutils}/bin/kill -TERM $MAINPID";
      # stop drains too, and niks3 push must live on to publish.
      KillMode = "mixed";
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

  # [Service] settings a host may override.
  serviceConfigDefaults = {
    LimitNOFILE = 65536;
    # Keep RPCs responsive next to builds in nix-daemon.service.
    CPUWeight = 1000;
    # Builds run in nix-daemon. This only bounds the proxy.
    MemoryMax = "2G";
  }
  // lib.optionalAttrs builder {
    TimeoutStopSec = "1h";
  };
}
