{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-daemon;
in
{
  options.services.nix-grpc-daemon = {
    enable = lib.mkEnableOption "the nix-grpc-daemon proxy";

    package = lib.mkOption {
      type = lib.types.package;
      # The daemon only links `nix-util`, so any recent Nix's libs will do.
      default = pkgs.callPackage ../package.nix {
        inherit (pkgs.nix.libs) nix-store nix-util;
      };
      defaultText = lib.literalExpression "pkgs.callPackage ./package.nix { }";
      description = "Package providing {command}`nix-grpc-daemon`.";
    };

    trustClients = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Add the proxy user to `nix.settings.trusted-users`. Needed to use
        this daemon as a remote builder (clients import unsigned store
        paths); gives every authenticated gRPC client trusted-user
        privileges.
      '';
    };

    listen = lib.mkOption {
      type = lib.types.str;
      default = "0.0.0.0:50051";
      description = "Address to listen on, in gRPC `host:port` form.";
    };

    logLevel = lib.mkOption {
      type = lib.types.enum [
        "info"
        "debug"
      ];
      default = "info";
      description = ''
        Access log verbosity; `info` logs Connect sessions and bulk
        transfers, `debug` also logs path queries and session starts.
      '';
    };

    metricsListen = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "127.0.0.1:9464";
      description = "Address to serve Prometheus metrics on; disabled if unset.";
    };

    proxySocket = lib.mkOption {
      type = lib.types.path;
      default = "/nix/var/nix/daemon-socket/socket";
      description = "Path to the backing nix-daemon unix socket.";
    };

    tls = {
      certFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "PEM server certificate chain. Plaintext if unset.";
      };
      keyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "PEM private key for {option}`certFile`.";
      };
      clientCaFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          PEM CA bundle used to verify client certificates. When set, clients
          must present a certificate signed by this CA (mTLS).
        '';
      };
    };

    extraFlags = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Additional command-line flags.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.tls.clientCaFile == null || cfg.tls.certFile != null;
        message = "services.nix-grpc-daemon.tls.clientCaFile requires tls.certFile/keyFile";
      }
      {
        assertion = (cfg.tls.certFile == null) == (cfg.tls.keyFile == null);
        message = "services.nix-grpc-daemon.tls.certFile and tls.keyFile must be set together";
      }
    ];

    # Reach the local nix-daemon even when allowed-users is restricted.
    nix.settings.extra-allowed-users = [ "nix-grpc-daemon" ];
    nix.settings.extra-trusted-users = lib.mkIf cfg.trustClients [ "nix-grpc-daemon" ];

    # gRPC clients inherit the store privileges of this uid via the proxied
    # nix-daemon connection, so default to a dedicated unprivileged user.
    users.users.nix-grpc-daemon = {
      isSystemUser = true;
      group = "nix-grpc-daemon";
    };
    users.groups.nix-grpc-daemon = { };

    systemd.services.nix-grpc-daemon = {
      description = "Nix worker-protocol over gRPC";
      wantedBy = [ "multi-user.target" ];
      # nix-daemon is socket-activated; ordering after the socket is enough,
      # the first proxied connection will start it.
      after = [
        "network.target"
        "nix-daemon.socket"
      ];
      wants = [ "nix-daemon.socket" ];
      serviceConfig = {
        User = "nix-grpc-daemon";
        Group = "nix-grpc-daemon";
        Restart = "on-failure";
        ExecStart = lib.escapeShellArgs (
          [
            (lib.getExe cfg.package)
            "--listen"
            cfg.listen
            "--proxy-socket"
            cfg.proxySocket
          ]
          ++ lib.optionals (cfg.tls.certFile != null) [
            "--tls-cert"
            cfg.tls.certFile
            "--tls-key"
            cfg.tls.keyFile
          ]
          ++ lib.optionals (cfg.tls.clientCaFile != null) [
            "--client-ca"
            cfg.tls.clientCaFile
          ]
          ++ lib.optionals (cfg.metricsListen != null) [
            "--metrics-listen"
            cfg.metricsListen
          ]
          ++ [
            "--log-level"
            cfg.logLevel
          ]
          ++ cfg.extraFlags
        );
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
