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
      description = ''
        Address to listen on, `host:port` or `unix:/path`. Bound by a
        systemd socket unit. The daemon is started on the first connection.
      '';
    };

    idleTimeout = lib.mkOption {
      type = lib.types.nullOr lib.types.ints.positive;
      default = 600;
      description = ''
        Exit after this many seconds without in-flight RPCs. systemd keeps
        the socket and restarts the daemon on the next connection. `null`
        keeps it running.
      '';
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

    accessRules = lib.mkOption {
      type = lib.types.listOf (
        lib.types.submodule {
          options = {
            cn = lib.mkOption {
              type = lib.types.str;
              example = "ci-*";
              description = "Glob pattern matched against the client certificate CN.";
            };
            role = lib.mkOption {
              type = lib.types.enum [
                "read-only"
                "write"
                "trusted"
              ];
              description = ''
                `read-only` allows path queries and NAR downloads.
                `write` additionally allows signed imports (signature
                checking is enforced) and server-side builds. `trusted`
                allows everything, including the raw worker-protocol
                tunnel and unsigned imports (the latter also needs
                {option}`trustClients`).
              '';
            };
          };
        }
      );
      default = [ ];
      example = lib.literalExpression ''
        [
          { cn = "ci-*"; role = "trusted"; }
          { cn = "cache-mirror"; role = "read-only"; }
          { cn = "*"; role = "write"; }
        ]
      '';
      description = ''
        Ordered access-control rules mapping client certificate CNs to
        roles; the first matching rule wins and clients matching no rule
        are denied. Requires {option}`tls.clientCaFile`. When empty, every
        authenticated client has full access.
      '';
    };

    anonymousRole = lib.mkOption {
      type = lib.types.nullOr (
        lib.types.enum [
          "read-only"
          "write"
          "trusted"
        ]
      );
      default = null;
      example = "read-only";
      description = ''
        Role for clients that present no certificate. When set, a client
        certificate is no longer required (but still verified when
        presented). `read-only` turns the daemon into a public binary
        cache while certificate holders keep their {option}`accessRules`
        roles. Requires {option}`tls.clientCaFile`.
      '';
    };

    oidc = lib.mkOption {
      type = lib.types.nullOr (pkgs.formats.json { }).type;
      default = null;
      example = lib.literalExpression ''
        {
          providers.github = {
            issuer = "https://token.actions.githubusercontent.com";
            audience = "grpc://cache.example.com";
            rules = [
              { bound_claims.repository_owner = [ "myorg" ]; scopes = [ "write" ]; }
              { bound_claims.ref = [ "refs/heads/main" ]; scopes = [ "admin" ]; }
            ];
          };
          providers.k8s = {
            audience = "grpc://cache.example.com";
            ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
            bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token";
            bound_claims."kubernetes.io.namespace" = [ "ci" ];
          };
        }
      '';
      description = ''
        Accept OIDC bearer tokens (`authorization: Bearer …`) besides client
        certificates. Same schema as niks3's `--oidc-config`, so one attrset
        can serve both. Scopes map to roles: `read` → read-only, `write` →
        write, `admin` → trusted. A client certificate, when presented,
        takes precedence. Clients set `token-file` or `$NIX_GRPC_TOKEN_FILE`.
      '';
    };

    extraFlags = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Additional command-line flags.";
    };

    farm = {
      enable = lib.mkEnableOption ''
        build farm worker mode. `BuildDerivation` claims outputs at niks3,
        substitutes inputs from the cache and publishes results through a
        long-lived `niks3 push --stdin`
      '';
      niks3Package = lib.mkOption {
        type = lib.types.package;
        example = lib.literalExpression "inputs.niks3.packages.\${system}.niks3";
        description = "Package providing the `niks3` client.";
      };
      niks3Url = lib.mkOption {
        type = lib.types.str;
        example = "https://niks3.example.org";
        description = "niks3 server URL.";
      };
      tokenFile = lib.mkOption {
        type = lib.types.path;
        example = "/run/secrets/niks3-token";
        description = "File with the niks3 API bearer token.";
      };
      cacheUrl = lib.mkOption {
        type = lib.types.str;
        example = "https://cache.example.org";
        description = "Binary cache the farm publishes to. Workers substitute inputs from it.";
      };
      publicKeys = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        description = "Signing keys of {option}`cacheUrl`.";
      };
      maxJobs = lib.mkOption {
        type = lib.types.ints.positive;
        default = 1;
        description = "Concurrent builds on this worker.";
      };
      minFree = lib.mkOption {
        type = lib.types.str;
        default = "10G";
        description = ''
          Below this much free space under /nix the worker reports
          NOT_SERVING to the balancer and bounces builds with UNAVAILABLE so
          they run elsewhere. `0` disables.
        '';
      };
      pushFlags = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [ ];
        example = [ "--max-concurrent-uploads" "8" ];
        description = "Extra flags for `niks3 push --stdin`.";
      };
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
      {
        assertion =
          (cfg.accessRules == [ ] && cfg.anonymousRole == null) || cfg.tls.clientCaFile != null || cfg.oidc != null;
        message = "services.nix-grpc-daemon.accessRules/anonymousRole requires tls.clientCaFile or oidc";
      }
      {
        assertion = cfg.oidc == null || cfg.tls.certFile != null || (cfg.oidc.allow_insecure or false);
        message = "services.nix-grpc-daemon.oidc sends bearer tokens, enable tls.certFile";
      }
    ];

    # Reach the local nix-daemon even when allowed-users is restricted.
    nix.settings.extra-allowed-users = [ "nix-grpc-daemon" ];
    nix.settings.extra-trusted-users = lib.mkIf cfg.trustClients [ "nix-grpc-daemon" ];

    # The cache is the share between workers. A path another
    # worker just pushed must not be negatively cached here.
    nix.settings.substituters = lib.mkIf cfg.farm.enable [ cfg.farm.cacheUrl ];
    nix.settings.trusted-public-keys = lib.mkIf cfg.farm.enable cfg.farm.publicKeys;
    nix.settings.narinfo-cache-negative-ttl = lib.mkIf cfg.farm.enable 0;
    nix.settings.max-jobs = lib.mkIf cfg.farm.enable (lib.mkDefault cfg.farm.maxJobs);
    nix.settings.keep-build-log = lib.mkIf cfg.farm.enable true;

    # gRPC clients inherit the store privileges of this uid via the proxied
    # nix-daemon connection, so default to a dedicated unprivileged user.
    users.users.nix-grpc-daemon = {
      isSystemUser = true;
      group = "nix-grpc-daemon";
    };
    users.groups.nix-grpc-daemon = { };

    systemd.sockets.nix-grpc-daemon = {
      description = "Nix worker-protocol over gRPC";
      wantedBy = [ "sockets.target" ];
      socketConfig.ListenStream = lib.removePrefix "unix:" cfg.listen;
    };

    systemd.services.nix-grpc-daemon = {
      description = "Nix worker-protocol over gRPC";
      # niks3 push shells out to `nix path-info`.
      path = lib.optional cfg.farm.enable config.nix.package;
      requires = [ "nix-grpc-daemon.socket" ];
      # nix-daemon is socket-activated; ordering after the socket is enough,
      # the first proxied connection will start it.
      after = [
        "nix-grpc-daemon.socket"
        "nix-daemon.socket"
      ];
      wants = [ "nix-daemon.socket" ];
      serviceConfig = {
        Type = "notify";
        WatchdogSec = 30;
        User = "nix-grpc-daemon";
        Group = "nix-grpc-daemon";
        Restart = "on-failure";
        ExecStart = lib.escapeShellArgs (
          [
            (lib.getExe cfg.package)
            "--proxy-socket"
            cfg.proxySocket
          ]
          ++ lib.optionals (cfg.idleTimeout != null) [
            "--idle-timeout"
            (toString cfg.idleTimeout)
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
          ++ lib.concatMap (rule: [
            "--allow"
            "${rule.cn}=${rule.role}"
          ]) cfg.accessRules
          ++ lib.optionals (cfg.anonymousRole != null) [
            "--allow-anonymous"
            cfg.anonymousRole
          ]
          ++ lib.optionals (cfg.oidc != null) [
            "--oidc-config"
            ((pkgs.formats.json { }).generate "nix-grpc-daemon-oidc.json" cfg.oidc)
          ]
          ++ lib.optionals (cfg.metricsListen != null) [
            "--metrics-listen"
            cfg.metricsListen
          ]
          ++ [
            "--log-level"
            cfg.logLevel
          ]
          ++ lib.optionals cfg.farm.enable [
            "--niks3"
            cfg.farm.niks3Url
            "--niks3-token-file"
            cfg.farm.tokenFile
            "--niks3-push"
            (lib.escapeShellArgs ([ (lib.getExe cfg.farm.niks3Package) "push" ] ++ cfg.farm.pushFlags))
            "--max-jobs"
            (toString cfg.farm.maxJobs)
            "--min-free"
            cfg.farm.minFree
          ]
          ++ cfg.extraFlags
        );
        # Builds run in nix-daemon. This only bounds the proxy.
        MemoryMax = lib.mkDefault "2G";
        # A farm worker holds claims for running builds.
        TimeoutStopSec = lib.mkIf cfg.farm.enable "infinity";
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
