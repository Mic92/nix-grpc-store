# Envoy in front of the nodes. `/nix.remote.Scheduler/*` goes to the one
# scheduler node (cluster `sched`, gRPC health service "nix.scheduler").
# Everything else goes to the per-system worker cluster: BuildDerivation to
# the worker named in x-nix-worker (override_host), the rest least-request.
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.nix-grpc-farm-lb;

  file = path: { filename = toString path; };
  tlsCert = {
    certificate_chain = file cfg.tls.certFile;
    private_key = file cfg.tls.keyFile;
  };

  upstreamTls = lib.optionalAttrs (cfg.tls.upstream.certFile != null) {
    transport_socket = {
      name = "envoy.transport_sockets.tls";
      typed_config = {
        "@type" = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext";
        common_tls_context = {
          alpn_protocols = [ "h2" ];
          tls_certificates = [
            {
              certificate_chain = file cfg.tls.upstream.certFile;
              private_key = file cfg.tls.upstream.keyFile;
            }
          ];
          validation_context.trusted_ca = file cfg.tls.upstream.caFile;
        };
      }
      // lib.optionalAttrs (cfg.tls.upstream.sni != null) { inherit (cfg.tls.upstream) sni; };
    };
  };

  stderrLog = fmt: {
    name = "envoy.access_loggers.stderr";
    typed_config = {
      "@type" = "type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StderrAccessLog";
      log_format.text_format_source.inline_string = fmt + "\n";
    };
  };

  downstreamTls = lib.optionalAttrs (cfg.tls.certFile != null) {
    transport_socket = {
      name = "envoy.transport_sockets.tls";
      typed_config = {
        "@type" = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext";
        common_tls_context = {
          alpn_protocols = [ "h2" ];
          tls_certificates = [ tlsCert ];
        }
        // lib.optionalAttrs (cfg.tls.clientCaFile != null) {
          validation_context.trusted_ca = file cfg.tls.clientCaFile;
        };
      }
      // lib.optionalAttrs (cfg.tls.clientCaFile != null) {
        require_client_certificate = false;
      };
    };
  };

  endpoint = addr: {
    endpoint.address.socket_address =
      let
        m = builtins.match "(.*):([0-9]+)" addr;
      in
      rec {
        address = lib.removeSuffix "]" (lib.removePrefix "[" (builtins.elemAt m 0));
        port_value = lib.toInt (builtins.elemAt m 1);
        ipv4_compat = address == "::";
      };
  };

  http2 = {
    typed_extension_protocol_options."envoy.extensions.upstreams.http.v3.HttpProtocolOptions" = {
      "@type" = "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions";
      explicit_http_config.http2_protocol_options = {
        max_concurrent_streams = cfg.maxStreams;
        connection_keepalive = {
          interval = "30s";
          timeout = "10s";
        };
      };
    };
  };

  healthCheck = service: {
    timeout = "2s";
    interval = cfg.healthCheckInterval;
    no_traffic_interval = cfg.healthCheckInterval;
    unhealthy_threshold = 2;
    healthy_threshold = 1;
    grpc_health_check = lib.optionalAttrs (service != "") { service_name = service; };
  };

  workerCluster =
    name: workers:
    upstreamTls
    // http2
    // {
      inherit name;
      type = "STRICT_DNS";
      connect_timeout = "5s";
      load_balancing_policy.policies = [
        {
          typed_extension_config = {
            name = "envoy.load_balancing_policies.override_host";
            typed_config = {
              "@type" = "type.googleapis.com/envoy.extensions.load_balancing_policies.override_host.v3.OverrideHost";
              override_host_sources = [ { header = "x-nix-worker"; } ];
              fallback_policy.policies = [
                {
                  typed_extension_config = {
                    name = "envoy.load_balancing_policies.least_request";
                    typed_config."@type" = "type.googleapis.com/envoy.extensions.load_balancing_policies.least_request.v3.LeastRequest";
                  };
                }
              ];
            };
          };
        }
      ];
      health_checks = [ (healthCheck "") ];
      load_assignment = {
        cluster_name = name;
        endpoints = [
          {
            priority = 0;
            lb_endpoints = map endpoint workers;
          }
        ];
      };
    };

  schedCluster =
    upstreamTls
    // http2
    // {
      name = "sched";
      type = "STRICT_DNS";
      connect_timeout = "5s";
      health_checks = [ (healthCheck "nix.scheduler") ];
      # One priority level per node: envoy uses the first healthy one.
      load_assignment = {
        cluster_name = "sched";
        endpoints = lib.imap0 (priority: addr: {
          inherit priority;
          lb_endpoints = [ (endpoint addr) ];
        }) (lib.toList cfg.scheduler);
      };
    };

  systemMatch =
    system:
    lib.optionalAttrs (system != cfg.defaultSystem) {
      headers = [
        {
          name = "x-nix-system";
          string_match.exact = system;
        }
      ];
    };

  schedRoute = {
    match = {
      prefix = "/nix.remote.Scheduler/";
      grpc = { };
    };
    route = {
      cluster = "sched";
      timeout = "0s";
    };
  };

  routesFor = system: [
    {
      match = {
        prefix = "/";
        grpc = { };
      }
      // systemMatch system;
      route = {
        cluster = system;
        timeout = "0s";
      };
    }
  ];

  systems = lib.attrNames cfg.workers;
  ordered = lib.filter (s: s != cfg.defaultSystem) systems ++ [ cfg.defaultSystem ];
in
{
  options.services.nix-grpc-farm-lb = {
    enable = lib.mkEnableOption "envoy load balancer for nix-grpc-daemon farm workers";

    listen = lib.mkOption {
      type = lib.types.str;
      default = "[::]:50051";
    };

    workers = lib.mkOption {
      type = lib.types.attrsOf (lib.types.nonEmptyListOf lib.types.str);
      example = {
        x86_64-linux = [
          "w1:50051"
          "w2:50051"
        ];
      };
      description = ''
        `IP:port` of builder nodes per system. Must equal each node's
        `services.nix-grpc-daemon.advertise` so `x-nix-worker` pins to it.
      '';
    };

    scheduler = lib.mkOption {
      type = lib.types.either lib.types.str (lib.types.listOf lib.types.str);
      default = lib.head cfg.workers.${cfg.defaultSystem};
      defaultText = lib.literalMD "first worker of `defaultSystem`";
      example = [
        "10.0.0.4:50051"
        "10.0.0.5:50051"
      ];
      description = ''
        The worker(s) with the `scheduler` role, in order of preference.
        Scheduler traffic goes to the first one that is up. The later ones
        stay passive until every node before them is down, see
        `services.nix-grpc-daemon.schedulerOrder`. A single string is a
        list of one.
      '';
    };

    defaultSystem = lib.mkOption {
      type = lib.types.str;
      default = lib.head systems;
      defaultText = lib.literalMD "first attribute of `workers`";
      description = "Cluster for requests without or with an unknown `x-nix-system` (store queries, uploads, `builtin`).";
    };

    healthCheckInterval = lib.mkOption {
      type = lib.types.str;
      default = "5s";
      description = "How often envoy probes gRPC health of workers and scheduler candidates. Two misses eject.";
    };

    admin = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = "127.0.0.1:9901";
      description = "Envoy admin listener (`/clusters`, `/stats`), or null.";
    };

    accessLog = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Log one line per connection to the journal with the TLS handshake
        failure reason and the client certificate subject.
      '';
    };

    maxStreams = lib.mkOption {
      type = lib.types.ints.positive;
      default = 1024;
      description = "HTTP/2 streams per connection, both directions. Must exceed client `max-builds`.";
    };

    tls = {
      certFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "PEM server certificate presented to clients. Plaintext listener if unset.";
      };
      keyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
      };
      clientCaFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          Verify client certificates against this CA and forward the subject to
          workers in `x-forwarded-client-cert`. Clients without a certificate
          are still accepted so bearer tokens keep working. The header is
          always overwritten, a client cannot inject one.
        '';
      };
      upstream = {
        certFile = lib.mkOption {
          type = lib.types.nullOr lib.types.path;
          default = null;
          description = ''
            Client certificate envoy presents to workers. Its CN must match the
            workers' `services.nix-grpc-daemon.trustedProxies`. Plaintext to
            workers if unset.
          '';
        };
        keyFile = lib.mkOption {
          type = lib.types.nullOr lib.types.path;
          default = null;
        };
        caFile = lib.mkOption {
          type = lib.types.nullOr lib.types.path;
          default = null;
          description = "CA that signed the workers' server certificates.";
        };
        sni = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "SNI and name to verify on worker certificates. Unset verifies the chain only.";
        };
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = (cfg.tls.certFile == null) == (cfg.tls.keyFile == null);
        message = "services.nix-grpc-farm-lb.tls.certFile and keyFile go together";
      }
      {
        assertion = cfg.tls.clientCaFile == null || cfg.tls.certFile != null;
        message = "services.nix-grpc-farm-lb.tls.clientCaFile requires tls.certFile";
      }
      {
        assertion =
          (cfg.tls.upstream.certFile == null)
          == (cfg.tls.upstream.keyFile == null)
          && (cfg.tls.upstream.certFile == null) == (cfg.tls.upstream.caFile == null);
        message = "services.nix-grpc-farm-lb.tls.upstream.{certFile,keyFile,caFile} go together";
      }
    ];
    # restart in one step instead of stop ... activate ... start, so a
    # deploy is a sub-second blip that the client's connect retry absorbs.
    systemd.services.envoy.stopIfChanged = false;

    services.envoy = {
      enable = true;
      package = lib.mkDefault pkgs.envoy-bin;
      # Validation resolves STRICT_DNS names, which the build sandbox cannot.
      requireValidConfig = false;
      settings = {
        admin = lib.mkIf (cfg.admin != null) { address = (endpoint cfg.admin).endpoint.address; };
        static_resources = {
          listeners = [
            {
              name = "farm";
              address = (endpoint cfg.listen).endpoint.address;
              access_log = lib.optional cfg.accessLog (stderrLog "conn peer=%DOWNSTREAM_REMOTE_ADDRESS% tls=%DOWNSTREAM_TLS_VERSION% subject=\"%DOWNSTREAM_PEER_SUBJECT%\" sni=%REQUESTED_SERVER_NAME% flags=%RESPONSE_FLAGS% tls_fail=\"%DOWNSTREAM_TRANSPORT_FAILURE_REASON%\" rx=%BYTES_RECEIVED% tx=%BYTES_SENT% ms=%DURATION%");
              filter_chains = [
                (
                  downstreamTls
                  // {
                  filters = [
                    {
                      name = "envoy.filters.network.http_connection_manager";
                      typed_config = {
                        "@type" =
                          "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager";
                        stat_prefix = "farm";
                        codec_type = "HTTP2";
                        stream_idle_timeout = "0s";
                        http2_protocol_options.max_concurrent_streams = cfg.maxStreams;
                        forward_client_cert_details = "SANITIZE_SET";
                        set_current_client_cert_details.subject = true;
                        route_config.virtual_hosts = [
                          {
                            name = "farm";
                            domains = [ "*" ];
                            routes = [ schedRoute ] ++ lib.concatMap routesFor ordered;
                          }
                        ];
                        http_filters = [
                          {
                            name = "envoy.filters.http.router";
                            typed_config."@type" = "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router";
                          }
                        ];
                      };
                    }
                  ];
                }
                )
              ];
            }
          ];
          clusters = lib.mapAttrsToList workerCluster cfg.workers ++ [ schedCluster ];
        };
      };
    };
  };
}
