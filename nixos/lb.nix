# Envoy in front of farm workers. One cluster per system, MAGLEV on
# x-nix-drv so the same derivation lands on the same worker from every
# client, gRPC health checks so a draining worker stops receiving work.
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

  cluster =
    system: workers:
    upstreamTls
    // {
      name = system;
      type = "STRICT_DNS";
    connect_timeout = "5s";
    lb_policy = "MAGLEV";
    # Worker slots are few. Spread rather than pile onto one hash bucket.
    common_lb_config.consistent_hashing_lb_config.hash_balance_factor = 125;
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
    # Eject a worker that keeps bouncing builds (UNAVAILABLE counts as 5xx).
    outlier_detection = {
      consecutive_5xx = 3;
      base_ejection_time = "30s";
      max_ejection_percent = 50;
    };
    health_checks = [
      {
        timeout = "2s";
        interval = "5s";
        # Farms idle between evaluations. Keep checking.
        no_traffic_interval = "5s";
        unhealthy_threshold = 2;
        healthy_threshold = 1;
        grpc_health_check = { };
      }
    ];
    load_assignment = {
      cluster_name = system;
      endpoints = [ { lb_endpoints = map endpoint workers; } ];
    };
  };

  route = system: {
    match = {
      prefix = "/";
      grpc = { };
    }
    // lib.optionalAttrs (system != cfg.defaultSystem) {
      headers = [
        {
          name = "x-nix-system";
          string_match.exact = system;
        }
      ];
    };
    route = {
      cluster = system;
      # Builds run for hours.
      timeout = "0s";
      hash_policy = [ { header.header_name = "x-nix-drv"; } ];
    };
  };

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
      description = "host:port of farm workers per system.";
    };

    defaultSystem = lib.mkOption {
      type = lib.types.str;
      default = lib.head systems;
      defaultText = lib.literalMD "first attribute of `workers`";
      description = "Cluster for requests without or with an unknown `x-nix-system` (store queries, uploads, `builtin`).";
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
        Log one line per connection and per request to the journal, including
        TLS handshake failures (`DOWNSTREAM_TRANSPORT_FAILURE_REASON`) and the
        client certificate subject.
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
                        access_log = lib.optional cfg.accessLog (stderrLog "rpc peer=%DOWNSTREAM_REMOTE_ADDRESS% subject=\"%DOWNSTREAM_PEER_SUBJECT%\" %REQ(:PATH)% system=%REQ(x-nix-system)% upstream=%UPSTREAM_HOST% grpc=%GRPC_STATUS% flags=%RESPONSE_FLAGS% ms=%DURATION%");
                        stream_idle_timeout = "0s";
                        http2_protocol_options.max_concurrent_streams = cfg.maxStreams;
                        forward_client_cert_details = "SANITIZE_SET";
                        set_current_client_cert_details.subject = true;
                        route_config.virtual_hosts = [
                          {
                            name = "farm";
                            domains = [ "*" ];
                            routes = map route ordered;
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
          clusters = lib.mapAttrsToList cluster cfg.workers;
        };
      };
    };
  };
}
