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

  endpoint = addr: {
    endpoint.address.socket_address =
      let
        m = builtins.match "(.*):([0-9]+)" addr;
      in
      {
        address = lib.removeSuffix "]" (lib.removePrefix "[" (builtins.elemAt m 0));
        port_value = lib.toInt (builtins.elemAt m 1);
      };
  };

  cluster = system: workers: {
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
    health_checks = [
      {
        timeout = "2s";
        interval = "5s";
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

    maxStreams = lib.mkOption {
      type = lib.types.ints.positive;
      default = 1024;
      description = "HTTP/2 streams per connection, both directions. Must exceed client `max-builds`.";
    };
  };

  config = lib.mkIf cfg.enable {
    services.envoy = {
      enable = true;
      package = lib.mkDefault pkgs.envoy-bin;
      # Validation resolves STRICT_DNS names, which the build sandbox cannot.
      requireValidConfig = false;
      settings = {
        static_resources = {
          listeners = [
            {
              name = "farm";
              address = (endpoint cfg.listen).endpoint.address;
              filter_chains = [
                {
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
              ];
            }
          ];
          clusters = lib.mapAttrsToList cluster cfg.workers;
        };
      };
    };
  };
}
