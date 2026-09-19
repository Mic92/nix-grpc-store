# Chart checks that need no cluster: lint, render, and envoy config parity
# with nixos/lb.nix for the same topology.
{
  pkgs,
  lbModule,
  chart,
  extraValues ? [ ], # more value sets that must lint
}:
let
  inherit (pkgs) lib;

  values = {
    niks3 = {
      serverURL = "http://niks3";
      cacheURL = "http://cache";
      publicKeys = [ "cache:AAAA" ];
      auth.existingSecret = "tok";
    };
    tls = {
      clientCA.existingSecret = "ca";
      lb.existingSecret = "lb";
      worker.existingSecret = "worker";
      worker.serverName = "worker";
    };
    auth.accessRules = [
      {
        cn = "ci-*";
        role = "trusted";
      }
    ];
    workers = {
      x86 = {
        system = "x86_64-linux";
        replicas = 2;
        features = [ "kvm" ];
      };
      arm = {
        system = "aarch64-linux";
      };
    };
    defaultGroup = "x86";
    lb.accessLog = true;
    grafanaDashboard.enabled = true;
    metrics.podMonitor.enabled = true;
    auth.workloadIdentity = {
      enabled = true;
      allowedServiceAccounts = [ "ci:builder" ];
    };
  };
  # Service account tokens both ways, no TLS.
  valuesInCluster = lib.recursiveUpdate values {
    niks3.auth = {
      existingSecret = "";
      serviceAccountToken.enabled = true;
    };
    tls = {
      clientCA.existingSecret = "";
      lb.existingSecret = "";
      worker.existingSecret = "";
    };
    auth.accessRules = [ ];
  };
  # formats.yaml emits a %YAML directive that helm rejects. JSON is YAML.
  json = pkgs.formats.json { };
  valuesFile = json.generate "values.json" values;
  allValuesFiles = map (json.generate "values.json") ([ values valuesInCluster ] ++ extraValues);

  # Same topology through the NixOS module.
  nixosEnvoy =
    (lib.evalModules {
      modules = [
        lbModule
        (
          { lib, ... }:
          {
            options.services.envoy = lib.mkOption { type = lib.types.anything; };
            options.systemd = lib.mkOption { type = lib.types.anything; };
            options.assertions = lib.mkOption { type = lib.types.anything; };
            config._module.args.pkgs = pkgs;
            config.services.nix-grpc-farm-lb = {
              enable = true;
              defaultSystem = "x86_64-linux";
              accessLog = true;
              workers = {
                x86_64-linux = [ "HOST-x86:50051" ];
                aarch64-linux = [ "HOST-arm:50051" ];
              };
              features.x86_64-linux.kvm = [ "HOST-x86:50051" ];
              tls = {
                certFile = "/etc/envoy/tls/lb/tls.crt";
                keyFile = "/etc/envoy/tls/lb/tls.key";
                clientCaFile = "/etc/envoy/tls/ca/ca.crt";
                upstream = {
                  certFile = "/etc/envoy/tls/lb/tls.crt";
                  keyFile = "/etc/envoy/tls/lb/tls.key";
                  caFile = "/etc/envoy/tls/ca/ca.crt";
                  sni = "worker";
                };
              };
            };
          }
        )
      ];
    }).config.services.envoy.settings;
  nixosJson = (pkgs.formats.json { }).generate "envoy-nixos.json" (
    # mkIf leaves `admin` wrapped. Only static_resources is compared.
    { inherit (nixosEnvoy) static_resources; }
  );

  # Cluster and system names differ by design (chart keys groups, the module
  # keys systems) and endpoints are hostnames vs. a headless Service.
  normalize = pkgs.writeText "normalize.jq" ''
    def names: {"x86_64-linux": "x86", "aarch64-linux": "arm", "x86_64-linux/kvm": "x86/kvm"};
    def ren: . as $n | (names[$n] // $n);
    .static_resources
    | .clusters |= (map(.name |= ren | .load_assignment.cluster_name |= ren
                        | .load_assignment.endpoints[0].lb_endpoints |= map(.endpoint.address.socket_address.address = "X" | .endpoint.address.socket_address |= del(.ipv4_compat))
                        | del(.dns_lookup_family))
                    | sort_by(.name))
    | .listeners[0].address.socket_address |= (del(.ipv4_compat) | .address = "X")
    | .listeners[0].filter_chains[0].filters[0].typed_config.route_config.virtual_hosts[0].routes |= map(.route.cluster |= ren)
  '';
in
pkgs.runCommand "nix-grpc-farm-helm-check"
  {
    nativeBuildInputs = [
      pkgs.kubernetes-helm
      pkgs.yq-go
      pkgs.jq
    ];
  }
  ''
    cp -r ${chart} chart && chmod -R u+w chart
    for v in ${toString allValuesFiles}; do
      helm lint --strict chart -f "$v"
      helm template t chart -f "$v" | yq -e '.kind' > /dev/null
    done
    helm template t chart -f ${valuesFile} > out.yaml

    yq -r 'select(.kind == "ConfigMap" and .metadata.name == "t-nix-grpc-farm-lb") | .data."envoy.json"' out.yaml \
      | jq -S -f ${normalize} > chart.json
    jq -S -f ${normalize} < ${nixosJson} > nixos.json
    if ! diff -u nixos.json chart.json; then
      echo "envoy config from the chart drifted from nixos/lb.nix" >&2
      exit 1
    fi

    touch $out
  ''
