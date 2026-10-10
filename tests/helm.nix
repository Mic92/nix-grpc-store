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
      };
      arm = {
        system = "aarch64-linux";
        spread = "required";
      };
    };
    defaultGroup = "x86";
    scheduler.replicas = 2;
    lb.accessLog = true;
    grafanaDashboard.enabled = true;
    metrics.podMonitor.enabled = true;
    auth.workloadIdentity = {
      enabled = true;
      allowedServiceAccounts = [ "ci:builder" ];
    };
  };
  # cert-manager PKI and VictoriaMetrics scraping.
  valuesCertManager = lib.recursiveUpdate values {
    tls = {
      certManager.enabled = true;
      clientCA.existingSecret = "";
      lb.existingSecret = "";
      worker.existingSecret = "";
    };
    metrics.podMonitor.enabled = false;
    metrics.vmPodScrape.enabled = true;
    lb.service.appProtocol = "kubernetes.io/h2c";
  };
  # Public client-facing certificate next to the farm PKI, both ways to get one.
  valuesPublicCertManager = lib.recursiveUpdate valuesCertManager {
    tls.certManager.public = {
      issuerRef = {
        name = "letsencrypt";
        kind = "ClusterIssuer";
      };
      dnsNames = [ "farm.example.com" ];
    };
  };
  valuesPublicSecret = lib.recursiveUpdate values { tls.public.existingSecret = "public"; };
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
  allValuesFiles = map (json.generate "values.json") (
    [
      values
      valuesInCluster
      valuesCertManager
      valuesPublicCertManager
      valuesPublicSecret
    ]
    ++ extraValues
  );

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
            options.warnings = lib.mkOption { type = lib.types.anything; };
            config._module.args.pkgs = pkgs;
            config.services.nix-grpc-farm-lb = {
              enable = true;
              defaultSystem = "x86_64-linux";
              accessLog = true;
              systems = [
                "x86_64-linux"
                "aarch64-linux"
              ];
              scheduler = [
                "HOST-sched0:50051"
                "HOST-sched1:50051"
              ];
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
    def names: {"x86_64-linux": "x86", "aarch64-linux": "arm"};
    def ren: . as $n | (names[$n] // $n);
    # SDS resource files: a ConfigMap path in the chart, a store path in NixOS.
    def sds: walk(if type == "object" and has("path_config_source") then .path_config_source.path = "X" else . end);
    .static_resources | sds
    | .clusters |= (map(.name |= ren
                        | if has("load_assignment") then
                            .load_assignment.cluster_name |= ren
                            | .load_assignment.endpoints[0].lb_endpoints |= map(.endpoint.address.socket_address.address = "X" | .endpoint.address.socket_address |= del(.ipv4_compat))
                          else . end
                        | del(.dns_lookup_family))
                    | sort_by(.name))
    | .listeners[0].address.socket_address |= (del(.ipv4_compat) | .address = "X")
    | .listeners[0].filter_chains[0].filters[0].typed_config.route_config.virtual_hosts[0].routes |= map(if has("route") then .route.cluster |= ren else . end)
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
      helm template t chart -f "$v" --api-versions monitoring.coreos.com/v1/PodMonitor | yq -e '.kind' > /dev/null
    done
    helm template t chart -f ${valuesFile} --api-versions monitoring.coreos.com/v1/PodMonitor > out.yaml

    yq -r 'select(.kind == "ConfigMap" and .metadata.name == "t-nix-grpc-farm-lb") | .data."envoy.json"' out.yaml \
      | jq -S -f ${normalize} > chart.json
    jq -S -f ${normalize} < ${nixosJson} > nixos.json
    if ! diff -u nixos.json chart.json; then
      echo "envoy config from the chart drifted from nixos/lb.nix" >&2
      exit 1
    fi
    jq -e '[.clusters[] | select(.name == "sched") | .common_lb_config.healthy_panic_threshold.value] == [0]' chart.json

    # Default placement: workers spread, scheduler replicas on distinct nodes.
    pick() { yq "select(.kind == \"$1\" and .metadata.name == \"$2\") | $3" "$4"; }
    test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.topologySpreadConstraints[0].topologyKey' out.yaml)" = kubernetes.io/hostname
    test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.topologySpreadConstraints[0].whenUnsatisfiable' out.yaml)" = ScheduleAnyway
    test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.topologySpreadConstraints[0].nodeTaintsPolicy' out.yaml)" = Honor
    test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.topologySpreadConstraints[0].matchLabelKeys[0]' out.yaml)" = pod-template-hash
    test "$(pick Deployment t-nix-grpc-farm-worker-arm '.spec.template.spec.topologySpreadConstraints[0].whenUnsatisfiable' out.yaml)" = DoNotSchedule
    test "$(pick Deployment t-nix-grpc-farm-scheduler-0 '.spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey' out.yaml)" = kubernetes.io/hostname

    test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.runtimeClassName' out.yaml)" = null
    helm template t chart -f ${valuesFile} --set sandbox.runtimeClassName=cgroup-writable --api-versions monitoring.coreos.com/v1/PodMonitor > rc.yaml
    test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.runtimeClassName' rc.yaml)" = cgroup-writable

    # cert-manager mode: chart-named Secrets everywhere, worker cert covers every scheduler Service.
    helm template t chart -f ${json.generate "values.json" valuesCertManager} > cm.yaml
    helm template t chart -f ${json.generate "values.json" valuesInCluster} --api-versions monitoring.coreos.com/v1/PodMonitor > in-cluster.yaml
    # The worker must not remain Ready after its separate nix-daemon container dies.
    for f in out.yaml cm.yaml in-cluster.yaml; do
      test "$(pick Deployment t-nix-grpc-farm-worker-x86 '.spec.template.spec.containers[] | select(.name == "nix-grpc-daemon") | .readinessProbe.exec.command | join(" ")' "$f")" = 'nix store info --store daemon'
    done
    for n in t-nix-grpc-farm-scheduler-0.default.svc t-nix-grpc-farm-scheduler-1.default.svc t-nix-grpc-farm.default.svc; do
      pick Certificate t-nix-grpc-farm-worker '.spec.dnsNames[]' cm.yaml | grep -qx "$n"
    done
    test "$(pick Certificate t-nix-grpc-farm-lb '.spec.commonName' cm.yaml)" = lb
    test "$(pick Service t-nix-grpc-farm '.spec.ports[0].appProtocol' cm.yaml)" = kubernetes.io/h2c
    test "$(pick Service t-nix-grpc-farm '.spec.ports[0].appProtocol' out.yaml)" = null
    test "$(pick Deployment t-nix-grpc-farm-scheduler-0 '.spec.template.spec.volumes[] | select(.name == "tls") | .secret.secretName' cm.yaml)" = t-nix-grpc-farm-worker-tls
    grep -q "kind: VMPodScrape" cm.yaml
    ! grep -q "kind: PodMonitor" cm.yaml

    # A public certificate faces clients, the farm-CA one still faces the nodes.
    envoyCerts() { yq -r 'select(.kind == "ConfigMap" and .metadata.name == "t-nix-grpc-farm-lb") | .data."envoy.json"' "$1" \
      | jq -c '[.static_resources.listeners[0].filter_chains[0].transport_socket.typed_config.common_tls_context.tls_certificate_sds_secret_configs[0].name, ([.static_resources.clusters[].transport_socket.typed_config.common_tls_context.tls_certificate_sds_secret_configs[0].name] | unique)]'; }
    test "$(envoyCerts out.yaml)" = '["lb",["lb"]]'
    ! grep -q "name: t-nix-grpc-farm-public" cm.yaml
    helm template t chart -f ${json.generate "values.json" valuesPublicCertManager} > public-cm.yaml
    test "$(envoyCerts public-cm.yaml)" = '["public",["lb"]]'
    test "$(pick Certificate t-nix-grpc-farm-public '.spec.issuerRef.name' public-cm.yaml)" = letsencrypt
    test "$(pick Certificate t-nix-grpc-farm-public '.spec.dnsNames[0]' public-cm.yaml)" = farm.example.com
    test "$(pick Deployment t-nix-grpc-farm-lb '.spec.template.spec.volumes[] | select(.name == "tls-public") | .secret.secretName' public-cm.yaml)" = t-nix-grpc-farm-public-tls
    helm template t chart -f ${json.generate "values.json" valuesPublicSecret} --api-versions monitoring.coreos.com/v1/PodMonitor > public-secret.yaml
    test "$(envoyCerts public-secret.yaml)" = '["public",["lb"]]'
    test "$(pick Deployment t-nix-grpc-farm-lb '.spec.template.spec.volumes[] | select(.name == "tls-public") | .secret.secretName' public-secret.yaml)" = public

    touch $out
  ''
