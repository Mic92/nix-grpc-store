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
  # Workers of a farm whose scheduler and balancer run outside the cluster,
  # reached through a port on their node.
  valuesExternal = lib.recursiveUpdate values {
    scheduler.external = "farm.example.com:50051";
    workers.x86 = {
      hostPort = 50052;
      metricsHostPort = 9465;
      advertise = {
        n1 = "10.0.0.1";
        n2 = "10.0.0.2";
      };
    };
    workers.arm.hostPort = 50052;
  };
  # A Secret mounted into the build container of every group.
  valuesExtraVolumes = lib.recursiveUpdate values {
    workerDefaults = {
      extraVolumes = [
        {
          name = "nix-secrets";
          secret.secretName = "nix-github";
        }
      ];
      extraVolumeMounts = [
        {
          name = "nix-secrets";
          mountPath = "/etc/nix/secrets";
          readOnly = true;
        }
      ];
    };
  };
  # formats.yaml emits a %YAML directive that helm rejects. JSON is YAML.
  json = pkgs.formats.json { };
  # What tests/helm_test.py reads, by name.
  renders = {
    default = { inherit values; };
    runtime-class = {
      inherit values;
      set = "sandbox.runtimeClassName=cgroup-writable";
    };
    other-upstream-cache = {
      inherit values;
      set = "upstreamCaches.urls={https://other}";
    };
    in-cluster.values = valuesInCluster;
    cert-manager.values = valuesCertManager;
    public-cert-manager.values = valuesPublicCertManager;
    public-secret.values = valuesPublicSecret;
    external.values = valuesExternal;
    extra-volumes.values = valuesExtraVolumes;
  }
  // lib.listToAttrs (
    lib.imap0 (idx: extra: {
      name = "extra-${toString idx}";
      value.values = extra;
    }) extraValues
  );
  render =
    name:
    {
      values,
      set ? null,
    }:
    let
      args = lib.escapeShellArgs (
        [
          "chart"
          "-f"
          (json.generate "values.json" values)
        ]
        ++ lib.optionals (set != null) [
          "--set"
          set
        ]
      );
    in
    ''
      helm lint --strict ${args}
      helm template t ${args} --api-versions monitoring.coreos.com/v1/PodMonitor > renders/${name}.yaml
    '';

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

in
pkgs.runCommand "nix-grpc-farm-helm-check"
  {
    nativeBuildInputs = [
      pkgs.kubernetes-helm
      (pkgs.python3.withPackages (ps: [
        ps.pytest
        ps.pyyaml
      ]))
    ];
    HELM_RENDERS = "renders";
  }
  ''
    cp -r ${chart} chart && chmod -R u+w chart
    mkdir renders
    ${lib.concatStrings (lib.mapAttrsToList render renders)}
    cp ${nixosJson} renders/envoy-nixos.json
    pytest -vv -p no:cacheprovider ${./helm_test.py}
    touch $out
  ''
