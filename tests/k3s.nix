# Helm chart in k3s: worker + envoy from our images, niks3/rustfs/postgres
# as NixOS services on the node. Workers authenticate to niks3 with their
# service account token and the client uses one for the farm. Then: build
# through the balancer, fill a store to trigger harmonia-gc, kill a worker
# mid-build.
{
  pkgs,
  niks3,
  chart,
  dockerWorker,
  dockerLb,
  nixPkgs,
  clientModule,
}:
let
  inherit (pkgs) lib;
  niks3Pkgs = niks3.packages.${pkgs.stdenv.hostPlatform.system};
  system = pkgs.stdenv.hostPlatform.system;

  chartTgz =
    pkgs.runCommand "nix-grpc-farm-chart.tgz" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
      ''
        cp -r ${chart} chart && chmod -R u+w chart
        helm package chart
        mv nix-grpc-farm-*.tgz $out
      '';

  hostIP = "192.168.1.1";
  nodePort = 30051;
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://${hostIP}:5751";

  # Bearer tokens need TLS on the client side, so the balancer and workers
  # get certificates. SANs cover the NodePort address and the headless
  # Service name envoy dials.
  certs = pkgs.runCommand "k3s-farm-certs" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout ca.key -out ca.crt -subj /CN=farm-ca
    issue() {
      openssl req -newkey rsa:2048 -nodes -keyout $1.key -out $1.csr -subj /CN=$2
      openssl x509 -req -in $1.csr -days 3650 -CA ca.crt -CAkey ca.key -set_serial 0x$(openssl rand -hex 8) \
        -extfile <(printf "subjectAltName=$3") -out $1.crt
    }
    issue lb lb "IP:127.0.0.1,IP:${hostIP},DNS:nix-grpc-farm.farm.svc"
    issue worker worker "DNS:worker"
  '';

  jobExpr = pkgs.writeText "job.nix" ''
    { tag }:
    let
      mk = name: deps: derivation {
        inherit name;
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${name}-''${tag} ''${toString deps} > $out" ];
      };
      a = mk "k3s-a" [ ];
      b = mk "k3s-b" [ a ];
    in
    mk "k3s-top" [ a b ]
  '';
  slowExpr = pkgs.writeText "slow.nix" ''
    { tag }:
    derivation {
      name = "slow-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "read -t 60 x < /dev/zero; echo > $out" ];
    }
  '';
  groupName = lib.replaceStrings [ "_" ] [ "-" ] system;
  chartValues = {
    image = {
      repository = dockerWorker.imageName;
      tag = dockerWorker.imageTag;
    };
    lbImage = {
      repository = dockerLb.imageName;
      tag = dockerLb.imageTag;
    };
    niks3 = {
      serverURL = niks3Url;
      cacheURL = niks3Url;
      publicKeys = [ signingPublicKey ];
      auth.serviceAccountToken.enabled = true;
    };
    auth.workloadIdentity = {
      enabled = true;
      allowedServiceAccounts = [ "ci:builder" ];
    };
    tls = {
      clientCA.existingSecret = "farm-ca";
      lb.existingSecret = "farm-lb-tls";
      worker.existingSecret = "farm-worker-tls";
      worker.serverName = "worker";
    };
    workers.${groupName} = {
      inherit system;
      replicas = 2;
      maxJobs = 2;
      minFree = "500M";
    };
    gc = {
      ensureFree = "2G";
      interval = 10;
    };
    lb = {
      replicas = 1;
      healthCheckInterval = "1s";
      service = {
        type = "NodePort";
        inherit nodePort;
      };
    };
    logLevel = "debug";
    terminationGracePeriodSeconds = 30;
  };
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-farm-k3s";
  # checks.helm lints these without booting a VM.
  passthru = { inherit chartValues; };
  globalTimeout = 1200;

  nodes.machine =
    { config, ... }:
    {
      imports = [
        clientModule
        (import ./lib/niks3-node.nix {
          inherit pkgs niks3;
          listenHost = hostIP;
          apiToken = "farm-token-that-is-at-least-36-characters-long";
        })
      ];
      virtualisation = {
        memorySize = 4096;
        cores = 4;
        diskSize = 16384;
      };
      networking.firewall.enable = false;
      assertions = [
        {
          assertion = config.networking.primaryIPAddress == hostIP;
          message = "hostIP";
        }
      ];
      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        substituters = lib.mkForce [ ];
        trusted-public-keys = [ signingPublicKey ];
      };
      programs.nix-grpc-store.enable = true;
      environment.systemPackages = [
        pkgs.kubectl
        pkgs.grpc-health-probe
      ];
      environment.sessionVariables.KUBECONFIG = "/etc/rancher/k3s/k3s.yaml";

      services.niks3 = {
        # What the niks3 chart's workloadIdentity helper renders, pointed at
        # k3s from outside the cluster.
        oidc.allowInsecure = true;
        oidc.providers.kubernetes = {
          audience = "niks3";
          jwksUrl = "https://127.0.0.1:6443/openid/v1/jwks";
          caFile = "/run/niks3-k8s-ca.crt";
          bearerTokenFile = "/run/niks3-k8s-token";
          boundSubject = [ "system:serviceaccount:farm:nix-grpc-farm" ];
          scopes = [ "write" ];
        };
      };
      # niks3 reads the issuer from a k8s token and the API server CA. In a
      # pod both come from the service account mount.
      systemd.services.niks3 = {
        after = [ "k3s.service" ];
        serviceConfig.ExecStartPre = [
          "+${pkgs.writeShellScript "k8s-token" ''
            for i in $(seq 120); do
              ${pkgs.kubectl}/bin/kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kube-system create token default > /run/niks3-k8s-token 2>/dev/null \
                && [ -s /run/niks3-k8s-token ] \
                && install -m0644 /var/lib/rancher/k3s/server/tls/server-ca.crt /run/niks3-k8s-ca.crt \
                && exit 0
              sleep 2
            done
            exit 1
          ''}"
        ];
        serviceConfig.TimeoutStartSec = 300;
      };

      services.k3s = {
        enable = true;
        role = "server";
        # The gc subtest fills the disk on purpose.
        extraFlags = [ "--kubelet-arg=eviction-hard=nodefs.available<1%,imagefs.available<1%" ];
        disable = [
          "traefik"
          "metrics-server"
          "local-storage"
        ];
        images = [
          config.services.k3s.package.airgap-images
          dockerWorker
          dockerLb
        ];
        manifests.ci.content = [
          {
            apiVersion = "v1";
            kind = "Namespace";
            metadata.name = "ci";
          }
          {
            apiVersion = "v1";
            kind = "Namespace";
            metadata.name = "farm";
          }
          {
            apiVersion = "v1";
            kind = "ServiceAccount";
            metadata = {
              name = "builder";
              namespace = "ci";
            };
          }
        ];
        autoDeployCharts.nix-grpc-farm = {
          package = chartTgz;
          targetNamespace = "farm";
          values = chartValues;
        };
      };
    };

  testScript = ''
    import shlex

    machine.wait_for_unit("k3s.service")
    kubectl = "kubectl -n farm "
    # Pods wait in ContainerCreating until these exist.
    machine.wait_until_succeeds("kubectl get ns farm", timeout=180)
    machine.succeed(
        kubectl + "create secret tls farm-lb-tls --cert=${certs}/lb.crt --key=${certs}/lb.key",
        kubectl + "create secret tls farm-worker-tls --cert=${certs}/worker.crt --key=${certs}/worker.key",
        kubectl + "create secret generic farm-ca --from-file=ca.crt=${certs}/ca.crt",
    )

    def dump(_ok: bool = False) -> None:
        machine.execute("{ kubectl get pods -A; kubectl get events -A --sort-by=.lastTimestamp | tail -40; kubectl -n kube-system logs -l batch.kubernetes.io/job-name=helm-install-nix-grpc-farm --tail=40; " + kubectl + "describe pods | tail -80; " + kubectl + "logs -l app.kubernetes.io/component=worker --all-containers --prefix --tail=30; } >&2")

    with subtest("chart deploys and workers become Ready"):
        try:
            machine.wait_until_succeeds(kubectl + "rollout status deployment nix-grpc-farm-lb --timeout=10s", timeout=420)
            machine.wait_until_succeeds(kubectl + "rollout status deployment nix-grpc-farm-worker-${groupName} --timeout=10s", timeout=420)
        except Exception:
            dump()
            raise
        machine.wait_for_unit("niks3.service")
        # envoy has health-checked both pods
        machine.wait_until_succeeds("test $(curl -sf $(" + kubectl + "get pod -l app.kubernetes.io/component=lb -o jsonpath='{.items[0].status.podIP}'):9901/clusters | grep -c 'health_flags::healthy') -ge 2", timeout=120)

    def worker_pods() -> list[str]:
        return machine.succeed(kubectl + "get pods -l app.kubernetes.io/component=worker --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'").split()
    pods = worker_pods()
    assert len(pods) == 2, pods

    machine.succeed("kubectl -n ci create token builder --audience nix-grpc-farm > /tmp/builder.jwt")
    store = "grpc://127.0.0.1:${toString nodePort}?ca-cert=${certs}/ca.crt&token-file=/tmp/builder.jwt"

    with subtest("build through the balancer lands in the cache"):
        machine.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag t1 >&2")
        top = machine.succeed("nix eval --raw -f ${jobExpr} --argstr tag t1 outPath").strip()
        machine.fail(f"test -e {top}")
        machine.succeed(f"nix copy --from ${niks3Url} {top} && grep k3s-top-t1 {top}")
        out = machine.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag t2 2>&1")
        # --worker-name from the downward API
        assert "machine/nix-grpc-farm-worker-" in out, out

    with subtest("metrics carry build_info"):
        ip = machine.succeed(kubectl + f"get pod {pods[0]} -o jsonpath='{{.status.podIP}}'").strip()
        machine.succeed(f"curl -sf http://{ip}:9464/metrics | grep -E 'nix_grpc_build_info\\{{.*worker=\"machine/{pods[0]}\"'")

    with subtest("harmonia-gc frees space and the worker stays Ready"):
        pod = pods[0]
        sh = lambda c, cmd: machine.succeed(kubectl + f"exec {pod} -c {c} -- sh -c {shlex.quote(cmd)}")
        avail = int(sh("gc", "df -B1 /nix/store | awk 'NR==2 {print $4}'").strip())
        # emptyDir sizeLimit is not a filesystem, so gc sees the node's disk.
        # Garbage for gc to find, then leave ~1G free: below ensureFree (2G), above minFree (500M).
        sh("nix-daemon", "for i in 1 2 3; do echo $i > /tmp/g$i; nix-store --add /tmp/g$i; done")
        sh("gc", f"fallocate -l {avail - 1024 * 1024 * 1024} /nix/fill")
        machine.wait_until_succeeds(kubectl + f"logs {pod} -c gc --since=30s | grep -E 'store paths deleted'", timeout=60)
        sh("gc", "rm /nix/fill")
        ready = machine.succeed(kubectl + f"get pod {pod} -o jsonpath='{{.status.conditions[?(@.type==\"Ready\")].status}}'").strip()
        assert ready == "True", ready
        sh("gc", "command -v nix-daemon && command -v niks3")  # tools survived gc

    with subtest("killing a worker mid-build bounces the build to the other"):
        machine.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${slowExpr} --argstr tag s1 > /tmp/slow.log 2>&1 & echo $! > /tmp/slow.pid")
        def building(_) -> bool:
            for p in pods:
                # [6] keeps the probe from matching itself.
                if machine.execute(kubectl + f"exec {p} -c nix-daemon -- pgrep -f 'read -t [6]0 x'")[0] == 0:
                    machine.succeed(f"echo {p} > /tmp/holder")
                    return True
            return False
        retry(building, timeout_seconds=120)
        holder = machine.succeed("cat /tmp/holder").strip()
        machine.succeed(kubectl + f"delete pod {holder} --wait=false")
        machine.wait_until_succeeds("! kill -0 $(cat /tmp/slow.pid) 2>/dev/null", timeout=300)
        out = machine.succeed("nix eval --raw -f ${slowExpr} --argstr tag s1 outPath").strip()
        narinfo = out.removeprefix("/nix/store/").split("-")[0] + ".narinfo"
        machine.succeed(f"curl -sf ${niks3Url}/{narinfo} > /dev/null || (cat /tmp/slow.log >&2; false)")
  '';
}
