# Helm chart in k3s. Workers and envoy run from our images, niks3, rustfs
# and postgres as NixOS services on the node. Workers authenticate to niks3
# with their service account token, and the client uses one for the farm.
# The test builds through the balancer, runs a Job with the client image,
# fills a store to trigger harmonia-gc, and deletes a worker pod mid-build.
{
  pkgs,
  niks3,
  chart,
  dockerWorker,
  dockerLb,
  dockerClient,
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
    issue lb-renewed lb "IP:127.0.0.1,IP:${hostIP},DNS:nix-grpc-farm.farm.svc,DNS:renewed.example"
    issue worker worker "DNS:worker,DNS:nix-grpc-farm-scheduler.farm.svc"
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
  nfbFlake = pkgs.writeText "flake.nix" ''
    {
      outputs = _: {
        checks.${system}.t = derivation {
          name = "nfb-ci";
          system = "${system}";
          builder = "/bin/sh";
          args = [ "-c" "echo > $out" ];
        };
      };
    }
  '';
  slowExpr = pkgs.writeText "slow.nix" ''
    { tag }:
    derivation {
      name = "slow-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "read -t 20 x < /dev/zero; echo > $out" ];
    }
  '';
  groupName = lib.replaceStrings [ "_" ] [ "-" ] system;
  # The pod spec from docs/kubernetes.md "Connect CI jobs in the cluster".
  ciJob = (pkgs.formats.json { }).generate "ci-job.json" {
    apiVersion = "batch/v1";
    kind = "Job";
    metadata = {
      name = "ci";
      namespace = "ci";
    };
    spec = {
      backoffLimit = 0;
      template.spec = {
        serviceAccountName = "builder";
        restartPolicy = "Never";
        containers = [
          {
            name = "build";
            image = "${dockerClient.imageName}:${dockerClient.imageTag}";
            env = [
              {
                name = "FARM";
                value = "grpc://nix-grpc-farm.farm.svc:50051?token-file=/var/run/secrets/nix-grpc-farm/token&ca-cert=/var/run/secrets/nix-grpc-farm/ca.crt";
              }
              {
                name = "NIX_CONFIG";
                value = ''
                  substituters = ${niks3Url}
                  trusted-public-keys = ${signingPublicKey}
                '';
              }
            ];
            command = [
              "bash"
              "-euc"
              ''
                nix build -L --store "$FARM" --eval-store auto -f /job/job.nix --argstr tag ci
                cp /job/flake.nix /tmp && cd /tmp && git init -q && git add flake.nix
                nix-fast-build --store "$FARM" --no-download --skip-cached
                out=$(nix eval --raw .#checks.${system}.t.outPath)
                if test -e "$out"; then echo "$out was downloaded" >&2; exit 1; fi
              ''
            ];
            volumeMounts = [
              {
                name = "nix-grpc-farm";
                mountPath = "/var/run/secrets/nix-grpc-farm";
                readOnly = true;
              }
              {
                name = "job";
                mountPath = "/job";
              }
            ];
          }
        ];
        volumes = [
          {
            name = "nix-grpc-farm";
            projected.sources = [
              {
                serviceAccountToken = {
                  audience = "nix-grpc-farm";
                  path = "token";
                };
              }
              {
                configMap = {
                  name = "nix-grpc-farm-ca";
                  items = [
                    {
                      key = "ca.crt";
                      path = "ca.crt";
                    }
                  ];
                };
              }
            ];
          }
          {
            name = "job";
            configMap.name = "job";
          }
        ];
      };
    };
  };
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
    # Above the slow build (20 s) so a pod delete can drain it.
    terminationGracePeriodSeconds = 60;
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
        pkgs.openssl
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
          dockerClient
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
    from datetime import timedelta
    def sec(n: int) -> timedelta:
        return timedelta(seconds=n)

    def kubectl(args: str) -> str:
        return machine.succeed("kubectl -n farm " + args)
    def pod_ip(selector: str) -> str:
        return kubectl(f"get pod {selector} -o jsonpath='{{.items[0].status.podIP}}{{.status.podIP}}'").strip()
    def rollout(deployment: str) -> None:
        machine.wait_until_succeeds(f"kubectl -n farm rollout status deployment {deployment} --timeout=10s", timeout=sec(420))
    def dump() -> None:
        machine.execute(
            "{ kubectl get pods -A; kubectl get events -A --sort-by=.lastTimestamp | tail -40;"
            " kubectl -n kube-system logs -l batch.kubernetes.io/job-name=helm-install-nix-grpc-farm --tail=40;"
            " kubectl -n farm describe pods | tail -80;"
            " kubectl -n farm logs -l app.kubernetes.io/component=worker --all-containers --prefix --tail=30;"
            " kubectl -n farm logs -l app.kubernetes.io/component=scheduler --tail=30; } >&2")

    machine.wait_for_unit("k3s.service")
    # Pods wait in ContainerCreating until these exist.
    machine.wait_until_succeeds("kubectl get ns farm", timeout=sec(180))
    kubectl("create secret tls farm-lb-tls --cert=${certs}/lb.crt --key=${certs}/lb.key")
    kubectl("create secret tls farm-worker-tls --cert=${certs}/worker.crt --key=${certs}/worker.key")
    kubectl("create secret generic farm-ca --from-file=ca.crt=${certs}/ca.crt")

    with subtest("chart deploys and workers become Ready"):
        try:
            for d in ["lb", "scheduler", "worker-${groupName}"]:
                rollout(f"nix-grpc-farm-{d}")
        except Exception:
            dump()
            raise
        machine.wait_for_unit("niks3.service")
        # envoy has health-checked both workers and the scheduler
        lb_ip = pod_ip("-l app.kubernetes.io/component=lb")
        machine.wait_until_succeeds(f"test $(curl -sf {lb_ip}:9901/clusters | grep -c 'health_flags::healthy') -ge 3", timeout=sec(120))
        sched_ip = pod_ip("-l app.kubernetes.io/component=scheduler")
        machine.wait_until_succeeds(f"curl -sf http://{sched_ip}:9464/metrics | grep -qx 'nix_grpc_sched{{kind=\"workers\"}} 2'", timeout=sec(120))

    pods = kubectl("get pods -l app.kubernetes.io/component=worker --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'").split()
    assert len(pods) == 2, pods

    machine.succeed("kubectl -n ci create token builder --audience nix-grpc-farm > /tmp/builder.jwt")
    store = "grpc://127.0.0.1:${toString nodePort}?ca-cert=${certs}/ca.crt&token-file=/tmp/builder.jwt"
    def build(expr: str, tag: str) -> str:
        return machine.succeed(f"nix build -L --store '{store}' --eval-store auto -f {expr} --argstr tag {tag} 2>&1")
    def out_path(expr: str, tag: str) -> str:
        return machine.succeed(f"nix eval --raw -f {expr} --argstr tag {tag} outPath").strip()

    with subtest("build through the balancer lands in the cache"):
        build("${jobExpr}", "t1")
        top = out_path("${jobExpr}", "t1")
        machine.fail(f"test -e {top}")
        machine.succeed(f"nix copy --from ${niks3Url} {top} && grep k3s-top-t1 {top}")
        # --worker-name from the downward API
        out = build("${jobExpr}", "t2")
        assert "machine/nix-grpc-farm-worker-" in out, out

    with subtest("a Job with the client image builds through the farm with its service account"):
        machine.succeed("kubectl -n ci create configmap nix-grpc-farm-ca --from-file=ca.crt=${certs}/ca.crt")
        machine.succeed("kubectl -n ci create configmap job --from-file=job.nix=${jobExpr} --from-file=flake.nix=${nfbFlake}")
        machine.succeed("kubectl apply -f ${ciJob}")
        try:
            # Complete or Failed, whichever comes first.
            machine.wait_until_succeeds("kubectl -n ci get job ci -o jsonpath='{.status.conditions[*].type}' | grep -qE 'Complete|Failed'", timeout=sec(180))
            machine.succeed("kubectl -n ci get job ci -o jsonpath='{.status.succeeded}' | grep -qx 1")
        finally:
            machine.execute("kubectl -n ci logs job/ci >&2")
        machine.succeed(f"curl -sf ${niks3Url}/{out_path('${jobExpr}', 'ci').removeprefix('/nix/store/').split('-')[0]}.narinfo > /dev/null")

    with subtest("a renewed balancer certificate is served without a rollout"):
        kubectl("create secret tls farm-lb-tls --cert=${certs}/lb-renewed.crt --key=${certs}/lb-renewed.key --dry-run=client -o yaml | kubectl -n farm apply -f -")
        # kubelet syncs Secret volumes about once a minute.
        lb_pod = kubectl("get pods -l app.kubernetes.io/component=lb -o jsonpath='{.items[0].metadata.name}'")
        machine.wait_until_succeeds(f"kubectl -n farm exec {lb_pod} -- cat /etc/envoy/tls/lb/tls.crt | openssl x509 -noout -ext subjectAltName | grep -q renewed.example", timeout=sec(120))
        machine.wait_until_succeeds(
            "openssl s_client -connect 127.0.0.1:${toString nodePort} </dev/null 2>/dev/null "
            "| openssl x509 -noout -ext subjectAltName | grep -q renewed.example",
            timeout=sec(20),
        )
        build("${jobExpr}", "t-renewed")

    with subtest("metrics carry build_info"):
        machine.succeed(f"curl -sf http://{pod_ip(pods[0])}:9464/metrics | grep -E 'nix_grpc_build_info\\{{.*worker=\"machine/{pods[0]}\"'")

    with subtest("harmonia-gc frees space and the worker stays Ready"):
        pod = pods[0]
        def sh(container: str, cmd: str) -> str:
            return kubectl(f"exec {pod} -c {container} -- sh -c {shlex.quote(cmd)}")
        # emptyDir sizeLimit is not a filesystem, so gc sees the node's disk.
        # Garbage for gc to find, then leave ~1G free: below ensureFree (2G), above minFree (500M).
        avail = int(sh("gc", "df -B1 /nix/store | awk 'NR==2 {print $4}'"))
        sh("nix-daemon", "for i in 1 2 3; do echo $i > /tmp/g$i; nix-store --add /tmp/g$i; done")
        sh("gc", f"fallocate -l {avail - 1024**3} /nix/fill")
        machine.wait_until_succeeds(f"kubectl -n farm logs {pod} -c gc --since=30s | grep -q 'store paths deleted'", timeout=sec(60))
        sh("gc", "rm /nix/fill")
        ready = kubectl(f"get pod {pod} -o jsonpath='{{.status.conditions[?(@.type==\"Ready\")].status}}'").strip()
        assert ready == "True", ready
        sh("gc", "command -v nix-daemon && command -v niks3")  # tools survived gc

    with subtest("deleting a worker pod mid-build drains it: the build finishes there and publishes"):
        machine.succeed(f"systemd-run --unit slow nix build -L --store '{store}' --eval-store auto -f ${slowExpr} --argstr tag s1")
        def holder() -> str | None:
            # [2] keeps the probe from matching itself.
            return next((p for p in pods if machine.execute(f"kubectl -n farm exec {p} -c nix-daemon -- pgrep -f 'read -t [2]0 x'")[0] == 0), None)
        retry(lambda _: holder() is not None, timeout=sec(120))
        busy = holder()
        kubectl(f"delete pod {busy} --wait=false")
        machine.sleep(duration=sec(5))
        assert holder() == busy, "pod delete killed the build instead of draining"
        machine.wait_until_succeeds("! systemctl is-active slow", timeout=sec(300))
        machine.succeed("systemctl show -p Result --value slow | grep -qx success || { journalctl -u slow >&2; false; }")
        machine.fail("journalctl -u slow | grep -q 'asking the scheduler again'")
        narinfo = out_path("${slowExpr}", "s1").removeprefix("/nix/store/").split("-")[0] + ".narinfo"
        machine.succeed(f"curl -sf ${niks3Url}/{narinfo} > /dev/null")
  '';
}
