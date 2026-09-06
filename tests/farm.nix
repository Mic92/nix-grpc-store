# Farm end to end: niks3 + S3, two farm workers behind two load
# balancers at opposite ends: envoy (L7, MAGLEV on x-nix-drv, gRPC health)
# and nginx stream (L4, knows nothing). Both must yield correct builds.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
}:

let
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  tokenFile = pkgs.writeText "niks3-token" apiToken;
  s3Key = "rustfsadmin";
  signingSecretKey = pkgs.writeText "key" "farm-test-1:1/icU6Hlts+rG2LxnM8NoIMcrLWAzdCgJEOLjewE8DxGQKUPC9+LF07Ci6sEjhQP2G50TfF9TkQFBwwVRW5FXw==";
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://cache:5751";
  niks3Pkgs = niks3.packages.${pkgs.stdenv.hostPlatform.system};

  common = {
    virtualisation.memorySize = 1536;
    nix.package = nixPkgs.nix-everything;
    nix.settings.experimental-features = [ "nix-command" ];
  };

  worker =
    { config, lib, ... }:
    {
      imports = [
        common
        module
      ];
      services.nix-grpc-daemon = {
        enable = true;
        listen = "[::]:50051";
        logLevel = "debug";
        idleTimeout = null;
        package = config.programs.nix-grpc-store.package;
        farm = {
          enable = true;
          niks3Package = niks3Pkgs.niks3;
          inherit niks3Url;
          tokenFile = toString tokenFile;
          cacheUrl = niks3Url;
          publicKeys = [ signingPublicKey ];
          minFree = "200M";
        };
      };
      programs.nix-grpc-store.enable = true;
      networking.firewall.allowedTCPPorts = [ 50051 ];
    };

  # Distinct name per run so the cache never already has it.
  jobExpr = pkgs.writeText "job.nix" ''
    { tag }:
    let
      mk = name: deps: derivation {
        inherit name;
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${name}-''${tag} ''${toString deps} > $out" ];
      };
      a = mk "farm-a" [ ];
      b = mk "farm-b" [ a ];
    in
    mk "farm-top" [ a b ]
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
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-farm";
  globalTimeout = 900;

  nodes = {
    cache =
      { ... }:
      {
        imports = [
          common
          niks3.nixosModules.niks3
        ];
        services.niks3 = {
          enable = true;
          package = niks3Pkgs.niks3;
          httpAddr = "0.0.0.0:5751";
          apiTokenFile = toString tokenFile;
          signKeyFiles = [ signingSecretKey ];
          readProxy.enable = true;
          s3 = {
            endpoint = "cache:9000";
            bucket = "farm";
            useSSL = false;
            accessKeyFile = pkgs.writeText "ak" s3Key;
            secretKeyFile = pkgs.writeText "sk" s3Key;
          };
        };
        systemd.services.rustfs = {
          wantedBy = [ "multi-user.target" ];
          serviceConfig = {
            ExecStart = "${pkgs.rustfs}/bin/rustfs --address 0.0.0.0:9000 --access-key ${s3Key} --secret-key ${s3Key} /var/lib/rustfs";
            StateDirectory = "rustfs";
            DynamicUser = true;
          };
        };
        systemd.services.rustfs-bucket = {
          requires = [ "rustfs.service" ];
          after = [ "rustfs.service" ];
          before = [ "niks3.service" ];
          requiredBy = [ "niks3.service" ];
          environment = {
            S3_ENDPOINT_URL = "http://cache:9000";
            AWS_ACCESS_KEY_ID = s3Key;
            AWS_SECRET_ACCESS_KEY = s3Key;
          };
          path = [ pkgs.s5cmd ];
          script = ''
            for i in $(seq 60); do s5cmd ls && break; sleep 1; done
            s5cmd mb s3://farm || true
          '';
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
          };
        };
        networking.firewall.allowedTCPPorts = [
          5751
          9000
        ];
      };

    worker1 = worker;
    worker2 = worker;

    lb =
      { ... }:
      {
        imports = [
          common
          module
        ];
        services.nix-grpc-farm-lb = {
          enable = true;
          workers.${pkgs.stdenv.hostPlatform.system} = [
            "worker1:50051"
            "worker2:50051"
          ];
        };
        services.nginx = {
          enable = true;
          streamConfig = ''
            upstream farm {
              server worker1:50051;
              server worker2:50051;
            }
            server {
              listen 50052;
              proxy_pass farm;
            }
          '';
        };
        networking.firewall.allowedTCPPorts = [
          50051
          50052
        ];
        environment.systemPackages = [ pkgs.grpc-health-probe ];
      };

    client =
      { ... }:
      {
        imports = [
          common
          module
        ];
        programs.nix-grpc-store.enable = true;
        nix.settings.substituters = pkgs.lib.mkForce [ ];
      };
  };

  testScript = ''
    start_all()
    cache.wait_for_unit("niks3.service")
    cache.wait_for_open_port(5751)
    for w in [worker1, worker2]:
        w.wait_for_unit("nix-grpc-daemon.socket")
    lb.wait_for_unit("envoy.service")
    lb.wait_for_open_port(50051)
    lb.wait_for_open_port(50052)
    # Envoy only routes to endpoints that passed a gRPC health check.
    for w in ["worker1", "worker2"]:
        lb.wait_until_succeeds(f"grpc-health-probe -addr {w}:50051", timeout=180)
    lb.wait_until_succeeds("grpc-health-probe -addr localhost:50051", timeout=60)

    envoy = "grpc://lb:50051?insecure=1"
    l4 = "grpc://lb:50052?insecure=1"

    def build(store: str, tag: str) -> str:
        client.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag {tag} >&2")
        return client.succeed(f"nix eval --raw -f ${jobExpr} --argstr tag {tag} outPath").strip()

    def holders(path: str) -> int:
        return sum(w.execute(f"test -e {path}")[0] == 0 for w in [worker1, worker2])

    with subtest("without --eval-store the farm refuses and hints"):
        out = client.fail(f"nix build --store '{envoy}' -f ${jobExpr} --argstr tag t0 2>&1")
        assert "--eval-store" in out, out

    for name, store in [("envoy", envoy), ("l4", l4)]:
        with subtest(f"{name}: fan-out build lands in the cache"):
            top = build(store, name)
            assert holders(top) == 1, "exactly one worker built it"
            # The client store has nothing. The cache does.
            client.fail(f"test -e {top}")
            client.succeed(f"nix copy --from ${niks3Url} --no-check-sigs {top} && grep farm-top-{name} {top}")

        with subtest(f"{name}: repeat is answered from the claim without building"):
            for w in [worker1, worker2]:
                w.succeed(f"nix-store --delete {top}")
            build(store, name)
            assert holders(top) == 0, "no worker rebuilt it"

    with subtest("interrupting the client stops the build on the worker"):
        # [6] keeps the probe from matching its own command line.
        builder = "pgrep -f 'read -t [6]0 x'"

        def building() -> bool:
            return any(w.execute(builder)[0] == 0 for w in [worker1, worker2])

        client.succeed(f"systemd-run --unit intr nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag intr")
        retry(lambda _: building(), timeout_seconds=60)
        client.succeed("systemctl kill -s INT intr")
        retry(lambda _: not building(), timeout_seconds=20)

    with subtest("low disk drains a worker and builds go to the other"):
        # Leave less than minFree on worker1.
        worker1.succeed("fallocate -l $(( $(df --output=avail -B1 /nix/store | tail -1) - 100*1024*1024 )) /nix/.rw-store/fill")
        lb.wait_until_succeeds("curl -sf localhost:9901/clusters | grep -q failed_active_hc", timeout=60)
        top = build(envoy, "drain")
        worker1.fail(f"test -e {top}")
        worker1.succeed("rm /nix/.rw-store/fill")
        lb.wait_until_fails("curl -sf localhost:9901/clusters | grep -q failed_active_hc", timeout=60)
  '';
}
