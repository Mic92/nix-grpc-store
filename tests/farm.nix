# Farm end to end: niks3 + S3, two farm workers behind envoy (MAGLEV on
# x-nix-drv, gRPC health, TLS with client certs or bearer tokens, mTLS to
# workers), a CI client using the build hook and an OIDC developer.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
  mockOidc,
}:

let
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  tokenFile = pkgs.writeText "niks3-token" apiToken;
  s3Key = "rustfsadmin";
  signingSecretKey = pkgs.writeText "key" "farm-test-1:1/icU6Hlts+rG2LxnM8NoIMcrLWAzdCgJEOLjewE8DxGQKUPC9+LF07Ci6sEjhQP2G50TfF9TkQFBwwVRW5FXw==";
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://lb:5751";
  niks3Pkgs = niks3.packages.${pkgs.stdenv.hostPlatform.system};

  certs = pkgs.runCommand "farm-certs" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout ca.key -out ca.pem -subj /CN=farm-ca
    issue() {
      openssl req -newkey rsa:2048 -nodes -keyout $1.key -out $1.csr -subj /CN=$2
      openssl x509 -req -in $1.csr -days 3650 -CA ca.pem -CAkey ca.key -set_serial 0x$(openssl rand -hex 8) \
        -extfile <(printf "subjectAltName=DNS:$3") -out $1.pem
    }
    issue lb lb lb
    issue lb-client lb-1 lb
    issue worker worker "worker1,DNS:worker2,DNS:lb"
    issue ci ci-1 client
    issue stranger stranger client
  '';

  # mock-oidc puts its listen address in the issuer.
  lbAddr = "192.168.1.2";
  oidcAudience = "grpc://lb:50051";
  oidcConfig = {
    allow_insecure = true;
    providers.mock = {
      issuer = "http://${lbAddr}:8080/oidc";
      audience = oidcAudience;
      rules = [
        {
          bound_subject = [ "dev:*" ];
          scopes = [ "write" ];
        }
      ];
    };
  };

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
        tls = {
          certFile = "${certs}/worker.pem";
          keyFile = "${certs}/worker.key";
          clientCaFile = "${certs}/ca.pem";
        };
        trustedProxies = [ "lb-*" ];
        accessRules = [
          {
            cn = "ci-*";
            role = "trusted";
          }
        ];
        oidc = oidcConfig;
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
    { tag, features ? [ ] }:
    let
      mk = name: deps: derivation {
        inherit name;
        system = builtins.currentSystem;
        requiredSystemFeatures = features;
        builder = "/bin/sh";
        args = [ "-c" "echo '@nix {\"action\":\"setPhase\",\"phase\":\"farmPhase\"}' >&2; echo LOG-''${name}-''${tag} >&2; echo ''${name}-''${tag} ''${toString deps} > $out" ];
      };
      a = mk "farm-a" [ ];
      b = mk "farm-b" [ a ];
    in
    mk "farm-top" [ a b ]
  '';
  depExpr = pkgs.writeText "dep.nix" ''
    { tag, salt ? "", inputPath ? null }:
    rec {
      input = if inputPath != null then builtins.storePath inputPath else derivation {
        name = "local-input-''${tag}";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${tag} > $out" ];
      };
      referrer = derivation {
        name = "refers-to-input-''${tag}";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${input} > $out" ];
      };
      job = derivation {
        name = "uses-input-''${tag}''${salt}";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "read x < ''${input}; echo $x > $out" ];
      };
    }
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
    worker1 = {
      imports = [ worker ];
      nix.settings.system-features = [ "vip" ];
    };
    worker2 = {
      imports = [ worker ];
      nix.settings.system-features = [ ];
    };

    lb =
      { config, ... }:
      {
        assertions = [
          {
            assertion = config.networking.primaryIPAddress == lbAddr;
            message = "lbAddr";
          }
        ];
        imports = [
          common
          module
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
            endpoint = "lb:9000";
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
            S3_ENDPOINT_URL = "http://lb:9000";
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
        services.nix-grpc-farm-lb = {
          accessLog = true;
          enable = true;
          workers.${pkgs.stdenv.hostPlatform.system} = [
            "worker1:50051"
            "worker2:50051"
          ];
          features.${pkgs.stdenv.hostPlatform.system}.vip = [ "worker1:50051" ];
          healthCheckInterval = "1s";
          tls = {
            certFile = "${certs}/lb.pem";
            keyFile = "${certs}/lb.key";
            clientCaFile = "${certs}/ca.pem";
            upstream = {
              certFile = "${certs}/lb-client.pem";
              keyFile = "${certs}/lb-client.key";
              caFile = "${certs}/ca.pem";
            };
          };
        };
        systemd.services.mock-oidc = {
          wantedBy = [ "multi-user.target" ];
          after = [ "network-online.target" ];
          wants = [ "network-online.target" ];
          serviceConfig.Restart = "on-failure";
          serviceConfig.ExecStart = "${pkgs.lib.getExe mockOidc} -addr ${lbAddr}:8080 -issue-addr 0.0.0.0:8081";
        };
        networking.firewall.allowedTCPPorts = [
          5751
          8080
          8081
          9000
          50051
        ];
        environment.systemPackages = [
          pkgs.grpc-health-probe
          pkgs.curl
        ];
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
        # build-remote runs inside nix-daemon.service; prove the hook still
        # reaches the balancer with daemon egress filtering on.
        networking.nftables.enable = true;
        networking.nftables.flushRuleset = false;
        nix.firewall.enable = true;
      };
  };

  testScript = ''
    import re

    start_all()
    lb.wait_for_unit("niks3.service")
    lb.wait_for_open_port(5751)
    for w in [worker1, worker2]:
        w.wait_for_unit("nix-grpc-daemon.socket")
    lb.wait_for_unit("envoy.service")
    lb.wait_for_open_port(50051)
    # Envoy only routes to endpoints that passed a gRPC health check.
    probe_tls = "-tls -tls-ca-cert ${certs}/ca.pem -tls-client-cert ${certs}/lb-client.pem -tls-client-key ${certs}/lb-client.key"
    for w in ["worker1", "worker2"]:
        lb.wait_until_succeeds(f"grpc-health-probe -addr {w}:50051 {probe_tls} -tls-server-name {w}", timeout=180)
    def unhealthy() -> set[str]:
        # cluster::[addr]:port::hostname::NAME / cluster::[addr]:port::health_flags::FLAGS
        name, down = {}, set()
        for l in lb.succeed("curl -sf localhost:9901/clusters").splitlines():
            if m := re.match(r"(.*)::hostname::(.*)", l):
                name[m[1]] = m[2]
            elif (m := re.match(r"(.*)::health_flags::(.*)", l)) and "failed_active_hc" in m[2]:
                down.add(m[1])
        return {name[k] for k in down}

    def wait_unhealthy(*names: str) -> None:
        want = set(names)
        with lb.nested(f"waiting until exactly {sorted(want) or 'no'} workers are unhealthy"):
            def check(last: bool) -> bool:
                got = unhealthy()
                if last and got != want:
                    raise AssertionError(f"unhealthy={sorted(got)} want={sorted(want)} \n" + lb.succeed("curl -sf localhost:9901/clusters | grep -E 'hostname|health_flags'"))
                return got == want
            retry(check, timeout_seconds=60)

    wait_unhealthy()

    tls = "ca-cert=${certs}/ca.pem"
    ci = f"{tls}&client-cert=${certs}/ci.pem&client-key=${certs}/ci.key"
    envoy = f"grpc://lb:50051?{ci}"

    def build(store: str, tag: str) -> str:
        client.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag {tag} >&2")
        return client.succeed(f"nix eval --raw -f ${jobExpr} --argstr tag {tag} outPath").strip()

    def holders(path: str) -> int:
        return sum(w.execute(f"test -e {path}")[0] == 0 for w in [worker1, worker2])

    def probe(query: str) -> str:
        rc, out = client.execute(f"nix path-info --store 'grpc://lb:50051?{tls}{query}' $(readlink -f /run/current-system) 2>&1")
        return "ok" if rc == 0 or "is not valid" in out else out

    with subtest("balancer auth: client cert, bearer token, nothing, unknown CN"):
        out = probe("&client-cert=${certs}/ci.pem&client-key=${certs}/ci.key")
        assert out == "ok", out
        client.succeed("curl -sfG http://lb:8081/issue --data-urlencode 'aud=${oidcAudience}' --data-urlencode sub=dev:alice > /root/dev.jwt && test -s /root/dev.jwt")
        out = probe("&token-file=/root/dev.jwt")
        assert out == "ok", out
        out = probe("")
        assert "client certificate or bearer token" in out, out
        out = probe("&client-cert=${certs}/stranger.pem&client-key=${certs}/stranger.key")
        assert "no access rule matches 'stranger'" in out, out

    with subtest("without --eval-store the farm refuses and hints"):
        out = client.fail(f"nix build --store '{envoy}' -f ${jobExpr} --argstr tag t0 2>&1")
        assert "--eval-store" in out, out

    with subtest("fan-out build lands in the cache"):
        top = build(envoy, "t1")
        assert holders(top) == 1, "exactly one worker built it"
        # The client store has nothing. The cache does.
        client.fail(f"test -e {top}")
        client.succeed(f"nix copy --from ${niks3Url} --no-check-sigs {top} && grep farm-top-t1 {top}")

    with subtest("repeat is answered from the claim without building"):
        for w in [worker1, worker2]:
            w.succeed(f"nix-store --delete {top}")
        build(envoy, "t1")
        assert holders(top) == 0, "no worker rebuilt it"

    with subtest("build hook: nix-daemon with builders = grpc://lb"):
        # NIX_REMOTE=daemon so build-remote is spawned by nix-daemon.service (egress-filtered), not by root's nix.
        out = client.succeed(f"NIX_REMOTE=daemon nix build --log-format internal-json --max-jobs 0 --builders '{envoy}&system=${pkgs.stdenv.hostPlatform.system} ${pkgs.stdenv.hostPlatform.system} - 4' --print-out-paths --no-link -f ${jobExpr} --argstr tag hook 2>/tmp/hook.log").strip()
        client.succeed(f"grep farm-top-hook {out}")
        # Worker build output and stdenv phases must reach nix through build-remote as activity results.
        client.succeed("grep -F 'LOG-farm-top-hook' /tmp/hook.log | grep -qF '\"type\":101' && grep -F 'farmPhase' /tmp/hook.log | grep -qF '\"type\":104' || { cat /tmp/hook.log >&2; false; }")
        # The hook's actBuild must name the drv exactly like nix's own so log UIs key them together.
        client.succeed("grep -F '\"type\":105' /tmp/hook.log | grep -F lb:50051 | grep -qF '\"fields\":[\"/nix/store/' || { grep -F '\"type\":105' /tmp/hook.log >&2; false; }")

    with subtest("an input only one worker has still reaches the builder"):
        inp = client.succeed("nix-build --no-out-link ${depExpr} -A input --argstr tag w1only").strip()
        client.succeed(f"nix-store --export {inp} > /tmp/shared/inp.closure")
        worker1.succeed("nix-store --import < /tmp/shared/inp.closure")
        worker2.fail(f"test -e {inp}")
        for salt in ["a", "b"]:
            client.succeed(f"NIX_REMOTE=daemon nix build -L --max-jobs 0 --builders '{envoy}&system=${pkgs.stdenv.hostPlatform.system} ${pkgs.stdenv.hostPlatform.system} - 4' --no-link -f ${depExpr} job --argstr tag w1only --argstr salt {salt} >&2")

    with subtest("upload whose reference the worker lacks is completed from the cache"):
        ref = client.succeed("nix-build --no-out-link ${depExpr} -A input --argstr tag viacache").strip()
        referrer = client.succeed("nix-build --no-out-link ${depExpr} -A referrer --argstr tag viacache").strip()
        client.succeed(f"nix-store --export {ref} > /tmp/shared/ref.closure")
        worker1.succeed("nix-store --import < /tmp/shared/ref.closure")
        worker2.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_unhealthy("worker2")
        client.succeed(f"nix path-info --store '{envoy}' {ref} >&2")  # worker1 publishes ref
        worker2.fail(f"test -e {ref}")
        worker2.systemctl("start nix-grpc-daemon.socket")
        worker1.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_unhealthy("worker1")
        client.succeed(f"nix copy --no-check-sigs --to '{envoy}' {referrer} >&2")
        worker2.succeed(f"test -e {ref} && test -e {referrer}")
        worker1.systemctl("start nix-grpc-daemon.socket")
        wait_unhealthy()

    with subtest("a drv uploaded to one worker is valid on and built by another via the cache"):
        drv = client.succeed("nix-instantiate ${jobExpr} --argstr tag drvcache 2>/dev/null").strip()
        worker1.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_unhealthy("worker1")
        client.succeed(f"nix copy --no-check-sigs --to '{envoy}' {drv} >&2")
        worker1.systemctl("start nix-grpc-daemon.socket")
        wait_unhealthy()
        worker2.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_unhealthy("worker2")
        worker1.fail(f"test -e {drv}")
        # The farm answers as one store: worker1 reports the cached drv valid, so this copies nothing.
        client.succeed(f"nix copy --no-check-sigs --to '{envoy}' {drv} >&2")
        worker1.fail(f"test -e {drv}")
        client.succeed(f"NIX_REMOTE=daemon nix build -L --max-jobs 0 --builders '{envoy}&system=${pkgs.stdenv.hostPlatform.system} ${pkgs.stdenv.hostPlatform.system} - 4' --no-link -f ${jobExpr} --argstr tag drvcache >&2")
        uploads = worker1.succeed("journalctl -u nix-grpc-daemon -o cat _SYSTEMD_INVOCATION_ID=$(systemctl show -p InvocationID --value nix-grpc-daemon) | grep -c method=AddMultipleToStore || true").strip()
        assert uploads == "0", f"worker1 took {uploads} uploads instead of substituting the drv closure from the cache"
        worker2.systemctl("start nix-grpc-daemon.socket")
        wait_unhealthy()

    with subtest("restarting a worker mid-upload is retried"):
        client.succeed("{ echo big; head -c 40M /dev/urandom; } > /tmp/big && nix-store --add /tmp/big > /tmp/big.path")
        big = client.succeed("cat /tmp/big.path").strip()
        client.succeed(f"systemd-run --unit up -E NIX_REMOTE=daemon nix build -L --max-jobs 0 --builders '{envoy}&system=${pkgs.stdenv.hostPlatform.system} ${pkgs.stdenv.hostPlatform.system} - 4' --no-link -f ${depExpr} job --argstr tag big --argstr inputPath {big}")
        client.wait_until_succeeds("journalctl -u up -o cat | grep -q 'copying path.*-big'", timeout=60)
        for w in [worker1, worker2]:
            w.succeed("systemctl restart nix-grpc-daemon.service")
        client.wait_until_succeeds("systemctl show -p ActiveState --value up | grep -qx inactive", timeout=60)
        client.succeed("systemctl show -p Result --value up | grep -qx success")

    with subtest("interrupting the client stops the build on the worker"):
        # [6] keeps the probe from matching its own command line.
        builder = "pgrep -f 'read -t [6]0 x'"

        def building() -> bool:
            return any(w.execute(builder)[0] == 0 for w in [worker1, worker2])

        client.succeed(f"systemd-run --unit intr nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag intr")
        retry(lambda _: building(), timeout_seconds=60)
        client.succeed("systemctl kill -s INT intr")
        retry(lambda _: not building(), timeout_seconds=20)

    with subtest("stopping a worker mid-build is prompt and the client retries elsewhere"):
        client.succeed(f"systemd-run --unit stopme nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag stopme")
        retry(lambda _: building(), timeout_seconds=60)
        busy = worker1 if worker1.execute(builder)[0] == 0 else worker2
        other = worker2 if busy is worker1 else worker1
        busy.succeed("timeout 30 systemctl stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        busy.fail(builder)
        retry(lambda _: other.execute(builder)[0] == 0, timeout_seconds=90)
        busy.succeed("systemctl start nix-grpc-daemon.socket")
        client.succeed("systemctl kill -s INT stopme")
        retry(lambda _: not building(), timeout_seconds=20)

    with subtest("requiredSystemFeatures route to workers that have them"):
        worker2.succeed("journalctl --rotate --vacuum-time=1s -u nix-grpc-daemon")
        # Several graphs at once so MAGLEV would spread them if the vip cluster had worker2.
        client.succeed(f"nix build -L --store '{envoy}' --eval-store auto --expr 'map (tag: import ${jobExpr} {{ inherit tag; features = [\"vip\"]; }}) [\"f1\" \"f2\" \"f3\"]' --impure >&2")
        worker2.fail("journalctl -u nix-grpc-daemon -o cat | grep -q 'event=rpc method=BuildDerivation'")
        # Without the balancer's help the worker refuses and names the feature.
        out = client.fail(f"nix build -L --store 'grpc://worker2:50051?{ci}&unavailable-retries=0' --eval-store auto -f ${jobExpr} --argstr tag f5 --arg features '[\"vip\"]' 2>&1")
        assert "lacks system feature 'vip'" in out, out

    with subtest("low disk drains a worker and builds go to the other"):
        # Leave less than minFree on worker1.
        worker1.succeed("fallocate -l $(( $(df --output=avail -B1 /nix/store | tail -1) - 100*1024*1024 )) /nix/.rw-store/fill")
        wait_unhealthy("worker1")
        top = build(envoy, "drain")
        worker1.fail(f"test -e {top}")
        worker1.succeed("rm /nix/.rw-store/fill")
        wait_unhealthy()
  '';
}
