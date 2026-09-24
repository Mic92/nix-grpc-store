# Multi-node end to end: niks3 + S3, two builder nodes (both may
# schedule, niks3 picks one) behind envoy, a CI client using the build hook and an OIDC
# developer.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
  mockOidc,
}:

let
  inherit (pkgs) lib;
  system = pkgs.stdenv.hostPlatform.system;
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  tokenFile = pkgs.writeText "niks3-token" apiToken;
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://lb:5751";
  niks3Pkgs = niks3.packages.${system};

  # nixos test framework: nodes get 192.168.1.<n> in attribute-name order.
  ip = {
    client = "192.168.1.1";
    lb = "192.168.1.2";
    worker1 = "192.168.1.3";
    worker2 = "192.168.1.4";
  };

  certs = pkgs.runCommand "farm-certs" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout ca.key -out ca.pem -subj /CN=farm-ca
    # The balancer's server cert comes from a "public" CA that nodes and
    # clients only know through the system trust store, like Let's Encrypt.
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout public-ca.key -out public-ca.pem -subj /CN=public-ca
    issue() {
      openssl req -newkey rsa:2048 -nodes -keyout $1.key -out $1.csr -subj /CN=$2
      openssl x509 -req -in $1.csr -days 3650 -CA $4.pem -CAkey $4.key -set_serial 0x$(openssl rand -hex 8) \
        -extfile <(printf "subjectAltName=$3\nextendedKeyUsage=serverAuth,clientAuth") -out $1.pem
    }
    issue lb lb "DNS:lb" public-ca
    issue lb-client lb-1 "DNS:lb" ca
    issue worker1 worker-1 "DNS:worker1,DNS:lb,IP:${ip.worker1}" ca
    issue worker2 worker-2 "DNS:worker2,DNS:lb,IP:${ip.worker2}" ca
    issue ci ci-1 "DNS:client" ca
    issue stranger stranger "DNS:client" ca
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout foreign.key -out foreign.pem -subj /CN=foreign
  '';

  oidcAudience = "grpc://lb:50051";
  oidcConfig = {
    allow_insecure = true;
    providers.mock = {
      issuer = "http://${ip.lb}:8080/oidc";
      audience = oidcAudience;
      rules = [
        {
          bound_subject = [ "dev:*" ];
          scopes = [ "write" ];
        }
        {
          # worker2's WorkerSession, see schedulerTokenFile below
          bound_subject = [ "node:*" ];
          scopes = [ "admin" ];
        }
      ];
    };
  };

  common = {
    virtualisation.memorySize = 1536;
    security.pki.certificateFiles = [ "${certs}/public-ca.pem" ];
    nix.package = nixPkgs.nix-everything;
    nix.settings.experimental-features = [ "nix-command" ];
  };

  worker =
    name:
    { config, ... }:
    {
      imports = [
        common
        module
      ];
      services.nix-grpc-daemon = {
        enable = true;
        listen = "[::]:50051";
        advertise = "${ip.${name}}:50051";
        # Through the balancer, so builders follow the active scheduler.
        scheduler = "lb:50051";
        logLevel = "debug";
        idleTimeout = null;
        package = config.programs.nix-grpc-store.package;
        metricsListen = "127.0.0.1:9464";
        tls = {
          certFile = "${certs}/${name}.pem";
          keyFile = "${certs}/${name}.key";
          clientCaFile = "${certs}/ca.pem";
        };
        trustedProxies = [ "lb-*" ];
        accessRules = [
          {
            cn = "ci-*";
            role = "trusted";
          }
          {
            # Envoy reads the builder list with its own certificate.
            cn = "lb-*";
            role = "trusted";
          }
          {
            # worker1's WorkerSession by certificate. Worker2's CN has no
            # rule on purpose, it gets in with its OIDC token instead.
            cn = "worker-1";
            role = "trusted";
          }
        ];
        oidc = oidcConfig;
        minFree = "200M";
        niks3 = {
          package = niks3Pkgs.niks3;
          url = niks3Url;
          tokenFile = toString tokenFile;
          cacheUrl = niks3Url;
          publicKeys = [ signingPublicKey ];
        };
      };
      programs.nix-grpc-store.enable = true;
      networking.firewall.allowedTCPPorts = [ 50051 ];
    };

  jobExpr = pkgs.writeText "job.nix" ''
    { tag, top ? tag, features ? [ ] }:
    let
      mk' = tag: name: deps: derivation {
        inherit name;
        system = builtins.currentSystem;
        requiredSystemFeatures = features;
        builder = "/bin/sh";
        args = [ "-c" "echo '@nix {\"action\":\"setPhase\",\"phase\":\"farmPhase\"}' >&2; echo LOG-''${name}-''${tag} >&2; echo ''${name}-''${tag} ''${toString deps} > $out" ];
      };
      mk = mk' tag;
      a = mk "farm-a" [ ];
      b = mk "farm-b" [ a ];
    in
    mk' top "farm-top" [ a b ]
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
  failExpr = pkgs.writeText "fail.nix" ''
    { tag }:
    derivation {
      name = "fail-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo BOOM-''${tag} >&2; exit 1" ];
    }
  '';
  slowExpr = pkgs.writeText "slow.nix" ''
    { tag }:
    derivation {
      name = "slow-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      # Inner sh so a test can pkill it to let the build succeed early.
      args = [ "-c" "/bin/sh -c 'read -t 90 x < /dev/zero'; echo ''${tag} > $out" ];
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-farm";
  globalTimeout = 900;

  nodes = {
    worker1 = {
      imports = [ (worker "worker1") ];
      nix.settings.system-features = [ "vip" ];
      services.nix-grpc-daemon.workerName = "node-a";
    };
    worker2 = {
      imports = [ (worker "worker2") ];
      nix.settings.system-features = [ ];
      services.nix-grpc-daemon.schedulerTokenFile = "/run/scheduler-token";
      systemd.services.scheduler-token = {
        requiredBy = [ "nix-grpc-daemon.service" ];
        before = [ "nix-grpc-daemon.service" ];
        after = [ "network-online.target" ];
        wants = [ "network-online.target" ];
        serviceConfig.Type = "oneshot";
        serviceConfig.Restart = "on-failure";
        serviceConfig.RestartSec = 1;
        script = "${lib.getExe pkgs.curl} -sfG http://${ip.lb}:8081/issue --data-urlencode 'aud=${oidcAudience}' --data-urlencode sub=node:worker2 -o /run/scheduler-token";
      };
      specialisation.next.configuration.services.nix-grpc-daemon.workerName = "worker2-next";
    };

    lb =
      { config, ... }:
      {
        imports = [
          common
          module
          (import ./lib/niks3-node.nix {
            inherit pkgs niks3 apiToken;
            listenHost = "lb";
          })
        ];
        services.nix-grpc-farm-lb = {
          accessLog = true;
          enable = true;
          systems = [ system ];
          scheduler = [
            "${ip.worker1}:50051"
            "${ip.worker2}:50051"
          ];
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
          serviceConfig.ExecStart = "${lib.getExe mockOidc} -addr ${ip.lb}:8080 -issue-addr 0.0.0.0:8081";
        };
        networking.firewall.allowedTCPPorts = [
          8080
          8081
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
        nix.settings.substituters = lib.mkForce [ ];
        # build-remote runs inside nix-daemon.service; prove the hook still
        # reaches the balancer with daemon egress filtering on.
        networking.nftables.enable = true;
        networking.nftables.flushRuleset = false;
        nix.firewall.enable = true;
      };
  };

  testScript = ''
    import re
    import time
    from datetime import timedelta
    def sec(n: int) -> timedelta:
        return timedelta(seconds=n)

    start_all()
    for n, a in [(client, "${ip.client}"), (lb, "${ip.lb}"), (worker1, "${ip.worker1}"), (worker2, "${ip.worker2}")]:
        n.wait_for_unit("network-addresses-eth1.service")
        n.succeed(f"ip -4 addr show | grep -qF {a}/ || {{ ip -4 addr >&2; false; }}")
    lb.wait_for_unit("niks3.service")
    lb.wait_for_open_port(5751)
    lb.wait_for_unit("envoy.service")
    lb.wait_for_open_port(50051)
    for w in [worker1, worker2]:
        w.systemctl("start nix-grpc-daemon.service")

    def cluster_lines() -> list[str]:
        return lb.succeed("curl -sf localhost:9901/clusters").splitlines()

    def unhealthy(cluster: str) -> set[str]:
        down = set()
        for l in cluster_lines():
            if (m := re.match(rf"{re.escape(cluster)}::([0-9.]+):[0-9]+::health_flags::(.*)", l)) and "failed_active_hc" in m[2]:
                down.add(m[1])
        return down

    names = {"${ip.worker1}": "worker1", "${ip.worker2}": "worker2"}

    def members(cluster: str) -> set[str]:
        up = set()
        for l in cluster_lines():
            if (m := re.match(rf"{re.escape(cluster)}::([0-9.]+):[0-9]+::health_flags::healthy$", l)):
                up.add(names[m[1]])
        return up

    def wait_members(cluster: str, *want: str) -> None:
        with lb.nested(f"waiting until exactly {sorted(want)} are healthy members of {cluster}"):
            def check(last: bool) -> bool:
                got = members(cluster)
                if last and got != set(want):
                    raise AssertionError(f"members={sorted(got)} want={sorted(want)}\n" + "\n".join(l for l in cluster_lines() if "health_flags" in l))
                return got == set(want)
            retry(check, timeout=sec(90))

    wait_members("${system}", "worker1", "worker2")

    def metrics(w) -> list[str]:
        return w.succeed("curl -sf http://127.0.0.1:9464/metrics").splitlines()
    def gauge(w, name: str) -> int:
        vals = [line.split()[-1] for line in metrics(w) if line.startswith(name + " ")]
        return int(float(vals[0])) if vals else 0
    def gauge_sum(w, label: str) -> int:
        return sum(int(float(line.split()[-1])) for line in metrics(w) if line.startswith("nix_grpc_sched_system") and label in line)
    def sched_workers(w) -> int:
        return gauge(w, 'nix_grpc_sched{kind="workers"}')
    def leader():
        up = [w for w in [worker1, worker2] if w.name not in {names[a] for a in unhealthy("sched")}]
        assert len(up) == 1, up
        return up[0]
    def standby():
        return worker2 if leader() is worker1 else worker1
    def settled(_=None) -> bool:
        down = unhealthy("sched")
        return len(down) == 1 and sched_workers(worker2 if names[down.pop()] == "worker1" else worker1) == 2
    def events(w, kind: str) -> int:
        return gauge(w, f'nix_grpc_events_total{{kind="{kind}"}}')
    def failures(reason: str) -> int:
        return sum(gauge(w, f'nix_grpc_build_failures_total{{reason="{reason}"}}') for w in [worker1, worker2])

    with subtest("one node schedules and both builders hold a WorkerSession on it"):
        retry(settled, timeout=sec(60))
        ldr = leader()
        # worker1 gets in by certificate, worker2's CN has no rule and its token counts.
        opened = leader().succeed("journalctl -u nix-grpc-daemon -o cat | grep 'event=worker_session_open'")
        assert "cn=worker-1 " in opened and "cn=oidc:mock:node:worker2 " in opened, opened
        assert gauge(ldr, 'nix_grpc_sched{kind="leader"}') == 1
        assert gauge(standby(), 'nix_grpc_sched{kind="leader"}') == 0
        # slots/free are keyed by offered features, which differ per worker here.
        slots = gauge_sum(ldr, 'kind="slots"')
        assert slots >= 2, slots
        assert gauge_sum(ldr, 'kind="free"') == slots
        assert gauge_sum(ldr, 'kind="queued"') == 0

    ci = "client-cert=${certs}/ci.pem&client-key=${certs}/ci.key"
    envoy = f"grpc://lb:50051?{ci}"
    def direct(w) -> str:
        return f"grpc://{w.name}:50051?{ci}&ca-cert=${certs}/ca.pem"
    hook = f"--max-jobs 0 --builders '{envoy}&system=${system} ${system} - 4'"

    def build(store: str, tag: str, extra: str = "") -> str:
        client.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag {tag} {extra} >&2")
        return client.succeed(f"nix eval --raw -f ${jobExpr} --argstr tag {tag} {extra} outPath").strip()

    def holders(path: str) -> list[str]:
        return [w.name for w in [worker1, worker2] if w.execute(f"test -e {path}")[0] == 0]

    def probe(query: str) -> str:
        rc, out = client.execute(f"nix path-info --store 'grpc://lb:50051?{query}' $(readlink -f /run/current-system) 2>&1")
        return "ok" if rc == 0 or "is not valid" in out else out

    with subtest("balancer auth: client cert, bearer token, nothing, unknown CN"):
        out = probe(ci)
        assert out == "ok", out
        client.succeed("curl -sfG http://lb:8081/issue --data-urlencode 'aud=${oidcAudience}' --data-urlencode sub=dev:alice > /root/dev.jwt && test -s /root/dev.jwt")
        client.succeed("install -D ${certs}/foreign.pem /var/lib/nix-grpc-store/client.crt && install -D ${certs}/foreign.key /var/lib/nix-grpc-store/client.key")
        out = probe("token-file=/root/dev.jwt")
        client.succeed("rm -r /var/lib/nix-grpc-store")
        assert out == "ok", out
        out = probe("")
        assert "client certificate or bearer token" in out, out
        out = probe("client-cert=${certs}/stranger.pem&client-key=${certs}/stranger.key")
        assert "no access rule matches 'stranger'" in out, out

    with subtest("DAG build is scheduled across workers and lands in the cache"):
        top = build(envoy, "t1")
        assert len(holders(top)) == 1, holders(top)
        client.fail(f"test -e {top}")
        client.succeed(f"nix copy --from ${niks3Url} --no-check-sigs {top} && grep farm-top-t1 {top}")
        # The scheduler picks the worker, so only the total is fixed.
        def inputs_of(w) -> tuple[int, int, int]:
            local = gauge(w, 'nix_grpc_build_inputs_total{state="local"}')
            fetched = gauge(w, 'nix_grpc_build_inputs_total{state="fetched"}')
            return local, fetched, gauge(w, "nix_grpc_build_input_seconds_count")
        counted = [inputs_of(w) for w in [worker1, worker2]]
        assert sum(local + fetched for local, fetched, _ in counted) >= 2, counted
        assert sum(builds for _, _, builds in counted) == 3, counted

    with subtest("repeat is answered Cached by the scheduler without building"):
        for w in [worker1, worker2]:
            w.succeed(f"nix-store --delete {top}")
        build(envoy, "t1")
        assert holders(top) == [], holders(top)
        assert events(leader(), "cached") >= 1

    with subtest("inputs already in the cache are not sent to the scheduler"):
        wants = lambda: int(leader().succeed("journalctl -u nix-grpc-daemon -o cat | grep -c 'event=want ' || true"))
        before = wants()
        build(envoy, "t1", "--argstr top t1b")
        assert wants() - before == 1, wants() - before

    with subtest("outputs read back through any worker carry the cache signature"):
        signed = build(envoy, "sig")
        for w in [worker1, worker2]:
            out = client.succeed(f"nix path-info --sigs --store '{direct(w)}' {signed}").strip()
            assert "${signingPublicKey}".split(":")[0] + ":" in out, (w.name, out)
        client.succeed(f"nix-store --delete {signed} && nix copy --from '{envoy}' --option trusted-public-keys '${signingPublicKey}' {signed}")

    builder = "pgrep -f 'read -t [9]0 x'"
    def building() -> list[str]:
        return [w.name for w in [worker1, worker2] if w.execute(builder)[0] == 0]
    def release_slow() -> None:
        for w in [worker1, worker2]:
            w.execute("pkill -f 'read -t [9]0 x'")

    with subtest("two clients wanting the same drv build it once"):
        client.succeed(f"systemd-run --unit dedup1 nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag dedup")
        client.succeed(f"sleep 1; systemd-run --unit dedup2 nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag dedup")
        retry(lambda _: building() != [], timeout=sec(60))
        client.succeed("sleep 2")
        release_slow()
        client.wait_until_succeeds("! systemctl is-active dedup1 dedup2", timeout=sec(60))
        client.succeed("systemctl show -p Result dedup1 dedup2 | grep -c success | grep -qx 2 || { journalctl -u dedup1 -u dedup2 >&2; false; }")
        # Both RPCs on one worker under one assign_id: the second attached.
        ids = [w.succeed("journalctl -u nix-grpc-daemon -o cat | grep 'method=BuildDerivation.*slow-dedup' | grep -o 'assign_id=[0-9]*' || true").split() for w in [worker1, worker2]]
        idle, busy = sorted(ids, key=len)
        assert idle == [] and len(busy) == 2 and len(set(busy)) == 1, ids
        assert sum(events(w, "attached") for w in [worker1, worker2]) == 1

    with subtest("--timeout and --max-silent-time reach the builder"):
        for flag in ["--timeout 5", "--max-silent-time 5"]:
            t0 = time.monotonic()
            rc, out = client.execute(f"nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag tmo{flag[2]} {flag} 2>&1")
            assert rc != 0 and "timed out" in out and time.monotonic() - t0 < 60, (flag, rc, time.monotonic() - t0, out)
        assert building() == []
        assert failures("TimedOut") == 2, [metrics(w) for w in [worker1, worker2]]

    with subtest("a failing build is counted with its own reason"):
        before = failures("PermanentFailure")
        rc, out = client.execute(f"nix build -L --store '{envoy}' --eval-store auto -f ${failExpr} --argstr tag boom 2>&1")
        assert rc != 0 and "BOOM-boom" in out, (rc, out)
        assert failures("PermanentFailure") == before + 1, [metrics(w) for w in [worker1, worker2]]
        assert sum(events(w, "build_failed") for w in [worker1, worker2]) == failures("PermanentFailure") + failures("TimedOut")

    with subtest("the first client leaving does not fail the second"):
        client.succeed(f"systemd-run --unit share1 nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag share")
        client.succeed(f"sleep 1; systemd-run --unit share2 nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag share")
        retry(lambda _: building() != [], timeout=sec(60))
        client.succeed("sleep 2; systemctl kill -s INT share1")
        client.wait_until_succeeds("! systemctl is-active share1", timeout=sec(30))
        retry(lambda _: building() != [], timeout=sec(60))
        release_slow()
        client.wait_until_succeeds("! systemctl is-active share2", timeout=sec(120))
        client.succeed("systemctl show -p Result share2 | grep -q success || { journalctl -u share2 >&2; false; }")

    with subtest("build hook: nix-daemon with builders = grpc://lb"):
        out = client.succeed(f"NIX_REMOTE=daemon nix build --log-format internal-json {hook} --print-out-paths --no-link -f ${jobExpr} --argstr tag hook 2>/tmp/hook.log").strip()
        client.succeed(f"grep farm-top-hook {out}")
        client.succeed("grep -F 'LOG-farm-top-hook' /tmp/hook.log | grep -qF '\"type\":101' && grep -F 'farmPhase' /tmp/hook.log | grep -qF '\"type\":104' || { cat /tmp/hook.log >&2; false; }")
        client.succeed("grep -F '\"type\":105' /tmp/hook.log | grep -F lb:50051 | grep -qF '\"fields\":[\"/nix/store/' || { grep -F '\"type\":105' /tmp/hook.log >&2; false; }")

    with subtest("an input only one worker has still reaches the builder"):
        inp = client.succeed("nix-build --no-out-link ${depExpr} -A input --argstr tag w1only").strip()
        client.succeed(f"nix-store --export {inp} > /tmp/shared/inp.closure")
        worker1.succeed("nix-store --import < /tmp/shared/inp.closure")
        worker2.fail(f"test -e {inp}")
        for salt in ["a", "b", "c"]:
            client.succeed(f"NIX_REMOTE=daemon nix build -L {hook} --no-link -f ${depExpr} job --argstr tag w1only --argstr salt {salt} >&2")

    with subtest("upload whose reference the worker lacks is completed from the cache"):
        ref = client.succeed("nix-build --no-out-link ${depExpr} -A input --argstr tag viacache").strip()
        referrer = client.succeed("nix-build --no-out-link ${depExpr} -A referrer --argstr tag viacache").strip()
        client.succeed(f"nix-store --export {ref} > /tmp/shared/ref.closure")
        worker1.succeed("nix-store --import < /tmp/shared/ref.closure")
        worker2.systemctl("stop nix-grpc-daemon.service")
        wait_members("${system}", "worker1")
        client.succeed(f"nix path-info --store '{envoy}' {ref} >&2")  # worker1 publishes ref
        worker2.fail(f"test -e {ref}")
        worker2.systemctl("start nix-grpc-daemon.service")
        worker1.systemctl("stop nix-grpc-daemon.service")
        wait_members("${system}", "worker2")
        client.succeed(f"nix copy --no-check-sigs --to '{envoy}' {referrer} >&2")
        worker2.succeed(f"test -e {ref} && test -e {referrer}")
        worker1.systemctl("start nix-grpc-daemon.service")
        wait_members("${system}", "worker1", "worker2")

    with subtest("a niks3 restart does not cost the leader its role"):
        was = leader()
        lb.systemctl("restart niks3.service")
        lb.wait_for_unit("niks3.service")
        retry(settled, timeout=sec(60))
        assert leader() is was
        was.fail("journalctl -u nix-grpc-daemon | grep -q event=scheduler_yield")
        build(envoy, "after-niks3-restart")

    with subtest("scheduler down: the other node takes the lock and keeps it"):
        was, nxt = leader(), standby()
        was.systemctl("stop nix-grpc-daemon.service")
        nxt.wait_until_succeeds("journalctl -u nix-grpc-daemon | grep -q event=scheduler_take_over", timeout=sec(30))
        retry(lambda _: sched_workers(nxt) == 1, timeout=sec(30))
        build(envoy, "on-standby")
        was.systemctl("start nix-grpc-daemon.service")
        retry(settled, timeout=sec(60))
        assert leader() is nxt
        assert sched_workers(was) == 0

    with subtest("clean scheduler restart: peers are told, running builds survive, no reconnect noise"):
        client.succeed(f"systemd-run --unit rs nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag rs")
        retry(lambda _: building() != [], timeout=sec(60))
        who = building()
        was, nxt = leader(), standby()
        nxt.succeed("journalctl --rotate --vacuum-time=1s -u nix-grpc-daemon >/dev/null 2>&1 || true")
        # --no-block: if the build sits on the leader it drains first and the
        # restart completes only after release_slow().
        was.succeed("systemctl restart --no-block nix-grpc-daemon.service")
        client.sleep(duration=sec(5))
        assert building() == who, f"restart killed or moved the build: {who} -> {building()}"
        release_slow()
        nxt.wait_until_succeeds("journalctl -u nix-grpc-daemon | grep -q 'event=scheduler_restarting addr='", timeout=sec(60))
        client.wait_until_succeeds("! systemctl is-active rs", timeout=sec(120))
        client.succeed("systemctl show -p Result --value rs | grep -qx success || { journalctl -u rs >&2; false; }")
        client.fail("journalctl -u rs | grep -q 'reconnecting'")
        retry(settled, timeout=sec(60))

    with subtest("scheduler restart mid-build: build finishes, no double build"):
        client.succeed(f"systemd-run --unit mid nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag mid")
        retry(lambda _: building() != [], timeout=sec(60))
        who = building()
        assert len(who) == 1, who
        # Scheduler state is lost. If the build ran on the leader it dies with
        # the daemon. Either way no second copy may start while one is alive.
        leader().succeed("systemctl kill -s KILL nix-grpc-daemon.service; systemctl start nix-grpc-daemon.service")
        client.sleep(duration=sec(5))
        assert len(building()) <= 1, building()
        release_slow()
        client.wait_until_succeeds("! systemctl is-active mid", timeout=sec(120))
        client.succeed("systemctl show -p Result --value mid | grep -qx success || { journalctl -u mid >&2; false; }")

    with subtest("interrupting the client stops the build on the worker"):
        client.succeed(f"systemd-run --unit intr nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag intr")
        retry(lambda _: building() != [], timeout=sec(60))
        client.succeed("systemctl kill -s INT intr")
        retry(lambda _: building() == [], timeout=sec(20))

    with subtest("a deploy mid-build drains: the build finishes, the new generation takes over"):
        client.succeed(f"systemd-run --unit sw nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag sw")
        retry(lambda _: building() != [], timeout=sec(60))
        busy = worker1 if building() == ["worker1"] else worker2
        if busy is worker2:
            worker2.succeed("timeout 20 /run/current-system/specialisation/next/bin/switch-to-configuration test >&2")
        else:
            worker1.succeed("systemctl reload nix-grpc-daemon.service")
        busy.succeed(builder)  # still running while draining
        build(envoy, "during-drain")  # goes to the other one
        busy.succeed("pkill -f 'read -t [9]0 x'")
        client.wait_until_succeeds("! systemctl is-active sw", timeout=sec(90))
        client.succeed("systemctl show -p Result --value sw | grep -qx success || { journalctl -u sw >&2; false; }")
        busy.wait_until_succeeds("systemctl is-active nix-grpc-daemon.service || systemctl start nix-grpc-daemon.service")
        wait_members("${system}", "worker1", "worker2")
        retry(settled, timeout=sec(90))

    with subtest("queue gauges show a burst while every build blocks"):
        slots = gauge_sum(leader(), 'kind="slots"')
        tags = " ".join(f'"q{i}"' for i in range(slots + 2))
        client.succeed(f"systemd-run --unit burst nix build --store '{envoy}' --eval-store auto --impure --expr 'map (tag: import ${slowExpr} {{ inherit tag; }}) [ {tags} ]'")
        # The Wants arrive within a second and nothing else happens until a
        # build ends, so only the trailing export can show the queue.
        retry(lambda _: gauge_sum(leader(), 'kind="queued"') == 2, timeout=sec(10))
        def burst_done(_) -> bool:
            release_slow()
            return client.execute("systemctl is-active burst")[0] != 0
        retry(burst_done, timeout=sec(90))
        client.succeed("systemctl show -p Result --value burst | grep -qx success || { journalctl -u burst >&2; false; }")
        retry(lambda _: gauge_sum(leader(), 'kind="queued"') == 0, timeout=sec(10))
        waits = gauge(leader(), 'nix_grpc_queue_seconds_count{system="${system}"}')
        assert waits >= slots + 2, waits

    with subtest("requiredSystemFeatures: placed on the worker that has them, refused when none does"):
        out = client.succeed(f"nix build -L --store '{envoy}' --eval-store auto --expr 'map (tag: import ${jobExpr} {{ inherit tag; features = [\"vip\"]; }}) [\"f1\" \"f2\" \"f3\"]' --impure 2>&1")
        assert "node-a: building " in out and "worker2: building" not in out, out
        assert any(re.match(r'nix_grpc_build_info\{.*features="[^"]*vip[^"]*".*worker="node-a"\} 1', line) for line in metrics(worker1)), metrics(worker1)
        client.succeed(f"systemd-run --unit gpu nix build -L --store '{envoy}&restart-grace=30' --eval-store auto -f ${jobExpr} --argstr tag f5 --arg features '[\"gpu\"]'")
        unp = 'nix_grpc_sched_system{features="gpu",kind="unplaceable",system="${system}"}'
        retry(lambda _: gauge(leader(), unp) == 1, timeout=sec(60))
        client.wait_until_succeeds("systemctl show -p Result gpu | grep -q exit-code", timeout=sec(60))
        out = client.succeed("journalctl -u gpu -o cat")
        assert "features {gpu}" in out, out
        retry(lambda _: gauge(leader(), unp) == 0, timeout=sec(30))

    with subtest("low disk drains a worker and builds go to the other"):
        worker1.succeed("fallocate -l $(( $(df --output=avail -B1 /nix/store | tail -1) - 100*1024*1024 )) /nix/.rw-store/fill")
        worker1.wait_until_succeeds("journalctl -u nix-grpc-daemon -o cat | grep -q 'event=unhealthy reason=min_free'", timeout=sec(30))
        top = build(envoy, "drain")
        assert holders(top) == ["worker2"], holders(top)
        worker1.succeed("rm /nix/.rw-store/fill")
        worker1.wait_until_succeeds("journalctl -u nix-grpc-daemon -o cat | grep -q 'event=healthy reason=min_free'", timeout=sec(30))
  '';
}
