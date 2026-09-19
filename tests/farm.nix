# Multi-node end to end: niks3 + S3, two builder nodes (worker1 also the
# scheduler) behind envoy, a CI client using the build hook and an OIDC
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
    issue() {
      openssl req -newkey rsa:2048 -nodes -keyout $1.key -out $1.csr -subj /CN=$2
      openssl x509 -req -in $1.csr -days 3650 -CA ca.pem -CAkey ca.key -set_serial 0x$(openssl rand -hex 8) \
        -extfile <(printf "subjectAltName=$3\nextendedKeyUsage=serverAuth,clientAuth") -out $1.pem
    }
    issue lb lb "DNS:lb"
    issue lb-client lb-1 "DNS:lb"
    issue worker worker-1 "DNS:worker1,DNS:worker2,DNS:lb,IP:${ip.worker1},IP:${ip.worker2}"
    issue ci ci-1 "DNS:client"
    issue stranger stranger "DNS:client"
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
      ];
    };
  };

  common = {
    virtualisation.memorySize = 1536;
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
        # WorkerSession goes through the balancer like a client would.
        scheduler = "lb:50051";
        logLevel = "debug";
        idleTimeout = null;
        package = config.programs.nix-grpc-store.package;
        metricsListen = "127.0.0.1:9464";
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
          {
            # other nodes' WorkerSession, forwarded by the balancer
            cn = "worker-*";
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
      services.nix-grpc-daemon.roles = [ "builder" ];
      nix.settings.system-features = [ ];
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
          workers.${system} = [
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

    def wait_health(cluster: str, *down: str) -> None:
        want = set(down)
        with lb.nested(f"waiting until exactly {sorted(want) or 'no'} endpoints of {cluster} are unhealthy"):
            def check(last: bool) -> bool:
                got = {names[a] for a in unhealthy(cluster)}
                if last and got != want:
                    raise AssertionError(f"unhealthy={sorted(got)} want={sorted(want)}\n" + "\n".join(l for l in cluster_lines() if "health_flags" in l))
                return got == want
            retry(check, timeout_seconds=90)

    wait_health("${system}")
    wait_health("sched")

    def gauge(w, name: str) -> int:
        out = w.succeed(f"curl -sf http://127.0.0.1:9464/metrics | grep -F '{name} ' || true").strip()
        return int(float(out.split()[-1])) if out else 0
    def sched_workers(w) -> int:
        return gauge(w, 'nix_grpc_sched{kind="workers"}')
    def events(w, kind: str) -> int:
        return gauge(w, f'nix_grpc_events_total{{kind="{kind}"}}')

    with subtest("both builders hold a WorkerSession on the scheduler"):
        retry(lambda _: sched_workers(worker1) == 2, timeout_seconds=60)

    tls = "ca-cert=${certs}/ca.pem"
    ci = f"{tls}&client-cert=${certs}/ci.pem&client-key=${certs}/ci.key"
    envoy = f"grpc://lb:50051?{ci}"
    hook = f"--max-jobs 0 --builders '{envoy}&system=${system} ${system} - 4'"

    def build(store: str, tag: str, extra: str = "") -> str:
        client.succeed(f"nix build -L --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag {tag} {extra} >&2")
        return client.succeed(f"nix eval --raw -f ${jobExpr} --argstr tag {tag} {extra} outPath").strip()

    def holders(path: str) -> list[str]:
        return [w.name for w in [worker1, worker2] if w.execute(f"test -e {path}")[0] == 0]

    def probe(query: str) -> str:
        rc, out = client.execute(f"nix path-info --store 'grpc://lb:50051?{tls}{query}' $(readlink -f /run/current-system) 2>&1")
        return "ok" if rc == 0 or "is not valid" in out else out

    with subtest("balancer auth: client cert, bearer token, nothing, unknown CN"):
        out = probe("&client-cert=${certs}/ci.pem&client-key=${certs}/ci.key")
        assert out == "ok", out
        client.succeed("curl -sfG http://lb:8081/issue --data-urlencode 'aud=${oidcAudience}' --data-urlencode sub=dev:alice > /root/dev.jwt && test -s /root/dev.jwt")
        client.succeed("install -D ${certs}/foreign.pem /var/lib/nix-grpc-store/client.crt && install -D ${certs}/foreign.key /var/lib/nix-grpc-store/client.key")
        out = probe("&token-file=/root/dev.jwt")
        client.succeed("rm -r /var/lib/nix-grpc-store")
        assert out == "ok", out
        out = probe("")
        assert "client certificate or bearer token" in out, out
        out = probe("&client-cert=${certs}/stranger.pem&client-key=${certs}/stranger.key")
        assert "no access rule matches 'stranger'" in out, out

    with subtest("DAG build is scheduled across workers and lands in the cache"):
        top = build(envoy, "t1")
        assert len(holders(top)) == 1, holders(top)
        client.fail(f"test -e {top}")
        client.succeed(f"nix copy --from ${niks3Url} --no-check-sigs {top} && grep farm-top-t1 {top}")

    with subtest("repeat is answered Cached by the scheduler without building"):
        for w in [worker1, worker2]:
            w.succeed(f"nix-store --delete {top}")
        build(envoy, "t1")
        assert holders(top) == [], holders(top)
        worker1.succeed("curl -sf http://127.0.0.1:9464/metrics | grep -q 'nix_grpc_events_total{kind=\"cached\"}'")

    builder = "pgrep -f 'read -t [9]0 x'"
    def building() -> list[str]:
        return [w.name for w in [worker1, worker2] if w.execute(builder)[0] == 0]
    def release_slow() -> None:
        for w in [worker1, worker2]:
            w.execute("pkill -f 'read -t [9]0 x'")

    with subtest("two clients wanting the same drv build it once"):
        client.succeed(f"systemd-run --unit dedup1 nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag dedup")
        client.succeed(f"sleep 1; systemd-run --unit dedup2 nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag dedup")
        retry(lambda _: building() != [], timeout_seconds=60)
        client.succeed("sleep 2")
        release_slow()
        client.wait_until_succeeds("! systemctl is-active dedup1 dedup2", timeout=60)
        client.succeed("systemctl show -p Result dedup1 dedup2 | grep -c success | grep -qx 2 || { journalctl -u dedup1 -u dedup2 >&2; false; }")
        # Both RPCs on one worker under one assign_id: the second attached.
        ids = [w.succeed("journalctl -u nix-grpc-daemon -o cat | grep 'method=BuildDerivation.*slow-dedup' | grep -o 'assign_id=[0-9]*' || true").split() for w in [worker1, worker2]]
        idle, busy = sorted(ids, key=len)
        assert idle == [] and len(busy) == 2 and len(set(busy)) == 1, ids
        assert sum(events(w, "attached") for w in [worker1, worker2]) == 1

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
        worker2.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_health("${system}", "worker2")
        client.succeed(f"nix path-info --store '{envoy}' {ref} >&2")  # worker1 publishes ref
        worker2.fail(f"test -e {ref}")
        worker2.systemctl("start nix-grpc-daemon.socket")
        worker1.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_health("${system}", "worker1")
        client.succeed(f"nix copy --no-check-sigs --to '{envoy}' {referrer} >&2")
        worker2.succeed(f"test -e {ref} && test -e {referrer}")
        worker1.systemctl("start nix-grpc-daemon.service")
        wait_health("${system}")

    with subtest("scheduler down: builds wait, then resume when it is back"):
        worker1.systemctl("stop nix-grpc-daemon.socket nix-grpc-daemon.service")
        wait_health("sched", "worker1")
        client.succeed(f"systemd-run --unit waits nix build --store '{envoy}' --eval-store auto -f ${jobExpr} --argstr tag schedback")
        client.sleep(3)
        client.succeed("systemctl is-active waits")
        worker1.systemctl("start nix-grpc-daemon.service")
        wait_health("sched")
        client.wait_until_succeeds("! systemctl is-active waits", timeout=120)
        client.succeed("systemctl show -p Result --value waits | grep -qx success || { journalctl -u waits >&2; false; }")
        retry(lambda _: sched_workers(worker1) == 2, timeout_seconds=60)

    with subtest("scheduler restart mid-build: build finishes, no double build"):
        client.succeed(f"systemd-run --unit mid nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag mid")
        retry(lambda _: building() != [], timeout_seconds=60)
        who = building()
        assert len(who) == 1, who
        # Scheduler state is lost. If the build ran on worker1 it dies with the
        # daemon; either way no second copy may start while one is alive.
        worker1.succeed("systemctl kill -s KILL nix-grpc-daemon.service; systemctl start nix-grpc-daemon.service")
        client.sleep(5)
        assert len(building()) <= 1, building()
        release_slow()
        client.wait_until_succeeds("! systemctl is-active mid", timeout=120)
        client.succeed("systemctl show -p Result --value mid | grep -qx success || { journalctl -u mid >&2; false; }")

    with subtest("interrupting the client stops the build on the worker"):
        client.succeed(f"systemd-run --unit intr nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag intr")
        retry(lambda _: building() != [], timeout_seconds=60)
        client.succeed("systemctl kill -s INT intr")
        retry(lambda _: building() == [], timeout_seconds=20)

    with subtest("a deploy mid-build drains: the build finishes, the new generation takes over"):
        client.succeed(f"systemd-run --unit sw nix build --store '{envoy}' --eval-store auto -f ${slowExpr} --argstr tag sw")
        retry(lambda _: building() != [], timeout_seconds=60)
        busy = worker1 if building() == ["worker1"] else worker2
        if busy is worker2:
            worker2.succeed("timeout 20 /run/current-system/specialisation/next/bin/switch-to-configuration test >&2")
        else:
            worker1.succeed("systemctl reload nix-grpc-daemon.service")
        busy.succeed(builder)  # still running while draining
        build(envoy, "during-drain")  # goes to the other one
        busy.succeed("pkill -f 'read -t [9]0 x'")
        client.wait_until_succeeds("! systemctl is-active sw", timeout=90)
        client.succeed("systemctl show -p Result --value sw | grep -qx success || { journalctl -u sw >&2; false; }")
        busy.wait_until_succeeds("systemctl is-active nix-grpc-daemon.service || systemctl start nix-grpc-daemon.service")
        wait_health("${system}")
        retry(lambda _: sched_workers(worker1) == 2, timeout_seconds=90)

    with subtest("requiredSystemFeatures: placed on the worker that has them, refused when none does"):
        out = client.succeed(f"nix build -L --store '{envoy}' --eval-store auto --expr 'map (tag: import ${jobExpr} {{ inherit tag; features = [\"vip\"]; }}) [\"f1\" \"f2\" \"f3\"]' --impure 2>&1")
        assert "node-a: building " in out and "worker2: building" not in out, out
        worker1.succeed("curl -sf http://127.0.0.1:9464/metrics | grep -E 'nix_grpc_build_info\\{.*features=\"[^\"]*vip[^\"]*\".*worker=\"node-a\"\\} 1'")
        out = client.fail(f"nix build -L --store '{envoy}' --eval-store auto -f ${jobExpr} --argstr tag f5 --arg features '[\"gpu\"]' 2>&1")
        assert "features {gpu}" in out, out

    with subtest("low disk drains a worker and builds go to the other"):
        worker1.succeed("fallocate -l $(( $(df --output=avail -B1 /nix/store | tail -1) - 100*1024*1024 )) /nix/.rw-store/fill")
        worker1.wait_until_succeeds("journalctl -u nix-grpc-daemon -o cat | grep -q 'event=unhealthy reason=min_free'", timeout=30)
        top = build(envoy, "drain")
        assert holders(top) == ["worker2"], holders(top)
        worker1.succeed("rm /nix/.rw-store/fill")
        worker1.wait_until_succeeds("journalctl -u nix-grpc-daemon -o cat | grep -q 'event=healthy reason=min_free'", timeout=30)
  '';
}
