# Multi-node end to end: niks3 + S3, two builder nodes (both may
# schedule, niks3 picks one) behind envoy, a CI client using the build hook and an OIDC
# developer.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
  mockOidc,
  e2eTests,
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
    { tag, salt ? "", inputPath ? null, features ? [ ], jobFeatures ? features }:
    rec {
      input = if inputPath != null then builtins.storePath inputPath else derivation {
        name = "local-input-''${tag}";
        system = builtins.currentSystem;
        requiredSystemFeatures = features;
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
        requiredSystemFeatures = jobFeatures;
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

  sshBackdoor.enable = true;
  defaults.virtualisation.qemu.enableSharedMemory = true;
  # The ssh generator only opens the vsock socket if /dev/vsock exists when it runs.
  defaults.boot.initrd.kernelModules = [ "vmw_vsock_virtio_transport" ];

  nodes = {
    worker1 = {
      imports = [ (worker "worker1") ];
      nix.settings.system-features = [ "vip" ];
      services.nix-grpc-daemon.workerName = "node-a";
    };
    worker2 = {
      imports = [ (worker "worker2") ];
      # Lets a test place a build on worker2 alone, as vip does on worker1.
      nix.settings.system-features = [ "w2" ];
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
    ${import ./lib/e2e.nix { inherit pkgs e2eTests; }}
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

    env = {
        "NGS_WORKER1_IP": "${ip.worker1}",
        "NGS_WORKER2_IP": "${ip.worker2}",
        "NGS_SYSTEM": "${system}",
        "NGS_CERTS": "${certs}",
        "NGS_OIDC_AUDIENCE": "${oidcAudience}",
        "NGS_SIGNING_PUBKEY": "${signingPublicKey}",
        "NGS_NIKS3_URL": "${niks3Url}",
        "NGS_JOB_EXPR": "${jobExpr}",
        "NGS_DEP_EXPR": "${depExpr}",
        "NGS_FAIL_EXPR": "${failExpr}",
        "NGS_SLOW_EXPR": "${slowExpr}",
    }
    nodes = {"client": client, "lb": lb, "worker1": worker1, "worker2": worker2}

    with subtest("independent builds"):
        run_e2e(nodes, env, "farm::independent::", "--test-threads=3")

    with subtest("scheduler and lifecycle"):
        run_e2e(nodes, env, "farm::serial::", "--test-threads=1")
  '';
}
