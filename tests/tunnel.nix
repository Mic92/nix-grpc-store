# A builder the balancer cannot dial: it opens tunnels to the balancer and
# gets its builds through them, first next to a dialled builder, then alone.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
}:

let
  system = pkgs.stdenv.hostPlatform.system;
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  tokenFile = pkgs.writeText "niks3-token" apiToken;
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://lb:5751";
  tunnelPort = 9443;

  # nixos test framework: nodes get 192.168.1.<n> in attribute-name order.
  dialledIp = "192.168.1.2";

  common = {
    virtualisation.memorySize = 1536;
    imports = [ ./lib/test-certs.nix ];
    nix.package = nixPkgs.nix-everything;
    nix.settings.experimental-features = [ "nix-command" ];
  };

  tls = {
    certFile = "/run/test-certs/cert.pem";
    keyFile = "/run/test-certs/key.pem";
    clientCaFile = "/run/test-certs/ca.pem";
  };

  builder =
    { config, ... }:
    {
      imports = [
        common
        module
      ];
      services.nix-grpc-daemon = {
        enable = true;
        scheduler = "lb:50051";
        logLevel = "debug";
        idleTimeout = null;
        # The default wants more free disk than a test VM has.
        minFree = "200M";
        package = config.programs.nix-grpc-store.package;
        inherit tls;
        trustedProxies = [ "lb-*" ];
        accessRules = map (cn: {
          inherit cn;
          role = "trusted";
        }) [ "ci-*" "lb-*" "worker-*" ];
        niks3 = {
          package = niks3.packages.${system}.niks3;
          url = niks3Url;
          tokenFile = toString tokenFile;
          cacheUrl = niks3Url;
          publicKeys = [ signingPublicKey ];
        };
      };
      programs.nix-grpc-store.enable = true;
      testCerts.neededBy = [ "nix-grpc-daemon.service" ];
    };

  jobExpr = pkgs.writeText "job.nix" ''
    { tag, features }:
    derivation {
      name = "tunnel-''${tag}";
      system = builtins.currentSystem;
      requiredSystemFeatures = features;
      builder = "/bin/sh";
      args = [ "-c" "echo ''${tag} > $out" ];
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-tunnel";
  globalTimeout = 1800;

  nodes = {
    dialled = {
      imports = [ builder ];
      testCerts.commonName = "worker-1";
      testCerts.subjectAltName = "DNS:dialled,IP:${dialledIp}";
      services.nix-grpc-daemon.listen = "[::]:50051";
      services.nix-grpc-daemon.advertise = "${dialledIp}:50051";
      networking.firewall.allowedTCPPorts = [ 50051 ];
    };

    # Its firewall lets nothing in, so every build reaches it through a tunnel.
    tunnelled = {
      imports = [ builder ];
      testCerts.commonName = "worker-2";
      # The balancer finds a tunnelled builder by the one name in its certificate.
      testCerts.subjectAltName = "DNS:tunnelled";
      nix.settings.system-features = [ "tunnelled" ];
      services.nix-grpc-daemon.roles = [ "builder" ];
      services.nix-grpc-daemon.tunnel.balancers = [ "lb:${toString tunnelPort}" ];
    };

    lb = {
      imports = [
        common
        module
        (import ./lib/niks3-node.nix {
          inherit pkgs niks3 apiToken;
          listenHost = "lb";
        })
      ];
      services.nix-grpc-farm-lb = {
        enable = true;
        systems = [ system ];
        scheduler = [ "${dialledIp}:50051" ];
        healthCheckInterval = "1s";
        tunnel.listen = "[::]:${toString tunnelPort}";
        tls = tls // {
          upstream = {
            inherit (tls) certFile keyFile;
            caFile = tls.clientCaFile;
          };
        };
      };
      testCerts = {
        createsCa = true;
        commonName = "lb-1";
        subjectAltName = "DNS:lb";
        neededBy = [ "envoy.service" ];
      };
      specialisation.tunnelsOnly.configuration.services.nix-grpc-farm-lb.systems = pkgs.lib.mkForce [ ];
      networking.firewall.allowedTCPPorts = [
        50051
        tunnelPort
      ];
      environment.systemPackages = [ pkgs.curl ];
    };

    client = {
      imports = [
        common
        module
      ];
      programs.nix-grpc-store.enable = true;
      testCerts.commonName = "ci-1";
      testCerts.subjectAltName = "DNS:client";
      nix.settings.substituters = pkgs.lib.mkForce [ ];
    };
  };

  testScript = ''
    start_all()
    lb.wait_for_unit("niks3.service")
    lb.wait_for_open_port(5751)
    lb.wait_for_open_port(50051)
    for node in [dialled, tunnelled]:
        node.systemctl("start nix-grpc-daemon.service")

    client.wait_for_unit("test-certs.service")
    certs = "/run/test-certs"
    store = f"grpc://lb:50051?client-cert={certs}/cert.pem&client-key={certs}/key.pem&ca-cert={certs}/ca.pem"

    def requests(cluster: str) -> int:
        stat = f"cluster.{cluster}.upstream_rq_total"
        out = lb.succeed(f"curl -sf 'localhost:9901/stats?filter=^{stat}$'")
        return int(out.split(":")[1]) if out.strip() else 0

    def build(tag: str) -> str:
        args = f"-f ${jobExpr} --argstr tag {tag} --arg features '[\"tunnelled\"]'"
        # The builder may still be connecting, so the first builds can find nobody.
        client.wait_until_succeeds(f"nix build -L --store '{store}' --eval-store auto {args}", timeout=180)
        return client.succeed(f"nix eval --raw {args} outPath")

    with subtest("a build reaches the tunnelled builder next to a dialled one"):
        tunnelled.succeed(f"test -e {build('mixed')}")
        assert requests("tunnelled") > 0, "the build did not go through a tunnel"
        assert requests("tunnelled-any") == 0, "store queries left the dialled builder"

    with subtest("a farm of tunnelled builders answers store queries too"):
        lb.succeed("/run/current-system/specialisation/tunnelsOnly/bin/switch-to-configuration test")
        lb.wait_for_open_port(50051)
        tunnelled.succeed(f"test -e {build('alone')}")
        assert requests("tunnelled") > 0, "the build did not go through a tunnel"
        assert requests("tunnelled-any") > 0, "store queries did not go through a tunnel"
  '';
}
