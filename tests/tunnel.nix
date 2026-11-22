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
    certFile = "/run/test-certs/node.pem";
    keyFile = "/run/test-certs/node.key";
    clientCaFile = "/run/test-certs/ca.pem";
  };
  cert = commonName: subjectAltName: { inherit commonName subjectAltName; };

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
        }) [ "ci-*" "lb-*" "worker-*" "spiffe://farm.test/worker/*" ];
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

  # Its firewall lets nothing in, so every build reaches it through a tunnel.
  # Both have the same certificate name, as pods of one Deployment do; the
  # balancer tells them apart by host name.
  tunnelled = feature: {
    imports = [ builder ];
    testCerts.certs.node = pkgs.lib.mkDefault (cert "worker-2" "DNS:tunnelled,DNS:tunnelled.test");
    nix.settings.system-features = [ feature ];
    services.nix-grpc-daemon.roles = [ "builder" ];
    services.nix-grpc-daemon.tunnel.balancers = [ "lb:${toString tunnelPort}" ];
  };

  jobExpr = pkgs.writeText "job.nix" ''
    { tag, feature }:
    derivation {
      name = "tunnel-''${tag}";
      system = builtins.currentSystem;
      requiredSystemFeatures = [ feature ];
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
      testCerts.certs.node = cert "worker-1" "DNS:dialled,IP:${dialledIp}";
      services.nix-grpc-daemon.listen = "[::]:50051";
      services.nix-grpc-daemon.advertise = "${dialledIp}:50051";
      networking.firewall.allowedTCPPorts = [ 50051 ];
    };

    tunnelled1 = tunnelled "one";
    tunnelled2 = tunnelled "two";
    # Known by its SPIFFE ID alone, as with a certificate from SPIRE.
    tunnelled3 = {
      imports = [ (tunnelled "three") ];
      testCerts.certs.node = cert null "DNS:tunnelled,URI:spiffe://farm.test/worker/svid";
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
        tunnel.spiffeId = "spiffe://farm.test/worker/";
        tls = tls // {
          upstream = {
            inherit (tls) certFile keyFile;
            caFile = tls.clientCaFile;
          };
        };
      };
      testCerts = {
        createsCas = true;
        certs.node = cert "lb-1" "DNS:lb";
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
      testCerts.certs.node = cert "ci-1" "DNS:client";
      nix.settings.substituters = pkgs.lib.mkForce [ ];
      environment.systemPackages = [ pkgs.openssl ];
    };
  };

  testScript = ''
    start_all()
    lb.wait_for_unit("niks3.service")
    lb.wait_for_open_port(5751)
    lb.wait_for_open_port(50051)
    for node in [dialled, tunnelled1, tunnelled2, tunnelled3]:
        node.systemctl("start nix-grpc-daemon.service")

    client.wait_for_unit("test-certs.service")
    certs = "/run/test-certs"
    store = f"grpc://lb:50051?client-cert={certs}/node.pem&client-key={certs}/node.key&ca-cert={certs}/ca.pem"

    def requests(cluster: str) -> int:
        stat = f"cluster.{cluster}.upstream_rq_total"
        out = lb.succeed(f"curl -sf 'localhost:9901/stats?filter=^{stat}$'")
        return int(out.split(":")[1]) if out.strip() else 0

    def build(tag: str, feature: str) -> str:
        args = f"-f ${jobExpr} --argstr tag {tag} --argstr feature {feature}"
        # The builder may still be connecting, so the first builds can find nobody.
        client.wait_until_succeeds(f"nix build -L --store '{store}' --eval-store auto {args}", timeout=180)
        return client.succeed(f"nix eval --raw {args} outPath")

    with subtest("a build reaches its tunnelled builder next to a dialled one"):
        tunnelled1.succeed(f"test -e {build('mixed1', 'one')}")
        tunnelled2.succeed(f"test -e {build('mixed2', 'two')}")
        assert requests("tunnelled") > 0, "the build did not go through a tunnel"
        assert requests("tunnelled-any") == 0, "store queries left the dialled builder"

    with subtest("a builder known by its SPIFFE ID alone gets its build through a tunnel"):
        tunnelled3.succeed(f"test -e {build('svid', 'three')}")

    with subtest("a client's certificate opens no tunnel"):
        # As the builders do it, for the set that gets what any builder may answer.
        headers = "".join(f"x-envoy-reverse-tunnel-{key}: {value}\\r\\n" for key, value in {
            "node-id": "any/ci-1/client",
            "cluster-id": "any",
            "tenant-id": "ci-1",
            "upstream-cluster-name": "lb",
        }.items())
        request = f"GET /reverse_connections/request HTTP/1.1\\r\\nhost: lb\\r\\n{headers}x-nix-instance: client\\r\\ncontent-length: 0\\r\\n\\r\\n"
        tls = f"-cert {certs}/node.pem -key {certs}/node.key -CAfile {certs}/ca.pem"
        # openssl ends with an error once the balancer closes the connection.
        answer = client.execute(
            f"(printf '{request}'; sleep 3) | openssl s_client -quiet -connect lb:${toString tunnelPort} -alpn nix-tunnel-any {tls} 2>/dev/null | head -n 1"
        )[1]
        assert "403" in answer, f"the balancer answered a client's certificate with: {answer!r}"

    with subtest("a farm of tunnelled builders answers store queries too"):
        lb.succeed("/run/current-system/specialisation/tunnelsOnly/bin/switch-to-configuration test")
        lb.wait_for_open_port(50051)
        tunnelled2.succeed(f"test -e {build('alone', 'two')}")
        assert requests("tunnelled") > 0, "the build did not go through a tunnel"
        assert requests("tunnelled-any") > 0, "store queries did not go through a tunnel"
  '';
}
