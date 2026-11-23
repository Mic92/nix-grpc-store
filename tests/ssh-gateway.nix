# A client with a stock nix (no plugin) uses the farm through the ssh
# gateway, as an ordinary ssh-ng:// remote builder: once as a trusted user
# through the build hook, once untrusted with `--store ssh-ng://`.
{
  pkgs,
  nixPkgs,
  module,
}:

let
  snakeoil = import "${pkgs.path}/nixos/tests/ssh-keys.nix" pkgs;
  certs = "/run/test-certs";

  expr = pkgs.writeText "job.nix" ''
    { tag }:
    rec {
      a = derivation {
        name = "gw-a-''${tag}";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo LOG-a-''${tag} >&2; echo a-''${tag} > $out" ];
      };
      b = derivation {
        name = "gw-b-''${tag}";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo LOG-b-''${tag} >&2; read x < ''${a}; echo b-$x > $out" ];
      };
      fail = derivation {
        name = "gw-fail-''${tag}";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo BOOM-''${tag} >&2; exit 1" ];
      };
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-ssh-gateway";
  globalTimeout = 900;

  nodes.farm =
    { config, ... }:
    {
      imports = [
        module
        ./lib/test-certs.nix
      ];
      virtualisation.memorySize = 1536;
      testCerts.createsCas = true;
      testCerts.certs = {
        farm = {
          commonName = "worker-farm";
          subjectAltName = "DNS:farm,DNS:localhost,IP:127.0.0.1";
        };
        gateway = {
          commonName = "gw-farm";
          subjectAltName = "DNS:gateway";
        };
      };
      testCerts.neededBy = [ "nix-grpc-daemon.service" ];
      nix.package = nixPkgs.nix-everything;
      nix.settings.experimental-features = [ "nix-command" ];
      nix.settings.substituters = [ ];
      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "127.0.0.1:50051";
        advertise = "127.0.0.1:50051";
        idleTimeout = null;
        logLevel = "debug";
        minFree = "0";
        package = config.programs.nix-grpc-store.package;
        tls = {
          certFile = "${certs}/farm.pem";
          keyFile = "${certs}/farm.key";
          clientCaFile = "${certs}/ca.pem";
        };
        accessRules = [
          {
            cn = "gw-*";
            role = "trusted";
          }
        ];
      };
      services.nix-grpc-ssh-gateway = {
        enable = true;
        farm = "grpc://localhost:50051?ca-cert=${certs}/ca.pem&client-cert=${certs}/gateway.pem&client-key=${certs}/gateway.key";
        trustedKeys = [ snakeoil.snakeOilEd25519PublicKey ];
        keys = [ snakeoil.snakeOilPublicKey ];
      };
    };

  nodes.client =
    { config, pkgs, ... }:
    {
      virtualisation.memorySize = 1024;
      # Stock nix: what a Determinate or nix-darwin client would have.
      nix.package = pkgs.nix;
      nix.settings.experimental-features = [ "nix-command" ];
      nix.settings.substituters = [ ];
      nix.distributedBuilds = true;
      nix.buildMachines = [
        {
          hostName = "farm";
          sshUser = "nix-farm";
          sshKey = "/root/.ssh/trusted";
          protocol = "ssh-ng";
          system = config.nixpkgs.hostPlatform.system;
          maxJobs = 4;
        }
      ];
      programs.ssh.knownHosts.farm.publicKey = snakeoil.snakeOilEd25519PublicKey;
      services.openssh.enable = true;
    };

  testScript = ''
    import json

    start_all()
    farm.wait_for_unit("nix-grpc-daemon.service")
    farm.wait_for_unit("sshd.service")
    farm.wait_for_open_port(50051)

    # The farm's host key doubles as the client's trusted key.
    farm.succeed("install -Dm600 ${snakeoil.snakeOilEd25519PrivateKey} /etc/ssh/ssh_host_ed25519_key")
    farm.succeed("systemctl restart sshd")
    client.succeed("install -Dm600 ${snakeoil.snakeOilEd25519PrivateKey} /root/.ssh/trusted")
    client.succeed("install -Dm600 ${snakeoil.snakeOilPrivateKey} /root/.ssh/untrusted")
    client.wait_for_unit("multi-user.target")

    trusted = "ssh-ng://nix-farm@farm?ssh-key=/root/.ssh/trusted"
    untrusted = "ssh-ng://nix-farm@farm?ssh-key=/root/.ssh/untrusted"

    def trust(store):
        # Without --json the report goes to stderr.
        return json.loads(client.succeed(f"nix store info --json --store '{store}'"))["trusted"]

    with subtest("store info through the gateway"):
        assert trust(trusted) is True
        assert trust(untrusted) is False

    with subtest("no shell for gateway keys"):
        client.fail("ssh -i /root/.ssh/trusted nix-farm@farm true")

    def tunnels():
        return int(farm.succeed("journalctl -u nix-grpc-daemon | grep -c 'event=rpc method=Connect' || true").strip())

    with subtest("build hook: a chain built by the farm"):
        before = tunnels()
        out = client.succeed("nix-build --max-jobs 0 -L ${expr} --argstr tag hook -A b 2>&1")
        print(out)
        assert "LOG-b-hook" in out, "build log not relayed"
        path = client.succeed("nix-build --max-jobs 0 ${expr} --argstr tag hook -A b").strip()
        assert client.succeed(f"cat {path}").strip() == "b-a-hook"
        # Built by the scheduler's worker, not over the tunnel.
        farm.succeed("journalctl -u nix-grpc-daemon | grep -q 'method=BuildDerivation.*gw-b-hook'")
        assert tunnels() == before, "the gateway fell back to the worker-protocol tunnel"

    with subtest("build hook: a failure is reported"):
        out = client.fail("nix-build --max-jobs 0 -L ${expr} --argstr tag f -A fail 2>&1")
        assert "BOOM-f" in out, out

    with subtest("untrusted --store ssh-ng://: closure uploaded, built by the farm"):
        drv = client.succeed("nix-instantiate ${expr} --argstr tag u -A b").strip()
        out = client.succeed(f"nix build -L --store '{untrusted}' --eval-store auto '{drv}^*' --print-out-paths 2>&1")
        print(out)
        farm.succeed("journalctl -u nix-grpc-daemon | grep -q 'method=BuildDerivation.*gw-b-u'")

    with subtest("untrusted: unsigned input-addressed paths are refused"):
        local = client.succeed("nix-build --builders ''' ${expr} --argstr tag local -A a").strip()
        client.fail(f"nix copy --to '{untrusted}' {local}")
        client.succeed(f"nix copy --to '{trusted}' {local}")
  '';
}
