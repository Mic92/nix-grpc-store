# Upload of a .drv whose input .drv the client skipped because only niks3
# has it: the worker must fetch the input before importing the referrer.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
}:
let
  apiToken = "drv-ref-token-that-is-at-least-36-characters-long";
  niks3Url = "http://localhost:5751";
  store = "grpc://127.0.0.1:50051?insecure=1";
  client = "local?root=/tmp/cli";
  expr = pkgs.writeText "drvs.nix" ''
    rec {
      a = derivation {
        name = "input-a";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo a > $out" ];
      };
      b = derivation {
        name = "uses-a";
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "cat ''${a} > $out" ];
      };
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-drv-ref";
  globalTimeout = 600;
  nodes.machine =
    { config, ... }:
    {
      imports = [
        module
        (import ./lib/niks3-node.nix {
          inherit pkgs niks3 apiToken;
          listenHost = "localhost";
        })
      ];
      virtualisation.memorySize = 2048;
      virtualisation.diskSize = 4096;
      nix.package = nixPkgs.nix-everything;
      nix.settings.experimental-features = [ "nix-command" ];
      programs.nix-grpc-store.enable = true;
      environment.systemPackages = [ pkgs.curl ];
      services.nix-grpc-daemon = {
        enable = true;
        listen = "127.0.0.1:50051";
        logLevel = "debug";
        trustClients = true;
        minFree = "0";
        package = config.programs.nix-grpc-store.package;
        niks3 = {
          package = niks3.packages.${pkgs.stdenv.hostPlatform.system}.niks3;
          url = niks3Url;
          tokenFile = toString (pkgs.writeText "niks3-token" apiToken);
          cacheUrl = niks3Url;
          publicKeys = [ "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=" ];
        };
      };
    };

  testScript = ''
    machine.wait_for_unit("niks3.service")
    machine.wait_for_open_port(5751)
    machine.wait_for_unit("nix-grpc-daemon.socket")

    a = machine.succeed("nix-instantiate --store '${client}' ${expr} -A a").strip()
    b = machine.succeed("nix-instantiate --store '${client}' ${expr} -A b").strip()

    # Lands in niks3 through the worker's publish.
    machine.succeed(f"nix copy --store '${client}' --to '${store}' {a}")
    machine.succeed(f"curl -sf ${niks3Url}/$(basename {a} | cut -d- -f1).narinfo")

    # Now only niks3 has it, like a drv some earlier client uploaded.
    machine.succeed(f"nix store delete --ignore-liveness {a}")
    machine.fail(f"nix path-info {a}")

    machine.succeed(f"nix copy --store '${client}' --to '${store}' {b} >&2")
    machine.succeed(f"nix path-info {a} {b}")
  '';
}
