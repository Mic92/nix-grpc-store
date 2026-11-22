# Latency benchmark, not part of `checks` so CI never runs it:
#   nix build .#bench-latency -L
#
# netem on lo delays the gRPC TCP path (unix sockets are unaffected), so a
# single VM can measure how well the protocol hides RTT, for both `nix copy`
# and remote builds. 25 ms each way = 50 ms RTT.
{
  pkgs,
  nixPkgs,
  module,
  e2eTests,
}:
let
  system = pkgs.stdenv.hostPlatform.system;
  chainNix = pkgs.writeText "chain.nix" ''
    { salt }:
    let
      mk = name: deps: derivation {
        inherit name deps;
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${name} ''${salt} ''${toString deps} > $out" ];
      };
      go = n: prev: if n == 0 then prev else go (n - 1) [ (mk "link-''${toString n}-''${salt}" prev) ];
    in
    builtins.head (go 20 [ ])
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-store-bench-latency";
  globalTimeout = 1200;
  # For the vsock device the agent of tests/e2e listens on.
  sshBackdoor.enable = true;
  defaults.virtualisation.qemu.enableSharedMemory = true;
  defaults.imports = [ (import ./lib/e2e-agent.nix { inherit e2eTests; }) ];

  nodes.machine =
    { config, ... }:
    {
      imports = [ module ];

      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        substituters = [ ];
        substitute = false;
      };

      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "127.0.0.1:50051";
        trustClients = true;
        package = config.programs.nix-grpc-store.package;
      };

      boot.kernelModules = [ "sch_netem" ];
    };

  testScript = ''
    ${import ./lib/e2e.nix { inherit e2eTests; }}
    machine.wait_for_unit("nix-grpc-daemon.socket")
    machine.wait_for_open_port(50051)
    env = {"NGS_CHAIN_EXPR": "${chainNix}", "NGS_SYSTEM": "${system}"}
    run_e2e({"machine": machine}, env, "latency::", "--test-threads=1")
  '';
}
