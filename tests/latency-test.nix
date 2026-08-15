# Latency benchmark, not part of `checks` so CI never runs it:
#   nix build .#bench-latency -L
#
# netem on lo delays the gRPC TCP path (unix sockets are unaffected), so a
# single VM can measure how well narFromPath request pipelining hides RTT.
# 25 ms each way = 50 ms RTT. A sequential client would need >= paths * RTT.
{
  pkgs,
  nixPkgs,
  module,
}:

pkgs.testers.runNixOSTest {
  name = "nix-grpc-store-bench-latency";
  globalTimeout = 1200;

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
    import time

    n_paths = 200
    rtt_s = 0.05

    machine.wait_for_unit("nix-grpc-daemon.service")
    machine.wait_for_open_port(50051)
    store = "grpc://127.0.0.1:50051?insecure=1"

    machine.succeed(
        "mkdir -p /root/small && "
        f"for i in $(seq {n_paths}); do "
        "  head -c 4096 /dev/urandom | base64 > /root/small/f$i; "
        "done"
    )
    small = machine.succeed(
        "cd /root/small && nix-store --store /root/src --add f*"
    ).splitlines()
    assert len(small) == n_paths, small
    paths = " ".join(f"'{p}'" for p in small)
    machine.succeed(f"nix copy --no-check-sigs --from /root/src --to '{store}' {paths}")

    def bench(label: str) -> float:
        machine.succeed("rm -rf /root/dst")
        t0 = time.monotonic()
        machine.succeed(
            f"nix copy --no-check-sigs --from '{store}' --to /root/dst {paths}"
        )
        dt = time.monotonic() - t0
        print(f"[bench] download/{label:9s} {dt:6.2f}s ({n_paths} paths)")
        return dt

    bench("loopback")

    machine.succeed(f"tc qdisc add dev lo root netem delay {int(rtt_s / 2 * 1000)}ms")
    try:
        rtt = machine.succeed("ping -c3 -q 127.0.0.1 | tail -1")
        print(f"[bench] {rtt.strip()}")
        delayed = bench("50ms-rtt")
    finally:
        machine.succeed("tc qdisc del dev lo root netem")

    # A client paying one round trip per path would need at least this long.
    sequential_floor = n_paths * rtt_s
    print(f"[bench] sequential floor {sequential_floor:6.2f}s")
    assert delayed < sequential_floor * 0.8, (
        f"download took {delayed:.2f}s, expected pipelining to stay well below "
        f"the sequential floor of {sequential_floor:.2f}s"
    )
  '';
}
