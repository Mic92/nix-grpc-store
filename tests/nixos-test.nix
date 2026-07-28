# End-to-end test: run nix-grpc-daemon backed by the system nix-daemon,
# then drive it from a Nix client that loads the plugin and speaks grpc://.
#
# Verifies the whole stack:
#   nix CLI → plugin → gRPC → nix-grpc-daemon → unix:// nix-daemon → LocalStore
{
  pkgs,
  nixPkgs,
  module,
}:

let
  # Certs live under /run so they are freshly generated on every test run
  # (a store path would be cached and eventually expire).
  certDir = "/run/nix-grpc-certs";
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-store";
  globalTimeout = 600;

  nodes.machine =
    { config, lib, ... }:
    {
      imports = [ module ];

      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      # Must be a Nix version the plugin bundle contains a build for.
      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        # Keep the benchmark deterministic and offline.
        substituters = [ ];
      };

      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "127.0.0.1:50051";
        # Reuse the client bundle so the test doesn't compile the project twice.
        package = config.programs.nix-grpc-store.package;
      };
      # Bulk-upload subtest needs the daemon to be a trusted user so
      # nix copy --to can add unsigned paths.
      services.nix-grpc-daemon.trustClients = true;

      environment.systemPackages = [ pkgs.perf ];
      # Allow perf to resolve kernel symbols and record system-wide as root.
      boot.kernel.sysctl."kernel.kptr_restrict" = 0;
      boot.kernel.sysctl."kernel.perf_event_paranoid" = -1;

      # A trivial derivation we can add / build without network.
      environment.etc."hello.nix".text = ''
        derivation {
          name = "hello-grpc";
          system = builtins.currentSystem;
          builder = "/bin/sh";
          args = [ "-c" "echo hello-over-grpc > $out" ];
        }
      '';

      # Self-signed CA plus server/client leaf certs for the mTLS subtest.
      systemd.services.nix-grpc-certs = {
        wantedBy = [ "multi-user.target" ];
        path = [ pkgs.openssl ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          RuntimeDirectory = "nix-grpc-certs";
          RuntimeDirectoryPreserve = true;
        };
        script = ''
          cd ${certDir}
          openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
            -keyout ca.key -out ca.pem -subj /CN=nix-grpc-ca
          for n in server client; do
            openssl req -newkey rsa:2048 -nodes \
              -keyout $n.key -out $n.csr -subj /CN=localhost
            openssl x509 -req -in $n.csr -days 1 \
              -CA ca.pem -CAkey ca.key -set_serial 0x$RANDOM \
              -extfile <(printf 'subjectAltName=DNS:localhost') \
              -out $n.pem
          done
          chmod a+r ${certDir}/*
        '';
      };

      # Second instance with mTLS enabled (module covers only one; hand-roll).
      systemd.services.nix-grpc-daemon-mtls = {
        wantedBy = [ "multi-user.target" ];
        after = [
          "nix-daemon.socket"
          "nix-grpc-certs.service"
        ];
        requires = [ "nix-grpc-certs.service" ];
        serviceConfig.ExecStart = ''
          ${lib.getExe config.services.nix-grpc-daemon.package} --listen 127.0.0.1:50052 \
            --proxy-socket /nix/var/nix/daemon-socket/socket \
            --tls-cert ${certDir}/server.pem --tls-key ${certDir}/server.key \
            --client-ca ${certDir}/ca.pem
        '';
      };
    };

  testScript = ''
    machine.wait_for_unit("nix-daemon.socket")
    machine.wait_for_unit("nix-grpc-daemon.service")
    machine.wait_for_open_port(50051)

    store = "grpc://127.0.0.1:50051?insecure=1"
    store_mtls = (
        "grpc://localhost:50052"
        "?ca-cert=${certDir}/ca.pem"
        "&client-cert=${certDir}/client.pem"
        "&client-key=${certDir}/client.key"
    )

    with subtest("store ping over gRPC"):
        out = machine.succeed(f"nix store info --json --store '{store}'")
        print(out)
        assert '"url":"grpc://127.0.0.1:50051' in out, out

    with subtest("add path over gRPC and read it back locally"):
        p = machine.succeed(
            f"nix store add --store '{store}' /etc/hello.nix"
        ).strip()
        # The gRPC daemon forwarded to the local nix-daemon, so the path must
        # exist in the real /nix/store.
        machine.succeed(f"nix path-info '{p}'")
        machine.succeed(f"test -e '{p}'")

    with subtest("build over gRPC"):
        p = machine.succeed(
            f"nix build --store '{store}' --impure -f /etc/hello.nix "
            "--no-link --print-out-paths"
        ).strip()
        machine.succeed(f"grep -q hello-over-grpc '{p}'")

    with subtest("copy from gRPC store to a local scratch store"):
        machine.succeed(
            f"nix copy --no-check-sigs --from '{store}' "
            f"--to /root/scratch '{p}'"
        )
        machine.succeed(f"test -e /root/scratch/nix/store/$(basename '{p}')")

    with subtest("mTLS"):
        machine.wait_for_unit("nix-grpc-daemon-mtls.service")
        machine.wait_for_open_port(50052)
        # Without a client cert the handshake must fail.
        machine.fail(
            "nix store info --store "
            "'grpc://localhost:50052?ca-cert=${certDir}/ca.pem'"
        )
        # With a client cert it succeeds and can round-trip a build.
        machine.succeed(f"nix store info --json --store '{store_mtls}'")
        machine.succeed(
            f"nix build --store '{store_mtls}' --impure -f /etc/hello.nix "
            "--no-link --print-out-paths"
        )

    with subtest("bulk upload over gRPC"):
        # Exercises server-side writeFull() on the daemon socket under
        # backpressure (readCoalesced() shares the fd with this write path).
        machine.succeed(
            "dd if=/dev/urandom of=/root/upblob bs=1M count=128 status=none"
        )
        up = machine.succeed(
            "nix store add --store /root/upsrc --mode flat /root/upblob"
        ).strip()
        machine.succeed(
            f"nix copy --no-check-sigs --from /root/upsrc --to '{store}' '{up}'"
        )
        machine.succeed(f"test -e '{up}'")

    with subtest("many small paths round-trip (native AddMultipleToStore / NarsFromPaths)"):
        import time

        # 200 small referencing paths: exercises one bulk-import RPC on upload
        # and per-path NarFromPath streams on download.
        machine.succeed(
            "mkdir -p /root/small && "
            "for i in $(seq 200); do "
            "  head -c 4096 /dev/urandom | base64 > /root/small/f$i; "
            "done"
        )
        # One invocation: per-path `nix store add` would spend most of the
        # time on CLI startup and store opening.
        small = machine.succeed(
            "cd /root/small && nix-store --store /root/smallsrc --add f*"
        ).splitlines()
        assert len(small) == 200, small
        paths = " ".join(f"'{p}'" for p in small)

        t0 = time.monotonic()
        machine.succeed(
            f"nix copy --no-check-sigs --from /root/smallsrc --to '{store}' {paths}"
        )
        print(f"[bench] small/upload   {time.monotonic() - t0:6.2f}s (200 paths)")
        for p in small[:3]:
            machine.succeed(f"test -e '{p}'")

        t0 = time.monotonic()
        machine.succeed(
            f"nix copy --no-check-sigs --from '{store}' --to /root/smalldst {paths}"
        )
        print(f"[bench] small/download {time.monotonic() - t0:6.2f}s (200 paths)")
        for p in small[:3]:
            machine.succeed(f"test -e /root/smalldst/nix/store/$(basename '{p}')")
            machine.succeed(
                f"cmp /root/smallsrc/nix/store/$(basename '{p}') "
                f"/root/smalldst/nix/store/$(basename '{p}')"
            )

    with subtest("throughput benchmark: gRPC vs unix-socket daemon"):
        import time

        def copy_cmd(uri: str, path: str) -> str:
            return (
                f"nix copy --no-check-sigs --from '{uri}' --to /root/bench '{path}'"
            )

        def bench(label: str, uri: str, path: str) -> float:
            machine.succeed("rm -rf /root/bench && mkdir -p /root/bench")
            t0 = time.monotonic()
            machine.succeed(copy_cmd(uri, path))
            dt = time.monotonic() - t0
            print(f"[bench] {label:16s} {dt:6.2f}s  {256 / dt:6.1f} MiB/s")
            return dt

        # Two corpora: incompressible (urandom) and highly compressible
        # (base64 of zeros) so zstd's effect is visible in both directions.
        corpora = {
            # `head` closing the pipe makes base64 exit non-zero under
            # pipefail; that is the expected way to bound the output.
            "text": "set +o pipefail; "
            "base64 /dev/zero | head -c $((256*1024*1024)) > /root/blob",
            "rand": "dd if=/dev/urandom of=/root/blob bs=1M count=256 status=none",
        }
        for tag, gen in corpora.items():
            machine.succeed(gen)
            blob = machine.succeed("nix store add --mode flat /root/blob").strip()

            bench(f"{tag}/warmup", "daemon", blob)
            t_unix = bench(f"{tag}/unix", "daemon", blob)
            t_grpc = bench(f"{tag}/grpc", store, blob)
            print(f"[bench] {tag}: grpc={t_grpc / t_unix:.2f}x unix")

    with subtest("perf profile of gRPC copy"):
        # perf stat: cheap, always-comparable counters for both transports.
        for label, uri in (("unix", "daemon"), ("grpc", store)):
            machine.succeed("rm -rf /root/bench && mkdir -p /root/bench")
            out = machine.succeed(
                "perf stat -a "
                "-e task-clock,context-switches,cycles,instructions,"
                "cache-misses,syscalls:sys_enter_read,syscalls:sys_enter_write "
                f"-- {copy_cmd(uri, blob)} 2>&1"
            )
            print(f"[perf-stat {label}]\n{out}")

        # perf record: system-wide so both the nix client (plugin) and
        # nix-grpc-daemon are sampled in one profile.
        machine.succeed("rm -rf /root/bench && mkdir -p /root/bench")
        machine.succeed(
            "perf record -a -g -F 999 -o /root/perf.data -- " + copy_cmd(store, blob)
        )
        report = machine.succeed(
            "perf report -i /root/perf.data --stdio --no-children "
            "--percent-limit 0.5 2>/dev/null | head -80"
        )
        print("[perf-report grpc top functions]\n" + report)

        # Per-DSO breakdown highlights where time goes even without symbols
        # (grpc/protobuf/libnixstore in nixpkgs are stripped).
        dso = machine.succeed(
            "perf report -i /root/perf.data --stdio --sort=dso "
            "--percent-limit 0.5 2>/dev/null | head -40"
        )
        print("[perf-report grpc per-DSO]\n" + dso)
  '';
}
