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
  # Static throwaway signing key: the ACL subtest needs the daemon's
  # nix-daemon to trust a key at eval time (trusted-public-keys is static
  # nix.conf), so it cannot be generated at runtime like the TLS certs.
  signingSecretKey = "nix-grpc-test-1:1/icU6Hlts+rG2LxnM8NoIMcrLWAzdCgJEOLjewE8DxGQKUPC9+LF07Ci6sEjhQP2G50TfF9TkQFBwwVRW5FXw==";
  signingPublicKey = "nix-grpc-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
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
        trusted-public-keys = [ signingPublicKey ];
      };

      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "127.0.0.1:50051";
        logLevel = "debug";
        idleTimeout = 3;
        # Reuse the client bundle so the test doesn't compile the project twice.
        package = config.programs.nix-grpc-store.package;
      };
      # Bulk-upload subtest needs the daemon to be a trusted user so
      # nix copy --to can add unsigned paths.
      services.nix-grpc-daemon.trustClients = true;

      environment.systemPackages = [
        pkgs.perf
        pkgs.curl
      ];
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

      # Bench corpora as input-addressed outputs: CA paths from `nix store
      # add` are re-hashed on import (RewritingSink), skewing the benchmark.
      environment.etc."blob.nix".text = ''
        let
          mk = name: cmd:
            derivation {
              inherit name;
              system = builtins.currentSystem;
              builder = "/bin/sh";
              args = [ "-c" cmd ];
              PATH = builtins.storePath "${pkgs.coreutils}" + "/bin";
            };
        in
        {
          rand = mk "blob-rand" "dd if=/dev/urandom of=$out bs=1M count=256 status=none";
          text = mk "blob-text" "base64 /dev/zero | head -c $((256*1024*1024)) > $out";
        }
      '';
      system.extraDependencies = [ pkgs.coreutils ];

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
          issue() {
            openssl req -newkey rsa:2048 -nodes \
              -keyout $1.key -out $1.csr -subj /CN=$2
            openssl x509 -req -in $1.csr -days 1 \
              -CA ca.pem -CAkey ca.key -set_serial 0x$RANDOM \
              -extfile <(printf 'subjectAltName=DNS:localhost') \
              -out $1.pem
          }
          issue server localhost
          issue client localhost
          # Distinct CNs for the ACL subtest.
          issue ro ro-client
          issue rw rw-client
          issue stranger stranger
          openssl req -newkey rsa:2048 -nodes \
            -keyout expired.key -out expired.csr -subj /CN=localhost
          openssl x509 -req -in expired.csr -not_before 20200101000000Z -not_after 20200102000000Z \
            -CA ca.pem -CAkey ca.key -set_serial 0x$RANDOM -out expired.pem
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
            --client-ca ${certDir}/ca.pem \
            --allow localhost=trusted \
            --allow ro-client=read-only \
            --allow rw-client=write \
            --allow-anonymous read-only \
            --metrics-listen 127.0.0.1:9464
        '';
      };

      systemd.services.nix-grpc-daemon-strict = {
        wantedBy = [ "multi-user.target" ];
        after = [
          "nix-daemon.socket"
          "nix-grpc-certs.service"
        ];
        requires = [ "nix-grpc-certs.service" ];
        serviceConfig.ExecStart = ''
          ${lib.getExe config.services.nix-grpc-daemon.package} --listen 127.0.0.1:50053 \
            --proxy-socket /nix/var/nix/daemon-socket/socket \
            --tls-cert ${certDir}/server.pem --tls-key ${certDir}/server.key \
            --client-ca ${certDir}/ca.pem \
            --allow localhost=trusted
        '';
      };
    };

  testScript = ''
    machine.wait_for_unit("nix-daemon.socket")
    machine.wait_for_unit("nix-grpc-daemon.socket")

    store = "grpc://127.0.0.1:50051?insecure=1"
    store_mtls = (
        "grpc://localhost:50052"
        "?ca-cert=${certDir}/ca.pem"
        "&client-cert=${certDir}/client.pem"
        "&client-key=${certDir}/client.key"
    )

    with subtest("socket activation and idle exit"):
        machine.require_unit_state("nix-grpc-daemon.service", "inactive")
        out = machine.succeed(f"nix store info --json --store '{store}'")
        assert '"url":"grpc://127.0.0.1:50051' in out, out
        # Type=notify: only READY=1 from the daemon makes it "active".
        machine.require_unit_state("nix-grpc-daemon.service", "active")
        machine.wait_until_succeeds("journalctl -u nix-grpc-daemon --since=-1min | grep -q event=idle_exit", timeout=30)
        machine.wait_until_succeeds("systemctl show -P ActiveState nix-grpc-daemon.service | grep -qx inactive", timeout=30)
        machine.succeed(f"nix store info --store '{store}'")

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
        # Cert-less clients are anonymous: reads work (--allow-anonymous
        # read-only), mutations are denied.
        store_anon = "grpc://localhost:50052?ca-cert=${certDir}/ca.pem"
        machine.succeed(f"nix path-info --store '{store_anon}' '{p}'")
        # Fresh content: an already-valid path would no-op without a write.
        machine.succeed("head -c 64 /dev/urandom | base64 > /root/denyfile")
        machine.fail(f"nix store add --store '{store_anon}' /root/denyfile")
        machine.succeed(
            "journalctl -u nix-grpc-daemon-mtls.service | "
            "grep -q 'event=denied method=Connect cn=- role=read-only'"
        )
        # With a client cert it succeeds and can round-trip a build.
        machine.succeed(f"nix store info --json --store '{store_mtls}'")
        machine.succeed(
            f"nix build --store '{store_mtls}' --impure -f /etc/hello.nix "
            "--no-link --print-out-paths"
        )

    with subtest("missing client cert yields a readable error"):
        machine.wait_for_unit("nix-grpc-daemon-strict.service")
        machine.wait_for_open_port(50053)
        store_strict = "grpc://localhost:50053?ca-cert=${certDir}/ca.pem"
        err = machine.fail(f"nix path-info --store '{store_strict}' '{p}' 2>&1")
        print(err)
        assert "requires a TLS client certificate" in err, err
        assert "no client certificate was presented" in err, err
        err = machine.fail(
            f"nix path-info --store '{store_strict}&client-cert=${certDir}/expired.pem"
            f"&client-key=${certDir}/expired.key' '{p}' 2>&1"
        )
        assert "has expired" in err, err
        machine.succeed(
            f"nix path-info --store '{store_strict}&client-cert=${certDir}/client.pem"
            f"&client-key=${certDir}/client.key' '{p}'"
        )

    with subtest("default client cert lookup in /var/lib/nix-grpc-store"):
        machine.succeed(
            "install -d /var/lib/nix-grpc-store",
            "install -m 0644 ${certDir}/client.pem /var/lib/nix-grpc-store/client.crt",
            "install -m 0600 ${certDir}/client.key /var/lib/nix-grpc-store/client.key",
        )
        # No client-cert/client-key URI params: falls back to /var/lib. A
        # mutation proves the trusted cert is picked up, since reads would
        # succeed anonymously too.
        machine.succeed(
            "nix store add --store "
            "'grpc://localhost:50052?ca-cert=${certDir}/ca.pem' /etc/hello.nix"
        )
        machine.succeed("rm -r /var/lib/nix-grpc-store")

    with subtest("certificate ACL: read-only role"):
        def cert_store(name: str) -> str:
            return (
                "grpc://localhost:50052"
                "?ca-cert=${certDir}/ca.pem"
                f"&client-cert=${certDir}/{name}.pem"
                f"&client-key=${certDir}/{name}.key"
            )

        store_ro = cert_store("ro")
        # Queries and downloads work. (`nix store info` would tunnel for the
        # daemon version, so probe with a native path query instead.)
        machine.succeed(f"nix path-info --store '{store_ro}' '{p}'")
        machine.succeed(
            f"nix copy --no-check-sigs --from '{store_ro}' --to /root/ro-dst '{p}'"
        )
        # Mutations are denied: store add and gc need the tunnel, build needs
        # the build permission.
        machine.fail(f"nix store add --store '{store_ro}' /root/denyfile")
        machine.fail(
            f"nix build --store '{store_ro}' --impure -f /etc/hello.nix --no-link"
        )
        machine.succeed(
            "journalctl -u nix-grpc-daemon-mtls.service | "
            "grep -q 'event=denied method=Connect cn=ro-client role=read-only'"
        )

    with subtest("certificate ACL: write role enforces signatures"):
        store_rw = cert_store("rw")
        # Input-addressed output: a CA path (nix store add) would pass
        # CheckSigs without a signature.
        machine.succeed(
            "printf 'derivation { name = \"acl-blob\"; "
            "system = builtins.currentSystem; builder = \"/bin/sh\"; "
            "args = [ \"-c\" \"echo acl-payload > $out\" ]; }' > /root/acl.nix"
        )
        up = machine.succeed(
            "nix build --store /root/aclsrc --impure -f /root/acl.nix "
            "--no-link --print-out-paths"
        ).strip()
        # The client asks to skip signature checks, but the server forces
        # CheckSigs for the write role, so the unsigned path is rejected.
        machine.fail(
            f"nix copy --no-check-sigs --from /root/aclsrc --to '{store_rw}' '{up}'"
        )
        # After signing with a key the daemon trusts, the same copy succeeds.
        machine.succeed(
            "install -m 0600 /dev/null /root/cache-key && "
            "echo '${signingSecretKey}' > /root/cache-key"
        )
        machine.succeed(f"nix store sign -k /root/cache-key --store /root/aclsrc '{up}'")
        machine.succeed(
            f"nix copy --no-check-sigs --from /root/aclsrc --to '{store_rw}' '{up}'"
        )
        machine.succeed(f"test -e '{up}'")
        # The opaque worker-protocol tunnel stays off limits.
        machine.fail(f"nix store add --store '{store_rw}' /root/denyfile")

    with subtest("certificate ACL: write role builds via the native BuildPaths RPC"):
        # Evaluation stays local; the drv closure is imported
        # (content-addressed, passes CheckSigs) and built server-side, so no
        # trusted role or worker-protocol tunnel is needed.
        machine.succeed(
            "printf 'derivation { name = \"bp-blob\"; "
            "system = builtins.currentSystem; builder = \"/bin/sh\"; "
            "args = [ \"-c\" \"echo bp-payload > $out\" ]; }' > /root/bp.nix"
        )
        out = machine.succeed(
            f"nix build --store '{store_rw}' --eval-store auto --impure "
            "-f /root/bp.nix --no-link --print-out-paths"
        ).strip()
        machine.succeed(f"grep -q bp-payload '{out}'")
        machine.succeed(
            "journalctl -u nix-grpc-daemon-mtls.service | "
            "grep -q 'event=rpc method=BuildPaths cn=rw-client'"
        )
        # A failing build reports the error without the tunnel.
        machine.succeed(
            "printf 'derivation { name = \"bp-fail\"; "
            "system = builtins.currentSystem; builder = \"/bin/sh\"; "
            "args = [ \"-c\" \"exit 1\" ]; }' > /root/bp-fail.nix"
        )
        machine.fail(
            f"nix build --store '{store_rw}' --eval-store auto --impure "
            "-f /root/bp-fail.nix --no-link"
        )
        # Repair rewrites existing store paths and stays trusted-only.
        machine.fail(
            f"nix build --store '{store_rw}' --eval-store auto --impure "
            "-f /root/bp.nix --no-link --repair"
        )
        machine.succeed(
            "journalctl -u nix-grpc-daemon-mtls.service | "
            "grep -q 'event=denied method=BuildPaths(repair) cn=rw-client'"
        )

    with subtest("certificate ACL: unmatched CN is denied"):
        machine.fail(f"nix store info --store '{cert_store('stranger')}'")
        machine.succeed(
            "journalctl -u nix-grpc-daemon-mtls.service | "
            "grep -q 'event=denied method=.* cn=stranger role=none'"
        )

    with subtest("access log attributes clients by certificate CN"):
        machine.succeed(
            "journalctl -u nix-grpc-daemon-mtls.service | "
            "grep -E 'event=session_end method=Connect cn=localhost .*bytes_out=[0-9]+'"
        )
        machine.succeed(
            "journalctl -u nix-grpc-daemon.service | "
            "grep -E 'event=session_end method=Connect cn=- '"
        )
        # Path queries only show up at --log-level debug.
        machine.succeed(
            "journalctl -u nix-grpc-daemon.service | "
            "grep -q 'level=debug event=rpc method=QueryPathInfos'"
        )
        machine.fail(
            "journalctl -u nix-grpc-daemon-mtls.service | grep -q level=debug"
        )

    with subtest("prometheus metrics labelled by certificate CN"):
        machine.succeed(
            "curl -sf http://127.0.0.1:9464/metrics | "
            "grep -E 'nix_grpc_rpcs_total\\{.*cn=\"localhost\".*method=\"Connect\".*\\} [0-9]+'"
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

        for tag in ("text", "rand"):
            blob = machine.succeed(
                f"nix build --impure -f /etc/blob.nix {tag} "
                "--no-link --print-out-paths"
            ).strip()

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
