# End-to-end test: run nix-grpc-daemon backed by the system nix-daemon,
# then drive it from a Nix client that loads the plugin and speaks grpc://.
#
# Verifies the whole stack:
#   nix CLI → plugin → gRPC → nix-grpc-daemon → unix:// nix-daemon → LocalStore
{
  pkgs,
  nixPkgs,
  module,
  mockOidc,
  e2eTests,
  # "main" or "upload"
  phase ? "main",
}:

let
  oidcAudience = "grpc://localhost:50052";
  # mockoidc serves discovery under /oidc on :8080, tokens are minted on :8081/issue.
  oidcConfig = (pkgs.formats.json { }).generate "oidc.json" {
    allow_insecure = true;
    providers.mock = {
      issuer = "http://127.0.0.1:8080/oidc";
      audience = oidcAudience;
      rules = [
        {
          bound_subject = [ "repo:myorg/*" ];
          scopes = [ "write" ];
        }
        {
          bound_claims.repository_owner = [ "admins" ];
          scopes = [ "admin" ];
        }
        {
          bound_claims.groups = [ "readers" ];
          scopes = [ "read" ];
        }
      ];
    };
  };
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
  name = "nix-grpc-store-${phase}";
  globalTimeout = 600;

  # The Rust tests in tests/e2e run on the driver host and drive the VM over
  # the vsock ssh backdoor, so independent tests overlap.
  sshBackdoor.enable = true;
  defaults.virtualisation.qemu.enableSharedMemory = true;
  # The ssh generator only opens the vsock socket if /dev/vsock exists when it runs.
  defaults.boot.initrd.kernelModules = [ "vmw_vsock_virtio_transport" ];

  nodes.machine =
    { config, lib, ... }:
    {
      imports = [
        module
        ./lib/envoy-proxy.nix
      ];

      virtualisation.memorySize = 2048;
      virtualisation.diskSize = 24576;
      # The default overlay lives in RAM, and the 1 GiB uploads need more.
      virtualisation.writableStoreUseTmpfs = false;
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
        # Test VM disk is ~1 GiB.
        minFree = "0";
        # Reuse the client bundle so the test doesn't compile the project twice.
        package = config.programs.nix-grpc-store.package;
      };
      # Bulk-upload subtest needs the daemon to be a trusted user so
      # nix copy --to can add unsigned paths.
      services.nix-grpc-daemon.trustClients = true;

      environment.systemPackages = [
        pkgs.perf
        pkgs.curl
        pkgs.openssl
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

      # Builders write arbitrary bytes to stderr; the log stream must carry
      # them.
      environment.etc."rawlog.nix".text = ''
        derivation {
          name = "rawlog-grpc";
          system = builtins.currentSystem;
          builder = "/bin/sh";
          args = [ "-c" "printf 'not utf-8: \\377\\n' >&2; echo rawlog-over-grpc > $out" ];
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

      systemd.services.mock-oidc = {
        wantedBy = [ "multi-user.target" ];
        serviceConfig.ExecStart = "${lib.getExe mockOidc} -addr 127.0.0.1:8080";
      };

      # Second instance with mTLS and OIDC (module covers only one; hand-roll).
      # Not ordered after mock-oidc on purpose: the issuer may be down at boot.
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
            --oidc-config ${oidcConfig} \
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
    ${import ./lib/e2e.nix { inherit pkgs e2eTests; }}
    machine.wait_for_unit("nix-daemon.socket")
    machine.wait_for_unit("nix-grpc-daemon.socket")

    env = {
        "NGS_CERT_DIR": "${certDir}",
        "NGS_OIDC_AUDIENCE": "${oidcAudience}",
        "NGS_SIGNING_KEY": "${signingSecretKey}",
    }
  '' + pkgs.lib.optionalString (phase == "main") ''
    with subtest("daemon lifecycle"):
        run_e2e({"machine": machine}, env, "lifecycle::", "--test-threads=1")

    with subtest("transfers, access control, logs and metrics"):
        run_e2e({"machine": machine}, env, "transfer::", "access::", "--test-threads=4")

    with subtest("benchmarks"):
        run_e2e({"machine": machine}, env, "bench::", "--test-threads=1")
  '' + pkgs.lib.optionalString (phase == "upload") ''
    with subtest("concurrent uploads of one input"):
        run_e2e({"machine": machine}, env, "upload::", "--test-threads=1")
  '';
}
