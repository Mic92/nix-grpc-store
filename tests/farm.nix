# PLAN.md end to end: niks3 + S3, two farm workers, one client with
# --eval-store. No load balancer yet, the client talks to worker1.
{
  pkgs,
  nixPkgs,
  module,
  niks3,
}:

let
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  tokenFile = pkgs.writeText "niks3-token" apiToken;
  s3Key = "rustfsadmin";
  signingSecretKey = pkgs.writeText "key" "farm-test-1:1/icU6Hlts+rG2LxnM8NoIMcrLWAzdCgJEOLjewE8DxGQKUPC9+LF07Ci6sEjhQP2G50TfF9TkQFBwwVRW5FXw==";
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://cache:5751";
  niks3Pkgs = niks3.packages.${pkgs.stdenv.hostPlatform.system};

  common = {
    virtualisation.memorySize = 1536;
    nix.package = nixPkgs.nix-everything;
    nix.settings.experimental-features = [ "nix-command" ];
  };

  worker =
    { config, lib, ... }:
    {
      imports = [
        common
        module
      ];
      services.nix-grpc-daemon = {
        enable = true;
        listen = "0.0.0.0:50051";
        logLevel = "debug";
        idleTimeout = null;
        package = config.programs.nix-grpc-store.package;
        farm = {
          enable = true;
          niks3Package = niks3Pkgs.niks3;
          inherit niks3Url;
          tokenFile = toString tokenFile;
          cacheUrl = niks3Url;
          publicKeys = [ signingPublicKey ];
        };
      };
      programs.nix-grpc-store.enable = true;
      networking.firewall.allowedTCPPorts = [ 50051 ];
    };

  # Distinct name per run so the cache never already has it.
  jobExpr = pkgs.writeText "job.nix" ''
    { tag }:
    let
      mk = name: deps: derivation {
        inherit name;
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${name}-''${tag} ''${toString deps} > $out" ];
      };
      a = mk "farm-a" [ ];
      b = mk "farm-b" [ a ];
    in
    mk "farm-top" [ a b ]
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-farm";
  globalTimeout = 900;

  nodes = {
    cache =
      { ... }:
      {
        imports = [
          common
          niks3.nixosModules.niks3
        ];
        services.niks3 = {
          enable = true;
          package = niks3Pkgs.niks3;
          httpAddr = "0.0.0.0:5751";
          apiTokenFile = toString tokenFile;
          signKeyFiles = [ signingSecretKey ];
          readProxy.enable = true;
          s3 = {
            endpoint = "cache:9000";
            bucket = "farm";
            useSSL = false;
            accessKeyFile = pkgs.writeText "ak" s3Key;
            secretKeyFile = pkgs.writeText "sk" s3Key;
          };
        };
        systemd.services.rustfs = {
          wantedBy = [ "multi-user.target" ];
          serviceConfig = {
            ExecStart = "${pkgs.rustfs}/bin/rustfs --address 0.0.0.0:9000 --access-key ${s3Key} --secret-key ${s3Key} /var/lib/rustfs";
            StateDirectory = "rustfs";
            DynamicUser = true;
          };
        };
        systemd.services.rustfs-bucket = {
          requires = [ "rustfs.service" ];
          after = [ "rustfs.service" ];
          before = [ "niks3.service" ];
          requiredBy = [ "niks3.service" ];
          environment = {
            S3_ENDPOINT_URL = "http://cache:9000";
            AWS_ACCESS_KEY_ID = s3Key;
            AWS_SECRET_ACCESS_KEY = s3Key;
          };
          path = [ pkgs.s5cmd ];
          script = ''
            for i in $(seq 60); do s5cmd ls && break; sleep 1; done
            s5cmd mb s3://farm || true
          '';
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
          };
        };
        networking.firewall.allowedTCPPorts = [
          5751
          9000
        ];
      };

    worker1 = worker;
    worker2 = worker;

    client =
      { ... }:
      {
        imports = [
          common
          module
        ];
        programs.nix-grpc-store.enable = true;
        nix.settings.substituters = pkgs.lib.mkForce [ ];
      };
  };

  testScript = ''
    start_all()
    cache.wait_for_unit("niks3.service")
    cache.wait_for_open_port(5751)
    for w in [worker1, worker2]:
        w.wait_for_unit("nix-grpc-daemon.socket")

    farm = "grpc://worker1:50051?insecure=1"

    with subtest("without --eval-store the farm refuses and hints"):
        out = client.fail(f"nix build --store '{farm}' -f ${jobExpr} --argstr tag t0 2>&1")
        assert "--eval-store" in out, out

    with subtest("fan-out build lands in the cache"):
        client.succeed(f"nix build -L --store '{farm}' --eval-store auto -f ${jobExpr} --argstr tag t1 >&2")
        top = client.succeed("nix eval --raw -f ${jobExpr} --argstr tag t1 outPath").strip()
        # The client store has nothing. The cache does.
        client.fail(f"test -e {top}")
        client.succeed(f"nix copy --from ${niks3Url} --no-check-sigs {top} && grep farm-top-t1 {top}")

    with subtest("second worker answers from the claim without building"):
        client.succeed("nix build -L --store 'grpc://worker2:50051?insecure=1' --eval-store auto -f ${jobExpr} --argstr tag t1 >&2")
        worker2.fail(f"test -e {top}")
        worker2.fail("journalctl -u nix-daemon | grep -q 'building.*farm-top'")
  '';
}
