# README access-control example: step-ca and nix-grpc-daemon share one
# machine; server and host certs both come from the internal ACME CA
# (CN = FQDN via forceCN). Hosts matching the name glob substitute signed
# paths over gRPC while the ACL limits them to read-only.
{
  pkgs,
  nixPkgs,
  module,
}:

let
  # Runtime PKI under /run: a store path would be cached across test runs
  # and its certificates would eventually expire.
  pkiDir = "/run/step-ca-pki";

  signingSecretKey = "nix-grpc-test-1:1/icU6Hlts+rG2LxnM8NoIMcrLWAzdCgJEOLjewE8DxGQKUPC9+LF07Ci6sEjhQP2G50TfF9TkQFBwwVRW5FXw==";
  signingPublicKey = "nix-grpc-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";

  storeUri =
    "grpc://server:50051"
    + "?ca-cert=/run/root_ca.crt"
    + "&client-cert=/var/lib/acme/host1/cert.pem"
    + "&client-key=/var/lib/acme/host1/key.pem";
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-store-acme-substituter";
  globalTimeout = 600;

  nodes = {
    server =
      { config, ... }:
      {
        imports = [ module ];
        virtualisation.memorySize = 2048;

        nix.package = nixPkgs.nix-everything;
        nix.settings = {
          experimental-features = [ "nix-command" ];
          substituters = [ ];
        };

        systemd.services.step-ca-pki = {
          wantedBy = [ "multi-user.target" ];
          path = [ pkgs.step-cli ];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            RuntimeDirectory = "step-ca-pki";
            RuntimeDirectoryPreserve = true;
          };
          script = ''
            cd ${pkiDir}
            step certificate create "Test Root CA" root_ca.crt root_ca.key \
              --profile root-ca --no-password --insecure
            step certificate create "Test Intermediate CA" intermediate_ca.crt intermediate_ca.key \
              --profile intermediate-ca --ca root_ca.crt --ca-key root_ca.key \
              --no-password --insecure
            cat root_ca.crt intermediate_ca.crt > ca-bundle.pem
            chmod a+r ${pkiDir}/*
          '';
        };

        services.step-ca = {
          enable = true;
          address = "0.0.0.0";
          port = 8443;
          openFirewall = true;
          intermediatePasswordFile = "/dev/null";
          settings = {
            dnsNames = [ "server" ];
            root = "${pkiDir}/root_ca.crt";
            crt = "${pkiDir}/intermediate_ca.crt";
            key = "${pkiDir}/intermediate_ca.key";
            db = {
              type = "badger";
              dataSource = "/var/lib/step-ca/db";
            };
            authority.provisioners = [
              {
                type = "ACME";
                name = "acme";
                forceCN = true;
              }
            ];
          };
        };
        systemd.services.step-ca = {
          after = [ "step-ca-pki.service" ];
          requires = [ "step-ca-pki.service" ];
        };

        security.acme = {
          acceptTerms = true;
          defaults.email = "root@example.org";
          certs."server" = {
            server = "https://server:8443/acme/acme/directory";
            listenHTTP = ":80";
            group = "nix-grpc-daemon";
          };
        };
        networking.firewall.allowedTCPPorts = [
          80
          50051
        ];
        systemd.services.acme-order-renew-server = {
          after = [ "step-ca.service" ];
          requires = [ "step-ca.service" ];
          environment.LEGO_CA_CERTIFICATES = "${pkiDir}/root_ca.crt";
        };

        services.nix-grpc-daemon = {
          enable = true;
          package = config.programs.nix-grpc-store.package;
          tls = {
            certFile = "/var/lib/acme/server/fullchain.pem";
            keyFile = "/var/lib/acme/server/key.pem";
            clientCaFile = "${pkiDir}/ca-bundle.pem";
          };
          accessRules = [
            {
              cn = "host*";
              role = "read-only";
            }
          ];
        };
        programs.nix-grpc-store.enable = true;
        # The "Ensure certificate" unit only provides a selfsigned
        # placeholder; wait for the real ACME order.
        systemd.services.nix-grpc-daemon = {
          after = [ "acme-order-renew-server.service" ];
          requires = [ "acme-order-renew-server.service" ];
        };

        environment.etc."hello.nix".text = ''
          derivation {
            name = "hello-grpc";
            system = builtins.currentSystem;
            builder = "/bin/sh";
            args = [ "-c" "echo hello-over-grpc > $out" ];
          }
        '';
        environment.etc."cache-key".text = signingSecretKey;
      };

    host1 = {
      imports = [ module ];
      virtualisation.memorySize = 2048;

      # Trust-on-first-use fetch of the CA root, like `step ca root`.
      systemd.services.fetch-ca-root = {
        wantedBy = [ "multi-user.target" ];
        after = [ "network-online.target" ];
        wants = [ "network-online.target" ];
        path = [ pkgs.curl ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''
          curl -fsS --insecure --retry 60 --retry-all-errors --retry-delay 1 \
            -o /run/root_ca.crt https://server:8443/roots.pem
        '';
      };

      security.acme = {
        acceptTerms = true;
        defaults.email = "root@example.org";
        certs."host1" = {
          server = "https://server:8443/acme/acme/directory";
          listenHTTP = ":80";
        };
      };
      systemd.services.acme-order-renew-host1 = {
        after = [ "fetch-ca-root.service" ];
        requires = [ "fetch-ca-root.service" ];
        environment.LEGO_CA_CERTIFICATES = "/run/root_ca.crt";
      };
      networking.firewall.allowedTCPPorts = [ 80 ];

      nix.package = nixPkgs.nix-everything;
      programs.nix-grpc-store.enable = true;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        substituters = [ storeUri ];
        trusted-public-keys = [ signingPublicKey ];
      };
    };
  };

  testScript = ''
    start_all()
    server.wait_for_unit("step-ca.service")
    server.wait_for_unit("nix-grpc-daemon.socket")
    server.wait_for_open_port(50051)

    with subtest("host1 obtains a certificate via ACME"):
        # The "Ensure certificate" unit installs a selfsigned placeholder
        # first; wait until the ACME-issued cert replaced it.
        host1.wait_until_succeeds(
            "${pkgs.openssl}/bin/openssl x509 -in /var/lib/acme/host1/cert.pem -noout -subject -issuer "
            "| grep -q 'Test Intermediate CA'"
        )
        host1.succeed(
            "${pkgs.openssl}/bin/openssl x509 -in /var/lib/acme/host1/cert.pem -noout -subject "
            "| grep -q 'CN *= *host1'"
        )

    with subtest("server builds and signs a path"):
        p = server.succeed(
            "nix build --impure -f /etc/hello.nix --no-link --print-out-paths"
        ).strip()
        server.succeed(f"nix store sign -k /etc/cache-key '{p}'")

    with subtest("host1 substitutes the signed path over gRPC (read-only cert)"):
        host1.fail(f"test -e '{p}'")
        host1.succeed(f"nix-store -r '{p}'")
        host1.succeed(f"grep -q hello-over-grpc '{p}'")

    with subtest("read-only host1 cannot write"):
        host1.succeed("echo deny > /root/denyfile")
        host1.fail("nix store add --store '${storeUri}' /root/denyfile")
        server.succeed(
            "journalctl -u nix-grpc-daemon.service | "
            "grep -q 'event=denied .*cn=host1 role=read-only'"
        )
  '';
}
