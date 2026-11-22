# Certificates for a NixOS test, made while the test runs: test-certs.service
# writes /run/test-certs/<name>.pem and <name>.key for every entry of
# testCerts.certs, plus <ca>.pem for every CA. One node creates the CAs in the
# directory all test VMs share; the others wait for them.
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.testCerts;
  shared = "/tmp/shared/test-cas";
  bundle = "/etc/ssl/certs/ca-certificates.crt";
in
{
  options.testCerts = {
    certs = lib.mkOption {
      default = { };
      type = lib.types.attrsOf (
        lib.types.submodule {
          options = {
            commonName = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              description = "Without one the subject is as SPIRE writes it.";
            };
            subjectAltName = lib.mkOption {
              type = lib.types.str;
              example = "DNS:worker1,IP:192.168.1.3";
            };
            ca = lib.mkOption {
              type = lib.types.str;
              default = "ca";
            };
          };
        }
      );
    };
    cas = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ "ca" ];
      description = "Must be the same on every node.";
    };
    createsCas = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Set on exactly one node.";
    };
    trusted = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "CAs to add to the system trust store.";
    };
    neededBy = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Units that read the certificates.";
    };
  };

  config.systemd.services.test-certs = {
    wantedBy = [ "multi-user.target" ];
    requiredBy = cfg.neededBy;
    before = cfg.neededBy;
    path = [ pkgs.openssl ];
    serviceConfig.Type = "oneshot";
    serviceConfig.RemainAfterExit = true;
    script = ''
      ${lib.optionalString cfg.createsCas ''
        mkdir -p ${shared}.tmp
        for ca in ${lib.escapeShellArgs cfg.cas}; do
          openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=$ca \
            -keyout ${shared}.tmp/$ca.key -out ${shared}.tmp/$ca.pem
        done
        mv ${shared}.tmp ${shared}
      ''}
      until [ -d ${shared} ]; do sleep 0.2; done
      mkdir -p /run/test-certs && cd /run/test-certs
      cp ${shared}/*.pem .
      issue() {
        openssl req -newkey rsa:2048 -nodes -keyout $1.key -out $1.csr -subj "$2"
        openssl x509 -req -in $1.csr -days 2 -CA $4.pem -CAkey ${shared}/$4.key \
          -set_serial 0x$(openssl rand -hex 8) -out $1.pem \
          -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth' "$3")
        rm $1.csr
        # The daemon and Envoy run as their own users.
        chmod 644 $1.key
      }
      ${lib.concatStrings (
        lib.mapAttrsToList (name: cert: ''
          issue ${
            lib.escapeShellArgs [
              name
              (if cert.commonName == null then "/C=US/O=SPIRE" else "/CN=${cert.commonName}")
              cert.subjectAltName
              cert.ca
            ]
          }
        '') cfg.certs
      )}
      ${lib.optionalString (cfg.trusted != [ ]) ''
        cat ${bundle} ${lib.concatMapStringsSep " " (ca: "${ca}.pem") cfg.trusted} > bundle.crt
        ln -sf /run/test-certs/bundle.crt ${bundle}
      ''}
    '';
  };

  # Switching to another configuration puts the stock bundle back.
  config.system.activationScripts.test-certs-trust = lib.mkIf (cfg.trusted != [ ]) {
    deps = [ "etc" ];
    text = "[ ! -e /run/test-certs/bundle.crt ] || ln -sf /run/test-certs/bundle.crt ${bundle}";
  };
}
