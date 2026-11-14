# Certificates for a NixOS test, made while the test runs: every node gets
# /run/test-certs/{ca,cert}.pem and key.pem from test-certs.service. One node
# creates the CA in the directory all test VMs share; the others wait for it.
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.testCerts;
  shared = "/tmp/shared/test-ca";
in
{
  options.testCerts = {
    commonName = lib.mkOption { type = lib.types.str; };
    subjectAltName = lib.mkOption {
      type = lib.types.str;
      example = "DNS:worker1,IP:192.168.1.3";
    };
    createsCa = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Set on exactly one node.";
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
      ${lib.optionalString cfg.createsCa ''
        mkdir -p ${shared}.tmp
        openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=test-ca \
          -keyout ${shared}.tmp/ca.key -out ${shared}.tmp/ca.pem
        mv ${shared}.tmp ${shared}
      ''}
      until [ -e ${shared}/ca.pem ]; do sleep 0.2; done
      mkdir -p /run/test-certs && cd /run/test-certs
      cp ${shared}/ca.pem ca.pem
      openssl req -newkey rsa:2048 -nodes -keyout key.pem -out csr.pem -subj /CN=${cfg.commonName}
      openssl x509 -req -in csr.pem -days 2 -CA ca.pem -CAkey ${shared}/ca.key \
        -set_serial 0x$(openssl rand -hex 8) -out cert.pem \
        -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth' ${lib.escapeShellArg cfg.subjectAltName})
      chmod 644 key.pem
    '';
  };
}
