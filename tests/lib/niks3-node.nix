# niks3 + rustfs + postgres on one NixOS node, shared by farm.nix and k3s.nix.
{
  pkgs,
  niks3,
  listenHost, # what S3/niks3 bind and advertise
  apiToken,
}:
let
  s3Key = "rustfsadmin";
in
{
  imports = [ niks3.nixosModules.niks3 ];

  # Public half: farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=
  services.niks3 = {
    enable = true;
    package = niks3.packages.${pkgs.stdenv.hostPlatform.system}.niks3;
    httpAddr = "0.0.0.0:5751";
    apiTokenFile = toString (pkgs.writeText "niks3-token" apiToken);
    signKeyFiles = [
      (pkgs.writeText "key" "farm-test-1:1/icU6Hlts+rG2LxnM8NoIMcrLWAzdCgJEOLjewE8DxGQKUPC9+LF07Ci6sEjhQP2G50TfF9TkQFBwwVRW5FXw==")
    ];
    readProxy.enable = true;
    s3 = {
      endpoint = "${listenHost}:9000";
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
      S3_ENDPOINT_URL = "http://${listenHost}:9000";
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
}
