# niks3 + garage + postgres on one NixOS node, shared by farm.nix and k3s.nix.
{
  pkgs,
  niks3,
  listenHost, # what S3/niks3 bind and advertise
  apiToken,
}:
let
  # Garage wants "GK" plus 24 hex digits as the key id and 64 hex as the secret.
  s3Key = "GK0123456789abcdef01234567";
  s3Secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
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
      secretKeyFile = pkgs.writeText "sk" s3Secret;
    };
  };
  services.garage = {
    enable = true;
    package = pkgs.garage;
    settings = {
      replication_mode = "none";
      rpc_bind_addr = "127.0.0.1:3901";
      rpc_secret = "5c1915fa04d0b6739675c61bf5907eb0fe3d9c69850c83820f51b4d25d13868c";
      s3_api = {
        s3_region = "us-east-1";
        api_bind_addr = "0.0.0.0:9000";
      };
    };
  };
  # Garage refuses to start with less than 1 GiB free.
  virtualisation.diskSize = pkgs.lib.mkDefault 2048;
  systemd.services.garage-setup = {
    requires = [ "garage.service" ];
    after = [ "garage.service" ];
    before = [ "niks3.service" ];
    requiredBy = [ "niks3.service" ];
    path = [ pkgs.garage ];
    # Each step tolerates a rerun, so a restart of the unit stays harmless.
    script = ''
      for i in $(seq 60); do garage status && break; sleep 1; done
      garage layout assign -z test -c 1G "$(garage node id -q | cut -d@ -f1)" || true
      garage layout apply --version 1 || true
      garage key import --yes -n farm ${s3Key} ${s3Secret} || true
      garage bucket create farm || true
      garage bucket allow --read --write --owner farm --key farm
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
