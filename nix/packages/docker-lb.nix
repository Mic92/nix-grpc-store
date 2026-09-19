# OCI image for the farm load balancer: envoy with a chart-mounted config.
{
  dockerTools,
  envoy-bin,
  cacert,
  busybox,
  imageName ? "nix-grpc-farm-lb",
  tag,
}:
dockerTools.buildLayeredImage {
  name = imageName;
  inherit tag;
  compressor = "none";
  contents = [
    envoy-bin
    cacert
    busybox
    dockerTools.usrBinEnv
  ];
  extraCommands = ''
    mkdir -p tmp etc/envoy
    chmod 1777 tmp
  '';
  config = {
    Entrypoint = [
      "/bin/envoy"
      "-c"
      "/etc/envoy/envoy.json"
    ];
    Env = [ "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt" ];
    ExposedPorts."50051/tcp" = { };
    Labels = {
      "org.opencontainers.image.source" = "https://github.com/Mic92/nix-grpc-store";
      "org.opencontainers.image.description" = "nix-grpc-store build farm load balancer (envoy)";
    };
  };
}
