# Farm worker image. The chart runs nix-daemon, nix-grpc-daemon and the
# harmonia-gc loop from it as separate containers.
{
  lib,
  dockerTools,
  runCommand,
  writeTextDir,
  buildEnv,
  busybox,
  busybox-sandbox-shell,
  cacert,
  iana-etc,
  nix,
  nix-grpc-daemon,
  niks3,
  harmonia-gc,
  imageName ? "nix-grpc-farm",
  tag ? nix-grpc-daemon.version,
}:
let
  # auto-allocate-uids: no nixbld users to maintain in /etc/passwd.
  passwd = writeTextDir "etc/passwd" ''
    root:x:0:0:root:/root:/bin/sh
    nix-grpc-daemon:x:990:990::/var/empty:/bin/false
    nobody:x:65534:65534:nobody:/var/empty:/bin/false
  '';
  group = writeTextDir "etc/group" ''
    root:x:0:
    nixbld:x:30000:
    nix-grpc-daemon:x:990:
    nogroup:x:65534:
  '';
  # The chart mounts nix.conf.d/farm.conf. A missing include is ignored.
  nixConf = writeTextDir "etc/nix/nix.conf" ''
    build-users-group = nixbld
    auto-allocate-uids = true
    experimental-features = nix-command auto-allocate-uids cgroups
    sandbox = true
    sandbox-fallback = false
    sandbox-paths = /bin/sh=${busybox-sandbox-shell}/bin/busybox
    keep-build-log = true
    narinfo-cache-negative-ttl = 0
    trusted-users = root nix-grpc-daemon
    allowed-users = *
    !include /etc/nix/nix.conf.d/farm.conf
  '';

  path = buildEnv {
    name = "nix-grpc-farm-path";
    paths = [
      nix
      nix-grpc-daemon
      niks3
      harmonia-gc
      busybox
    ];
    pathsToLink = [ "/bin" ];
  };

  # Keeps harmonia-gc from deleting the tools once /nix lives on the pod volume.
  gcRoot = runCommand "nix-grpc-farm-gcroot" { } ''
    mkdir -p $out/nix/var/nix/gcroots
    ln -s ${path} $out/nix/var/nix/gcroots/image
  '';
in
dockerTools.buildLayeredImage {
  name = imageName;
  inherit tag;
  # docker-multiarch decompresses it again right away.
  compressor = "none";
  contents = [
    path
    passwd
    group
    nixConf
    cacert
    iana-etc
    gcRoot
    dockerTools.usrBinEnv
  ];
  includeNixDB = true;
  extraCommands = ''
    mkdir -p tmp root var/empty etc/nix/nix.conf.d
    chmod 1777 tmp
  '';
  config = {
    Env = [
      "PATH=/bin"
      "NIX_SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
      "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
    ];
    Labels = {
      "org.opencontainers.image.source" = "https://github.com/Mic92/nix-grpc-store";
      "org.opencontainers.image.description" = "nix-grpc-store build farm worker";
    };
  };
  passthru = { inherit path nixConf; };
}
