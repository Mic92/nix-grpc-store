# Farm images. `worker` runs nix-daemon, nix-grpc-daemon and the harmonia-gc
# loop as separate containers. `client` is a CI job image whose nix has the
# grpc:// plugin loaded, see docs/farm.md.
{
  lib,
  dockerTools,
  runCommand,
  writeTextDir,
  buildEnv,
  busybox,
  busybox-sandbox-shell,
  bashInteractive,
  coreutils,
  cacert,
  iana-etc,
  gitMinimal,
  openssh,
  jq,
  nix,
  nix-grpc-daemon,
  nix-eval-jobs ? null,
  nix-fast-build ? null,
  niks3,
  harmonia-gc,
  variant ? "worker",
  imageName ? { worker = "nix-grpc-farm"; client = "nix-grpc-farm-client"; }.${variant},
  tag ? nix-grpc-daemon.version,
}:
let
  client = variant == "client";
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
  nixConf = writeTextDir "etc/nix/nix.conf" (
    if client then
      ''
        # Single-user nix as root. Builds run on the farm, so no sandbox
        # and no build users. NIX_CONFIG or nix.conf.d/farm.conf point at it.
        build-users-group =
        sandbox = false
        experimental-features = nix-command flakes
        plugin-files = ${nix-grpc-daemon}/lib/nix/plugins
        accept-flake-config = false
        !include /etc/nix/nix.conf.d/farm.conf
      ''
    else
      ''
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
      ''
  );

  path = buildEnv {
    name = "${imageName}-path";
    paths =
      if client then
        [
          nix
          nix-eval-jobs
          nix-fast-build
          niks3
          gitMinimal
          openssh
          jq
          # CI scripts expect bash and GNU coreutils. busybox fills the rest.
          bashInteractive
          coreutils
          busybox
        ]
      else
        [
          nix
          nix-grpc-daemon
          niks3
          harmonia-gc
          busybox
        ];
    pathsToLink = [ "/bin" ];
    ignoreCollisions = client; # coreutils and bash before busybox
  };

  # Keeps gc from deleting the tools once /nix lives on a volume.
  gcRoot = runCommand "${imageName}-gcroot" { } ''
    mkdir -p $out/nix/var/nix/gcroots
    ln -s ${path} $out/nix/var/nix/gcroots/image
    ${lib.optionalString client "ln -s ${nix-grpc-daemon} $out/nix/var/nix/gcroots/plugin"}
  '';
in
assert client -> nix-eval-jobs != null && nix-fast-build != null;
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
    ]
    ++ lib.optionals client [
      "HOME=/root"
      "USER=root"
      "GIT_SSL_CAINFO=${cacert}/etc/ssl/certs/ca-bundle.crt"
    ];
    Labels = {
      "org.opencontainers.image.source" = "https://github.com/Mic92/nix-grpc-store";
      "org.opencontainers.image.description" =
        if client then "CI job image with nix and the grpc:// store plugin" else "nix-grpc-store build farm worker";
    };
  }
  // lib.optionalAttrs client { Cmd = [ "/bin/bash" ]; };
  passthru = { inherit path nixConf; };
}
