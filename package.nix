{
  lib,
  stdenv,
  meson,
  ninja,
  pkg-config,
  protobuf,
  grpc,
  zstd,
  # Nix component libraries. When building the client plugin these must be
  # ABI-compatible with the `nix` binary that will dlopen() the .so; the NixOS
  # client module passes `config.nix.package.libs.*` here for that reason.
  nix-store,
  nix-util,
}:

stdenv.mkDerivation {
  pname = "nix-grpc-store";
  version = "0.1.0";
  src = ./.;

  nativeBuildInputs = [
    meson
    ninja
    pkg-config
    protobuf
    grpc
  ];

  buildInputs = [
    grpc
    protobuf
    zstd
    nix-store
    nix-util
  ];

  # Frame pointers + symbols so `perf` in the VM test can attribute samples
  # inside the plugin and daemon.
  env.NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer -g";
  dontStrip = true;

  meta = {
    description = "gRPC transport for the Nix remote store protocol";
    mainProgram = "nix-grpc-daemon";
    license = lib.licenses.mit;
  };
}
