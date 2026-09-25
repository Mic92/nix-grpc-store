{
  lib,
  abseil-cpp,
  stdenv,
  meson,
  ninja,
  pkg-config,
  protobuf,
  grpc,
  openssl,
  prometheus-cpp,
  zstd,
  curl,
  nlohmann_json,
  jwt-cpp,
  jemalloc,
  python3,
  mold,
  # Nix component libraries. When building the client plugin these must be
  # ABI-compatible with the `nix` binary that will dlopen() the .so; the NixOS
  # client module passes `config.nix.package.libs.*` here for that reason.
  nix-store,
  nix-util,
}:

stdenv.mkDerivation {
  pname = "nix-grpc-store";
  version = lib.fileContents ../../.version;
  src = lib.fileset.toSource {
    root = ../..;
    fileset = lib.fileset.unions [
      ../../.clang-tidy
      ../../.version
      ../../meson.build
      ../../meson.options
      ../../pch
      ../../proto
      ../../src
      ../../fuzz
      ../../tests/farm-mock.py
      ../../tests/farm-client-test.cc
      ../../tests/farm-client-test.sh
      ../../tests/oidc-test.cc
      ../../tests/oidc-test.sh
      ../../tests/xfcc-test.cc
      ../../tests/log-spill-test.cc
      ../../tests/scheduler-test.cc
      ../../tests/dispatcher-bench.cc
      ../../tests/scheduler-grpc-bench.cc
    ];
  };

  doCheck = true;
  nativeCheckInputs = [ python3 ];
  # The tests bind loopback mock servers.
  __darwinAllowLocalNetworking = true;

  nativeBuildInputs = [
    meson
    ninja
    pkg-config
    protobuf
    grpc
  ] ++ lib.optional stdenv.hostPlatform.isLinux mold;

  buildInputs = [
    abseil-cpp
    grpc
    openssl
    protobuf
    prometheus-cpp
    zstd
    nix-store
    nix-util
    curl
    nlohmann_json
    jwt-cpp
    jemalloc
  ];

  # Frame pointers for perf in the VM test. DWARF goes to the debug output,
  # unstripped it dragged every -dev input into the runtime closure.
  env.NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer";
  separateDebugInfo = true;

  meta = {
    description = "gRPC transport for the Nix remote store protocol";
    mainProgram = "nix-grpc-daemon";
    license = lib.licenses.mit;
  };
}
