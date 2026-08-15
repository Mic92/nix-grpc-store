# ASan/UBSan smoke test inside the build sandbox: chroot stores plus a nix
# daemon on a unix socket. The client preloads the ASan runtime so the
# uninstrumented nix binary can dlopen() the sanitized plugin.
{
  pkgs,
  package,
  nix,
}:
pkgs.stdenv.mkDerivation {
  name = "nix-grpc-store-sanitize-smoke";
  nativeBuildInputs = [
    nix
    package
  ];
  buildCommand = ''
    export HOME=$TMPDIR
    export NIX_DAEMON_SOCKET_PATH=$TMPDIR/daemon.sock
    export NIX_CONFIG="experimental-features = nix-command"
    export ASAN_OPTIONS=abort_on_error=1:detect_leaks=0
    export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1

    trap 'echo === nixd.log ===; cat nixd.log; echo === daemon.log ===; cat daemon.log' EXIT

    nix daemon --store "local?root=$TMPDIR/remote" 2> nixd.log &
    for _ in $(seq 50); do
      [ -e "$NIX_DAEMON_SOCKET_PATH" ] && break
      sleep 0.2
    done

    # The native RPCs read NARs from the filesystem, so they need the
    # chroot store's real root.
    nix-grpc-daemon --listen 127.0.0.1:50051 \
      --proxy-socket "$NIX_DAEMON_SOCKET_PATH" \
      --proxy-store "unix://$NIX_DAEMON_SOCKET_PATH?root=$TMPDIR/remote" \
      --log-level debug 2> daemon.log &
    daemon_pid=$!
    store="grpc://127.0.0.1:50051?insecure=1"

    plugin=$(echo ${package}/lib/nix/nix-grpc-store-versions/*/nix-grpc-store.so)
    asan_rt=$($CC -print-file-name=libasan.so)
    client() {
      LD_PRELOAD=$asan_rt nix --option plugin-files "$plugin" "$@"
    }

    for _ in $(seq 50); do
      client store info --store "$store" > /dev/null 2>&1 && break
      sleep 0.2
    done
    client store info --json --store "$store"

    # Exercise the pipelined NAR stream, bulk import and batched queries.
    mkdir small
    for i in $(seq 50); do
      head -c 4096 /dev/urandom > small/f$i
    done
    paths=$(cd small && nix-store --store "local?root=$TMPDIR/src" --add f*)
    client copy --no-check-sigs --from "local?root=$TMPDIR/src" --to "$store" $paths
    client copy --no-check-sigs --from "$store" --to "local?root=$TMPDIR/dst" $paths
    for p in $paths; do
      cmp "$TMPDIR/src/$p" "$TMPDIR/dst/$p"
    done

    kill "$daemon_pid"
    wait "$daemon_pid" || [ $? = 143 ] # SIGTERM is the expected exit
    if grep -E 'AddressSanitizer|runtime error' daemon.log; then
      echo "sanitizer reports in daemon log" >&2
      exit 1
    fi
    touch $out
  '';
}
