# Many short-lived TLS clients: catches races between gRPC's asynchronous
# shutdown and OpenSSL's atexit cleanup (seen as SIGSEGV on Darwin).
{
  pkgs,
  package,
  nix,
  iterations ? 400,
}:
pkgs.stdenv.mkDerivation {
  name = "nix-grpc-store-exit-stress";
  nativeBuildInputs = [
    nix
    package
    pkgs.openssl
  ];
  __darwinAllowLocalNetworking = true;
  buildCommand = ''
    export HOME=$TMPDIR
    export NIX_DAEMON_SOCKET_PATH=$TMPDIR/daemon.sock
    export NIX_CONFIG="experimental-features = nix-command"

    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=stress-ca \
      -keyout ca.key -out ca.pem 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
      -keyout server.key -out server.csr 2>/dev/null
    openssl x509 -req -in server.csr -days 1 -CA ca.pem -CAkey ca.key \
      -extfile <(printf 'subjectAltName=IP:127.0.0.1') -out server.pem 2>/dev/null

    # Parallel builds on one Darwin host share the network namespace.
    port=$((20000 + RANDOM % 20000))
    trap 'rc=$?; kill %1 %2 2>/dev/null || true; [ $rc = 0 ] || cat nixd.log daemon.log' EXIT
    nix daemon --store "local?root=$TMPDIR/remote" 2> nixd.log &
    for _ in $(seq 50); do
      [ -e "$NIX_DAEMON_SOCKET_PATH" ] && break
      sleep 0.2
    done
    nix-grpc-daemon --listen 127.0.0.1:$port \
      --proxy-socket "$NIX_DAEMON_SOCKET_PATH" \
      --proxy-store "unix://$NIX_DAEMON_SOCKET_PATH?root=$TMPDIR/remote" \
      --tls-cert server.pem --tls-key server.key 2> daemon.log &

    plugin=$(echo ${package}/lib/nix/nix-grpc-store-versions/*/nix-grpc-store.*)
    store="grpc://127.0.0.1:$port?ca-cert=$PWD/ca.pem"
    client() { nix --option plugin-files "$plugin" store info --store "$store" "$@"; }

    for _ in $(seq 50); do
      client > /dev/null 2>&1 && break
      sleep 0.2
    done
    client

    crashes=0
    for i in $(seq ${toString iterations}); do
      client > /dev/null 2> client.log || {
        rc=$?
        if [ "$rc" -gt 128 ]; then
          echo "iteration $i: killed by signal $((rc - 128))"
          cat client.log
          crashes=$((crashes + 1))
        fi
      }
    done
    echo "crashes: $crashes / ${toString iterations}"
    [ "$crashes" = 0 ]
    touch $out
  '';
}
