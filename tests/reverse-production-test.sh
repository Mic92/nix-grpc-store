#!/bin/sh
set -eu
bin=$1
dir=$(mktemp -d "${TMPDIR:-/tmp}/reverse-production.XXXXXX")
dir=$(cd "$dir" && pwd -P)
daemon_state="$dir/daemon-state"
# Older Nix on Darwin ignores NIX_STATE_DIR for the daemon socket, but does
# honor the rooted store's state directory. Linux uses NIX_STATE_DIR here.
if [ "$(uname -s)" = Darwin ]; then
  daemon_state="$dir/worker-store/nix/var/nix"
fi
daemon_pid=
daemon2_pid=
pid=
garage_pid=
cache_pid=
pg_pid=
trap 'for child in "$daemon_pid" "$daemon2_pid" "$pid" "$cache_pid" "$garage_pid"; do if [ -n "$child" ]; then kill "$child" 2>/dev/null || :; wait "$child" 2>/dev/null || :; fi; done; if [ -n "$pg_pid" ]; then "$PG_BIN/pg_ctl" -D "$dir/postgres" -m immediate stop >/dev/null 2>&1 || :; fi; rm -rf "$dir"' EXIT
nix key generate-secret --key-name reverse-production >"$dir/signing.key"
nix key convert-secret-to-public <"$dir/signing.key" >"$dir/signing.pub"
# Keep the gateway process untrusted at the separate daemon when it is not
# root; the write-role capability also independently enforces check_sigs.
NIX_STATE_DIR="$daemon_state" NIX_CONFIG="trusted-public-keys = $(cat "$dir/signing.pub")
trusted-users = root
sandbox = false" nix-daemon --store "local?root=$dir/worker-store" >"$dir/daemon.log" 2>&1 &
daemon_pid=$!
socket="$daemon_state/daemon-socket/socket"
printf %s "$socket" >"$dir/daemon-socket-path"
ready=0
for attempt in $(seq 1 100); do
  if [ -S "$socket" ]; then ready=1; break; fi
  kill -0 "$daemon_pid" 2>/dev/null || { cat "$dir/daemon.log"; exit 1; }
  sleep 0.05
done
[ "$ready" -eq 1 ] || { cat "$dir/daemon.log"; exit 1; }
if [ "$(uname -s)" = Linux ]; then
  NIX_STATE_DIR="$dir/daemon2-state" NIX_CONFIG="trusted-public-keys = $(cat "$dir/signing.pub")
trusted-users = root
sandbox = false" nix-daemon --store "local?root=$dir/worker2-store" >"$dir/daemon2.log" 2>&1 &
  daemon2_pid=$!
  socket2="$dir/daemon2-state/daemon-socket/socket"
  printf %s "$socket2" >"$dir/daemon2-socket-path"
  ready=0
  for attempt in $(seq 1 100); do
    if [ -S "$socket2" ]; then ready=1; break; fi
    kill -0 "$daemon2_pid" 2>/dev/null || { cat "$dir/daemon2.log"; exit 1; }
    sleep 0.05
  done
  [ "$ready" -eq 1 ] || { cat "$dir/daemon2.log"; exit 1; }
  for name in one two; do
    nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-concurrent-'"$name"'"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-'"$name"' > $out" ]; }' >"$dir/$name.drv"
    nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/$name.drv")" >"$dir/$name-sign.log" 2>&1
  done
  # Only shell builtins: sandboxed builders need not have /bin/sleep or PATH.
  # The loop is stopped by revocation; the build RPC has a 25s deadline.
  nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-cancel-build"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-cancel-running >&2; while :; do :; done" ]; }' >"$dir/cancel.drv"
  nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/cancel.drv")" >"$dir/cancel-sign.log" 2>&1
  # Shell builtins only: long enough to disconnect after the first marker,
  # bounded so a fresh client-driven attempt can finish in the sandbox.
  nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-disconnect-build"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-disconnect-running >&2; i=0; while [ \"$i\" -lt 2000000 ]; do i=$((i + 1)); done; echo reverse-recovered > $out" ]; }' >"$dir/disconnect.drv"
  nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/disconnect.drv")" >"$dir/disconnect-sign.log" 2>&1
  nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-auto-retry"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-auto-running >&2; i=0; while [ \"$i\" -lt 2000000 ]; do i=$((i + 1)); done; echo reverse-auto-recovered > $out" ]; }' >"$dir/auto.drv"
  nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/auto.drv")" >"$dir/auto-sign.log" 2>&1
  nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-publish-fail"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-publish-retried > $out" ]; }' >"$dir/publishfail.drv"
  nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/publishfail.drv")" >"$dir/publishfail-sign.log" 2>&1
  nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "direct-regression"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo direct-worker-ok > $out" ]; }' >"$dir/direct.drv"
  nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/direct.drv")" >"$dir/direct-sign.log" 2>&1
  nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "standby-regression"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo standby-worker-ok > $out" ]; }' >"$dir/standby.drv"
  nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/standby.drv")" >"$dir/standby-sign.log" 2>&1
fi
nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-build"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-built > $out" ]; }' >"$dir/build.drv"
nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/build.drv")" >"$dir/sign.log" 2>&1
if [ "$(uname -s)" = Linux ] && [ "${NIX_GRPC_CACHE_FIXTURE:-0}" = 1 ]; then
  : "${GARAGE_BIN:?set GARAGE_BIN to the Garage bin directory}"
  : "${PG_BIN:?set PG_BIN to the PostgreSQL bin directory}"
  : "${NIKS3_SERVER_BIN:?set NIKS3_SERVER_BIN to the niks3-server bin directory}"
  : "${NIKS3_BIN:?set NIKS3_BIN to the niks3 CLI bin directory}"
  python3 - "$dir" <<'PY'
import pathlib, socket, sys
dest = pathlib.Path(sys.argv[1])
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    dest.joinpath('garage-port').write_text(str(s.getsockname()[1]))
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    dest.joinpath('rpc-port').write_text(str(s.getsockname()[1]))
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    dest.joinpath('cache-port').write_text(str(s.getsockname()[1]))
PY
  s3port=$(cat "$dir/garage-port")
  rpcport=$(cat "$dir/rpc-port")
  cacheport=$(cat "$dir/cache-port")
  mkdir -p "$dir/garage/meta" "$dir/garage/data" "$dir/postgres" "$dir/pgsocket"
  chmod 777 "$dir" "$dir/pgsocket"
  chown nobody:nobody "$dir/postgres" "$dir/pgsocket"
  /bin/setpriv --reuid=nobody --regid=nobody --clear-groups "$PG_BIN/initdb" -D "$dir/postgres" -A trust >"$dir/pg-init.log" 2>&1
  /bin/setpriv --reuid=nobody --regid=nobody --clear-groups "$PG_BIN/postgres" -D "$dir/postgres" -k "$dir/pgsocket" -c listen_addresses= -c unix_socket_permissions=0700 >"$dir/pg.log" 2>&1 &
  pg_pid=$!
  for attempt in $(seq 1 100); do
    if "$PG_BIN/pg_isready" -h "$dir/pgsocket" -U nobody -q; then break; fi
    kill -0 "$pg_pid" 2>/dev/null || { cat "$dir/pg.log"; exit 1; }
    sleep 0.05
  done
  "$PG_BIN/createdb" -h "$dir/pgsocket" -U nobody niks3
  cat >"$dir/garage.toml" <<EOF
metadata_dir = "$dir/garage/meta"
data_dir = "$dir/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:$rpcport"
rpc_secret = "5c1915fa04d0b6739675c61bf5907eb0fe3d9c69850c83820f51b4d25d13868c"
[s3_api]
s3_region = "us-east-1"
api_bind_addr = "127.0.0.1:$s3port"
EOF
  "$GARAGE_BIN/garage" -c "$dir/garage.toml" server >"$dir/garage.log" 2>&1 &
  garage_pid=$!
  for attempt in $(seq 1 100); do
    if "$GARAGE_BIN/garage" -c "$dir/garage.toml" status >"$dir/garage-status.log" 2>&1; then break; fi
    kill -0 "$garage_pid" 2>/dev/null || { cat "$dir/garage.log"; exit 1; }
    sleep 0.05
  done
  node=$($GARAGE_BIN/garage -c "$dir/garage.toml" node id -q | cut -d@ -f1)
  "$GARAGE_BIN/garage" -c "$dir/garage.toml" layout assign -z fixture -c 1G "$node"
  "$GARAGE_BIN/garage" -c "$dir/garage.toml" layout apply --version 1
  s3key=GK0123456789abcdef01234567
  s3secret=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  "$GARAGE_BIN/garage" -c "$dir/garage.toml" key import --yes -n fixture "$s3key" "$s3secret"
  "$GARAGE_BIN/garage" -c "$dir/garage.toml" bucket create fixture
  "$GARAGE_BIN/garage" -c "$dir/garage.toml" bucket allow --read --write --owner fixture --key fixture
  nix key generate-secret --key-name reverse-cache >"$dir/cache-signing.key"
  nix key convert-secret-to-public <"$dir/cache-signing.key" >"$dir/cache-signing.pub"
  printf '%s\n' 'reverse-production-isolated-cache-token-2026' >"$dir/cache-token"
  "$NIKS3_SERVER_BIN/niks3-server" -db "host=$dir/pgsocket user=nobody dbname=niks3 sslmode=disable" \
    -http-addr "127.0.0.1:$cacheport" -s3-endpoint "127.0.0.1:$s3port" -s3-use-ssl=false \
    -s3-bucket fixture -s3-region us-east-1 -s3-bucket-lookup path \
    -s3-access-key "$s3key" -s3-secret-key "$s3secret" -api-token-path "$dir/cache-token" \
    -sign-key-path "$dir/cache-signing.key" -enable-read-proxy >"$dir/cache.log" 2>&1 &
  cache_pid=$!
  for attempt in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:$cacheport/nix-cache-info" >/dev/null 2>&1; then break; fi
    kill -0 "$cache_pid" 2>/dev/null || { cat "$dir/cache.log"; exit 1; }
    sleep 0.05
  done
  printf 'http://127.0.0.1:%s' "$cacheport" >"$dir/cache-url"
  # niks3 runs `nix` against the fixture's rooted worker store. A wrapped
  # niks3 may pin another Nix; a newer one waits forever for the exclusive
  # lock its schema migration needs while this fixture holds the store open.
  niks3_nix=$(sed -n "s@^PATH='\(/nix/store/[^']*\)'\$PATH\$@\1@p" "$NIKS3_BIN/niks3" | head -n 1)
  if [ -n "$niks3_nix" ] && [ -x "$niks3_nix/nix" ] \
    && [ "$("$niks3_nix/nix" --version)" != "$(nix --version)" ]; then
    echo "NIKS3_BIN pins $("$niks3_nix/nix" --version), fixture uses $(nix --version); use a niks3 built against the same Nix" >&2
    exit 1
  fi
  # Preserve the Nix/niks3 CLI runtime inside the private store mount.
  nix-store -qR "$NIKS3_BIN/niks3" "$(command -v nix)" >"$dir/cache-runtime"
  helper=$(cd "$(dirname "$0")" && pwd -P)/reverse-cache-push.py
  cat >"$dir/cache-push" <<EOF
#!/bin/sh
export NIX_REMOTE='local?root=$dir/worker-store'
# niks3 reads NARs from filesystem paths rather than the rooted daemon.
# Only this child sees the fixture's store paths at /nix/store.
exec unshare --mount -- python3 '$helper' \
  '$dir/cache-runtime' '$dir/host-store' '$dir/worker-store/nix/store' '$NIKS3_BIN/niks3' "\$@"
EOF
  chmod +x "$dir/cache-push"
  sed "s@worker-store@worker2-store@g" "$dir/cache-push" >"$dir/cache-push2"
  chmod +x "$dir/cache-push2"
fi
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$dir/server.key" -out "$dir/server.crt" -subj /CN=localhost -addext subjectAltName=DNS:localhost -days 1 >"$dir/cert.log" 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$dir/client.key" -out "$dir/client.csr" -subj /CN=client >"$dir/client-cert.log" 2>&1
openssl x509 -req -in "$dir/client.csr" -CA "$dir/server.crt" -CAkey "$dir/server.key" -CAcreateserial -out "$dir/client.crt" -days 1 >>"$dir/client-cert.log" 2>&1
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$dir/jwt.key" >"$dir/key.log" 2>&1
openssl rsa -in "$dir/jwt.key" -RSAPublicKey_out -outform DER -out "$dir/jwt.der" 2>"$dir/pub.log"
python3 -u -m http.server 0 --bind 127.0.0.1 --directory "$dir" >"$dir/log" 2>&1 &
pid=$!
while ! grep -q 'port ' "$dir/log" 2>/dev/null; do
  kill -0 "$pid" 2>/dev/null || { cat "$dir/log"; exit 1; }
  sleep 0.05
done
port=$(sed -n 's/.*port \([0-9]*\).*/\1/p' "$dir/log")
python3 - "$dir" "$port" <<'PY'
import base64, json, pathlib, subprocess, sys, time
dir, port = pathlib.Path(sys.argv[1]), sys.argv[2]
def b64(b): return base64.urlsafe_b64encode(b).decode().rstrip('=')
data = (dir / 'jwt.der').read_bytes()
def integer(pos):
    assert data[pos] == 2
    size = data[pos+1]
    pos += 2
    if size & 128:
        length = size & 127
        size = int.from_bytes(data[pos:pos+length], 'big')
        pos += length
    return data[pos:pos+size].lstrip(b'\0'), pos+size
assert data[0] == 0x30
pos = 2
if data[1] & 128: pos += data[1] & 127
n, pos = integer(pos)
e, pos = integer(pos)
(dir / 'jwks.json').write_text(json.dumps({'keys':[{'kty':'RSA','alg':'RS256','use':'sig','kid':'worker','n':b64(n),'e':b64(e)}]}))
issuer = f'http://127.0.0.1:{port}'
(dir / 'oidc.json').write_text(json.dumps({'allow_insecure':True, 'providers':{'worker-tunnel':{
    'issuer':issuer, 'jwks_url':issuer+'/jwks.json', 'audience':'grpc://worker-tunnel',
    'rules':[{'bound_claims':{'kubernetes.io.namespace':['staging'],
                              'kubernetes.io.serviceaccount.name':['worker']}, 'scopes':['admin']}]}}}))
claims = {'iss':issuer, 'aud':'grpc://worker-tunnel', 'exp':int(time.time())+300,
          'iat':int(time.time()), 'sub':'system:serviceaccount:staging:worker',
          'kubernetes.io':{'namespace':'staging','pod':{'name':'worker-0','uid':'12345678-1234-1234-1234-123456789abc'},
                           'serviceaccount':{'name':'worker'}}}
message = b64(json.dumps({'alg':'RS256','kid':'worker','typ':'JWT'}).encode())+'.'+b64(json.dumps(claims).encode())
sig = subprocess.run(['openssl','dgst','-sha256','-sign',str(dir/'jwt.key')],input=message.encode(),capture_output=True,check=True).stdout
(dir / 'token-next').write_text(message+'.'+b64(sig)+'\n')
# The worker's first token expires early; the fixture rotates to token-next
# and both reverse sessions must survive the original expiry.
short = dict(claims, exp=int(time.time())+20)
message = b64(json.dumps({'alg':'RS256','kid':'worker','typ':'JWT'}).encode())+'.'+b64(json.dumps(short).encode())
sig = subprocess.run(['openssl','dgst','-sha256','-sign',str(dir/'jwt.key')],input=message.encode(),capture_output=True,check=True).stdout
(dir / 'token').write_text(message+'.'+b64(sig)+'\n')
(dir / 'token-expires').write_text(str(short['exp'])+'\n')
claims['kubernetes.io']['pod'] = {'name':'worker-1','uid':'87654321-4321-4321-4321-cba987654321'}
message = b64(json.dumps({'alg':'RS256','kid':'worker','typ':'JWT'}).encode())+'.'+b64(json.dumps(claims).encode())
sig = subprocess.run(['openssl','dgst','-sha256','-sign',str(dir/'jwt.key')],input=message.encode(),capture_output=True,check=True).stdout
(dir / 'token2').write_text(message+'.'+b64(sig)+'\n')
PY
"$bin" "$dir" "http://127.0.0.1:$port"
