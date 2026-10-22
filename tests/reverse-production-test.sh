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
pid=
trap 'if [ -n "$daemon_pid" ]; then kill "$daemon_pid" 2>/dev/null || :; wait "$daemon_pid" 2>/dev/null || :; fi; if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi; rm -rf "$dir"' EXIT
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
nix-instantiate --store "local?root=$dir/source-store" --expr 'builtins.derivation { name = "reverse-build"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo reverse-built > $out" ]; }' >"$dir/build.drv"
nix store sign --store "local?root=$dir/source-store" --key-file "$dir/signing.key" --derivation "$(cat "$dir/build.drv")" >"$dir/sign.log" 2>&1
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
(dir / 'token').write_text(message+'.'+b64(sig)+'\n')
PY
"$bin" "$dir" "http://127.0.0.1:$port"
