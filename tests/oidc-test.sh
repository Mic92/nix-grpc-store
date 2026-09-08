#!/bin/sh
set -eu
bin=$1
dir=$(mktemp -d)
mkdir -p "$dir/.well-known"
trap 'kill "$pid" 2>/dev/null; rm -rf "$dir"' EXIT
python3 -u -m http.server 0 --bind 127.0.0.1 --directory "$dir" >"$dir/log" 2>&1 &
pid=$!
while ! grep -q 'port ' "$dir/log" 2>/dev/null; do sleep 0.05; done
port=$(sed -n 's/.*port \([0-9]*\).*/\1/p' "$dir/log")
"$bin" "$dir" "http://127.0.0.1:$port"
