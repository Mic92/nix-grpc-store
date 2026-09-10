#!/bin/sh
# meson test wrapper: start the mock, run the C++ driver against it.
set -eu
bin=$1
mock=$2
dir=$(mktemp -d)
trap 'kill "$pid" 2>/dev/null; rm -rf "$dir"' EXIT
python3 "$mock" serve >"$dir/port" &
pid=$!
while [ ! -s "$dir/port" ]; do sleep 0.05; done
"$bin" "http://127.0.0.1:$(cat "$dir/port")" "$mock"
