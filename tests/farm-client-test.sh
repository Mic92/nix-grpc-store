#!/bin/sh
# meson test wrapper: start the mock, run the C++ driver against it.
set -eu
bin=$1
mock=$2
dir=$(mktemp -d)
trap 'kill "$pid" 2>/dev/null; rm -rf "$dir"' EXIT
python3 "$mock" serve >"$dir/port" &
pid=$!
while [ ! -s "$dir/port" ]; do
  kill -0 "$pid" 2>/dev/null || exit 1
  sleep 0.05
done
# nix::startProcess allocates a logger in the forked child before exec.
export ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}detect_leaks=0"
"$bin" "http://127.0.0.1:$(cat "$dir/port")" "$mock"
