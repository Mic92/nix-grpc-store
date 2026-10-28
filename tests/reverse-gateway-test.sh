#!/bin/sh
set -eu
bin=$1
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$dir/server.key" -out "$dir/server.crt" -subj /CN=localhost -addext subjectAltName=DNS:localhost -days 1 >"$dir/cert.log" 2>&1
"$bin" "$dir"
