#!/usr/bin/env bash
# Bump nix/packages/harmonia-gc.json to the latest nix-community/harmonia main.
set -euo pipefail
cd "$(dirname "$0")/.."
rev=${1:-$(git ls-remote https://github.com/nix-community/harmonia refs/heads/main | cut -f1)}
hash=$(nix store prefetch-file --json --unpack "https://github.com/nix-community/harmonia/archive/$rev.tar.gz" | jq -r .hash)
jq -n --arg rev "$rev" --arg hash "$hash" '{rev: $rev, hash: $hash, cargoHash: ""}' >nix/packages/harmonia-gc.json
cargoHash=$(nix build .#harmonia-gc 2>&1 | sed -n 's/.*got: *//p' | head -1)
[[ -n $cargoHash ]] || { echo "could not determine cargoHash" >&2; exit 1; }
jq --arg h "$cargoHash" '.cargoHash = $h' nix/packages/harmonia-gc.json >harmonia-gc.json.tmp
mv harmonia-gc.json.tmp nix/packages/harmonia-gc.json
nix build .#harmonia-gc --no-link
echo "harmonia-gc -> $rev"
