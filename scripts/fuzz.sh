#!/usr/bin/env bash
# Build the libFuzzer targets and run each one against a persistent corpus
# under $XDG_CACHE_HOME/nix-grpc-store/fuzz/<target>.
#
#   scripts/fuzz.sh                 # every target, 60 s each
#   scripts/fuzz.sh -t 600 wire     # one target, 10 min
#   scripts/fuzz.sh -j 8            # 8 libFuzzer workers per target
set -euo pipefail

seconds=60
jobs=1
while getopts "t:j:" opt; do
  case "$opt" in
    t) seconds=$OPTARG ;;
    j) jobs=$OPTARG ;;
    *) echo "usage: $0 [-t seconds] [-j jobs] [target...]" >&2; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
bin=$(nix build --no-link --print-out-paths "$root#fuzzers")/bin
corpusRoot=${XDG_CACHE_HOME:-$HOME/.cache}/nix-grpc-store/fuzz

if [[ $# -eq 0 ]]; then
  mapfile -t targets < <(cd "$bin" && printf '%s\n' fuzz-* | sed 's/^fuzz-//')
else
  targets=("$@")
fi

status=0
for target in "${targets[@]}"; do
  corpus=$corpusRoot/$target
  artifacts=$corpusRoot/$target-artifacts/
  mkdir -p "$corpus" "$artifacts"
  echo "==> fuzz-$target (${seconds}s, corpus: $corpus)"
  if ! "$bin/fuzz-$target" \
      -max_total_time="$seconds" \
      -jobs="$jobs" -workers="$jobs" \
      -rss_limit_mb=4096 -malloc_limit_mb=1000000 \
      -max_len=65536 \
      -artifact_prefix="$artifacts" \
      "$corpus"; then
    echo "!!  fuzz-$target failed, reproducers in $artifacts" >&2
    status=1
  fi
done
exit "$status"
