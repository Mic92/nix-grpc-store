#!/usr/bin/env bash
# Build the libFuzzer targets and run each one against a persistent corpus
# under $XDG_CACHE_HOME/nix-grpc-store/fuzz/<target>.
#
#   scripts/fuzz.sh                 # every target, 60 s each
#   scripts/fuzz.sh -t 600 wire     # one target, 10 min
#   scripts/fuzz.sh -j 8            # 8 libFuzzer workers per target
#   scripts/fuzz.sh -c              # replay corpus, print line coverage of src/
set -euo pipefail

seconds=60
jobs=1
coverage=0
while getopts "t:j:c" opt; do
  case "$opt" in
    t) seconds=$OPTARG ;;
    j) jobs=$OPTARG ;;
    c) coverage=1 ;;
    *) echo "usage: $0 [-t seconds] [-j jobs] [-c] [target...]" >&2; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
attr=fuzzers
[[ $coverage -eq 1 ]] && attr=fuzzers-coverage
bin=$(nix build --no-link --print-out-paths "$root#$attr")/bin
corpusRoot=${XDG_CACHE_HOME:-$HOME/.cache}/nix-grpc-store/fuzz

if [[ $# -eq 0 ]]; then
  mapfile -t targets < <(cd "$bin" && printf '%s\n' fuzz-* | sed 's/^fuzz-//')
else
  targets=("$@")
fi

if [[ $coverage -eq 1 ]]; then
  profdir=$(mktemp -d)
  trap 'rm -rf "$profdir"' EXIT
  objects=()
  for target in "${targets[@]}"; do
    LLVM_PROFILE_FILE="$profdir/$target.profraw" "$bin/fuzz-$target" -runs=0 "$corpusRoot/$target" >/dev/null 2>&1
    objects+=(-object "$bin/fuzz-$target")
  done
  llvm=(nix shell --inputs-from "$root" nixpkgs#llvmPackages.llvm -c)
  "${llvm[@]}" llvm-profdata merge -o "$profdir/merged.profdata" "$profdir"/*.profraw
  "${llvm[@]}" llvm-cov report "${objects[@]:1}" -instr-profile="$profdir/merged.profdata" \
    -ignore-filename-regex='(nix_remote\..*pb|/nix/store/|fuzz/)'
  exit 0
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
