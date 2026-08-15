#!/usr/bin/env python3
"""Compare remote-build transports (e.g. ssh-ng:// vs grpc://) for RTT overhead.

Builds a serial chain of trivial derivations through the build hook, so
wall time is dominated by per-derivation protocol round trips. Every run
uses a fresh salt, so nothing is ever cached; runs are interleaved so
link drift hits all transports equally. Results are appended as JSON for
scripts/bench-plot.py.

Example:
  ./scripts/bench-builds.py \\
      --builder ssh-ng://root@eve.i --builder grpc://eve.thalheim.io:50051
"""

import argparse
import datetime
import platform
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import benchlib

CHAIN_NIX = """
{ salt, length, system }:
let
  mk = name: deps: derivation {
    inherit name system deps;
    builder = "/bin/sh";
    args = [ "-c" "echo ${name} ${salt} $deps > $out" ];
  };
  go = n: prev:
    if n == 0 then prev
    else go (n - 1) [ (mk "bench-link-${toString n}-${salt}" prev) ];
in builtins.head (go length [ ])
"""


def bench_once(
    target: str, chain: Path, store: Path, salt: str, length: int, system: str
) -> float:
    mode, url = target.split(":", 1)
    if store.exists():
        subprocess.run(["chmod", "-R", "u+w", store], check=True)
        shutil.rmtree(store)
    cmd = [
        "nix",
        "build",
        "-f",
        str(chain),
        "--argstr",
        "salt",
        salt,
        "--arg",
        "length",
        str(length),
        "--argstr",
        "system",
        system,
        "--no-link",
        "--substituters",
        "",
        "--option",
        "substitute",
        "false",
    ]
    if mode == "hook":
        cmd += [
            "--store",
            str(store),
            "--max-jobs",
            "0",
            "--builders",
            f"{url} {system} - 1 1",
        ]
    else:
        cmd += ["--store", url]
    t0 = time.monotonic()
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"nix build via {target} failed:\n{proc.stderr}")
    return time.monotonic() - t0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--builder", action="append", default=[], help="build-hook builder, repeatable"
    )
    parser.add_argument(
        "--direct", action="append", default=[], help="remote store url, repeatable"
    )
    parser.add_argument("--length", type=int, default=15, help="chain length")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument(
        "--system", default=f"{platform.machine()}-linux", help="builder system"
    )
    parser.add_argument("--output", type=Path, default=Path("bench-build-results.json"))
    args = parser.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="nix-bench-build-"))
    chain = tmp / "chain.nix"
    chain.write_text(CHAIN_NIX)
    store = tmp / "store"

    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M%S")

    def describe(dt: float) -> str:
        return f"{dt / args.length * 1000:5.0f} ms/drv"

    targets = [f"hook:{b}" for b in args.builder] + [f"direct:{s}" for s in args.direct]
    if not targets:
        parser.error("need at least one --builder or --direct")

    def once(target: str, i: int) -> float:
        salt = f"{stamp}r{i}t{targets.index(target)}"
        return bench_once(target, chain, store, salt, args.length, args.system)

    results = benchlib.interleaved_runs(targets, once, args.runs, args.warmup, describe)
    benchlib.print_summary(results, describe)
    benchlib.append_history(args.output, {"length": args.length, "runs": results})


if __name__ == "__main__":
    main()
