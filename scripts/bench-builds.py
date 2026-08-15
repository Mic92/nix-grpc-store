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
import json
import platform
import shutil
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

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
    builder: str, chain: Path, store: Path, salt: str, length: int, system: str
) -> float:
    if store.exists():
        subprocess.run(["chmod", "-R", "u+w", store], check=True)
        shutil.rmtree(store)
    t0 = time.monotonic()
    proc = subprocess.run(
        [
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
            "--store",
            str(store),
            "--no-link",
            "--max-jobs",
            "0",
            "--substituters",
            "",
            "--option",
            "substitute",
            "false",
            "--builders",
            f"{builder} {system} - 1 1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"nix build via {builder} failed:\n{proc.stderr}")
    return time.monotonic() - t0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--builder", action="append", required=True, help="repeatable")
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

    results: dict[str, list[float]] = {builder: [] for builder in args.builder}
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M%S")
    for i in range(args.warmup + args.runs):
        for builder in args.builder:
            salt = f"{stamp}r{i}t{args.builder.index(builder)}"
            dt = bench_once(builder, chain, store, salt, args.length, args.system)
            print(
                f"  {builder:45s} run {i} {dt:7.2f}s"
                f" ({dt / args.length * 1000:5.0f} ms/drv)",
                flush=True,
            )
            if i >= args.warmup:
                results[builder].append(dt)

    for builder, times in results.items():
        mean = statistics.mean(times)
        stdev = statistics.stdev(times) if len(times) > 1 else 0.0
        print(
            f"{builder:45s} {mean:7.2f}s ±{stdev:5.2f}"
            f"  {mean / args.length * 1000:6.0f} ms/drv"
        )

    history = json.loads(args.output.read_text()) if args.output.exists() else []
    history.append(
        {
            "date": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
            "length": args.length,
            "runs": results,
        }
    )
    args.output.write_text(json.dumps(history, indent=2) + "\n")


if __name__ == "__main__":
    main()
