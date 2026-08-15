#!/usr/bin/env python3
"""Compare nix copy transports (e.g. ssh-ng:// vs grpc://) for one closure.

Benchmarks the repo's pinned real-world closure (.#bench-closure) unless
--path is given. Runs are interleaved so link-speed drift hits all
transports equally. Results are appended as JSON for scripts/bench-plot.py.

Example:
  ./scripts/bench-transports.py \\
      --store ssh-ng://root@eve.i --store grpc://eve.thalheim.io:50051
"""

import argparse
import datetime
import json
import shutil
import statistics
import subprocess
import time
from pathlib import Path


def nix(*args: str) -> str:
    return subprocess.run(
        ["nix", *args], check=True, capture_output=True, text=True
    ).stdout


def bench_once(store: str, path: str, dest: Path) -> float:
    if dest.exists():
        subprocess.run(["chmod", "-R", "u+w", dest], check=True)
        shutil.rmtree(dest)
    t0 = time.monotonic()
    nix("copy", "--no-check-sigs", "--from", store, "--to", str(dest), path)
    return time.monotonic() - t0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", action="append", required=True, help="repeatable")
    parser.add_argument("--path", help="store path (default: build .#bench-closure)")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--output", type=Path, default=Path("bench-results.json"))
    args = parser.parse_args()

    path = args.path
    if not path:
        path = nix("build", ".#bench-closure", "--no-link", "--print-out-paths").strip()
        for store in args.store:
            print(f"pushing closure to {store}")
            nix("copy", "--no-check-sigs", "--to", store, path)

    info = nix("path-info", "-r", "-S", "--store", args.store[0], path).splitlines()
    n_paths, n_bytes = len(info), int(info[-1].split()[-1])
    print(f"closure: {n_paths} paths, {n_bytes / 1e6:.0f} MB")

    results: dict[str, list[float]] = {store: [] for store in args.store}
    dest = Path("/tmp/nix-bench-dst")
    for i in range(args.warmup + args.runs):
        for store in args.store:
            dt = bench_once(store, path, dest)
            print(f"  {store:45s} run {i} {dt:7.2f}s", flush=True)
            if i >= args.warmup:
                results[store].append(dt)

    for store, times in results.items():
        mean = statistics.mean(times)
        stdev = statistics.stdev(times) if len(times) > 1 else 0.0
        print(
            f"{store:45s} {mean:7.2f}s ±{stdev:5.2f}  {n_bytes / mean / 1e6:6.1f} MB/s"
        )

    history = json.loads(args.output.read_text()) if args.output.exists() else []
    history.append(
        {
            "date": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
            "path": path,
            "paths": n_paths,
            "bytes": n_bytes,
            "runs": results,
        }
    )
    args.output.write_text(json.dumps(history, indent=2) + "\n")


if __name__ == "__main__":
    main()
