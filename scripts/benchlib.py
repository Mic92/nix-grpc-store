"""Shared harness for the transport benchmarks: interleaved runs and
JSON result history."""

import datetime
import json
import statistics
from collections.abc import Callable
from pathlib import Path


def interleaved_runs(
    targets: list[str],
    bench_once: Callable[[str, int], float],
    runs: int,
    warmup: int,
    describe: Callable[[float], str],
) -> dict[str, list[float]]:
    results: dict[str, list[float]] = {target: [] for target in targets}
    for i in range(warmup + runs):
        for target in targets:
            dt = bench_once(target, i)
            print(f"  {target:45s} run {i} {dt:7.2f}s ({describe(dt)})", flush=True)
            if i >= warmup:
                results[target].append(dt)
    return results


def print_summary(
    results: dict[str, list[float]], describe: Callable[[float], str]
) -> None:
    for target, times in results.items():
        mean = statistics.mean(times)
        stdev = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f"{target:45s} {mean:7.2f}s \u00b1{stdev:5.2f}  {describe(mean)}")


def append_history(output: Path, record: dict) -> None:
    history = json.loads(output.read_text()) if output.exists() else []
    record["date"] = datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")
    history.append(record)
    output.write_text(json.dumps(history, indent=2) + "\n")
