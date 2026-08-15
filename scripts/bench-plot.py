#!/usr/bin/env python3
"""Plot bench-transports.py or bench-builds.py results.

Usage:
  nix shell nixpkgs#python3Packages.matplotlib -c \\
      ./scripts/bench-plot.py bench-results.json -o bench.png
"""

import argparse
import json
import statistics
from pathlib import Path

import matplotlib  # type: ignore[import-not-found]

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore[import-not-found]

COLORS = ["#4c72b0", "#55a868", "#c44e52", "#8172b3", "#ccb974"]


def short_label(store: str) -> str:
    return store.split("://")[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("-o", "--output", type=Path, default=Path("bench.png"))
    args = parser.parse_args()

    history = json.loads(args.results.read_text())
    stores = sorted({store for record in history for store in record["runs"]})
    width = 0.8 / len(stores)

    fig, axis = plt.subplots(figsize=(max(6, 1.5 + 2.5 * len(history)), 4.5), dpi=150)
    axis.set_xlim(-0.6, len(history) - 1 + 0.8 + 0.6)
    axis.set_axisbelow(True)
    axis.grid(axis="y", color="#dddddd")
    for spine in ("top", "right"):
        axis.spines[spine].set_visible(False)

    for si, store in enumerate(stores):
        xs = [ri + si * width for ri in range(len(history))]
        means = [statistics.mean(r["runs"][store]) for r in history]
        errs = [statistics.stdev(r["runs"][store]) for r in history]
        bars = axis.bar(
            xs,
            means,
            width=width * 0.9,
            yerr=errs,
            capsize=3,
            label=store,
            color=COLORS[si % len(COLORS)],
        )
        axis.bar_label(bars, fmt="%.1fs", padding=3, fontsize=8)

    # Headroom for the bar labels.
    tallest = max(
        statistics.mean(times) for r in history for times in r["runs"].values()
    )
    axis.set_ylim(0, tallest * 1.35)

    builds = "length" in history[0]
    labels = []
    for record in history:
        means = [statistics.mean(record["runs"][store]) for store in stores]
        what = (
            f"{record['length']} derivations"
            if builds
            else f"{record['paths']} paths, {record['bytes'] / 1e6:.0f} MB"
        )
        labels.append(
            f"{record['date'][:10]}, {what}\n"
            f"fastest is {max(means) / min(means):.1f}x faster"
        )
    axis.set_xticks(
        [ri + width * (len(stores) - 1) / 2 for ri in range(len(history))],
        labels,
        fontsize=8,
    )
    verb = "remote build" if builds else "nix copy"
    axis.set_ylabel(f"{verb} wall time (s)")
    axis.set_title(f"{verb} transport comparison", fontsize=11)
    axis.legend(fontsize=8, frameon=False, loc="upper right")
    fig.tight_layout()
    fig.savefig(args.output)


if __name__ == "__main__":
    main()
