#!/usr/bin/env python3
"""Render the cross-platform BerlinMOD timing matrix as a grouped bar SVG.

Reads the timing matrix from the literal definitions below (one source of
truth) and emits two SVGs:

- cross_platform_standard.svg     — the standard-index matrix (Q1-Q10).
- cross_platform_th3index.svg     — the th3index prefilter matrix where
                                    measurements exist.

Y axis is logarithmic so Q5 / Q10 / Q11 spikes do not collapse the small
queries.  Each query gets three coloured bars side by side (one per
platform).  Two sentinels are recognised in the timing dicts:

- ``None``: query shape is not defined on that platform.  Rendered as a
  flat marker at the y-axis floor labelled ``n/a``.
- ``">cap"``: query exceeded the per-query time budget (the cap is
  ``max(20 × slowest other platform, 30 min)``).  Rendered as a hatched
  bar at the 30-min ceiling labelled ``>cap``.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


CAP_SECONDS = 1800.0  # 30-min wall-clock ceiling rendered for ">cap" entries


@dataclass(frozen=True)
class Series:
    label: str
    color: str
    # query name -> seconds, None (n/a), or ">cap" (exceeded time budget)
    timings: dict[str, float | None | str]


# Standard matrix — all three platforms with their default index.
# MobilityDB: R-tree on `trip` + R-tree on `trajectory` (GiST opclass)
# MobilityDuck: no spatial index (DuckDB columnar full scan)
# MobilitySpark: local[4], no spatial index
STANDARD: list[Series] = [
    Series("MobilityDB R-tree", "#1f77b4", {
        "Q1": 0.78, "Q2": 0.15, "Q3": 5.70, "Q4": 15.19, "Q5": 80.61,
        "Q6": 4.23, "Q7": 9.24, "Q8": 1.18, "Q9": 9.81, "Q10": 6.46,
        "Q11": 2.31, "Q12": 2.37, "Q13": 4.55, "Q14": 0.44, "Q15": 4.13,
        "Q16": 16.35, "Q17": 9.74,
    }),
    Series("MobilityDuck (no index)", "#ff7f0e", {
        "Q1": 0.01, "Q2": 0.005, "Q3": 0.41, "Q4": 0.79, "Q5": 81.34,
        "Q6": 0.31, "Q7": 0.68, "Q8": 0.14, "Q9": 6.19, "Q10": 6.24,
        "Q11": 0.62, "Q12": 0.65, "Q13": 7.54, "Q14": 0.54, "Q15": 7.49,
        "Q16": 3.28, "Q17": 0.70,
    }),
    Series("MobilitySpark local[4]", "#2ca02c", {
        "Q1": 0.46, "Q2": 49.92, "Q3": 41.08, "Q4": 47.65, "Q5": 368.29,
        "Q6": 3.87, "Q7": 48.36, "Q8": 0.10, "Q9": 44.05, "Q10": 1156.49,
        "Q11": ">cap", "Q12": ">cap", "Q13": 110.57, "Q14": ">cap",
        "Q15": 261.90, "Q16": 69.65, "Q17": 99.26,
    }),
]


# th3index prefilter matrix — measurements that exist today.
# MobilityDB th3index: same-session warm-cache numbers from the subagent
# pass; MobilityDuck th3index: this session, sf 0.005.
TH3INDEX: list[Series] = [
    Series("MobilityDB R-tree (baseline)", "#1f77b4", {
        "Q4": 15.19, "Q5": 80.61, "Q6": 1.95, "Q7": 9.24, "Q10": 43.46,
    }),
    Series("MobilityDB th3index", "#aec7e8", {
        "Q4": 5.62, "Q5": 86.60, "Q6": 0.05, "Q7": 5.03, "Q10": 1.83,
    }),
    Series("MobilityDuck (baseline, no index)", "#ff7f0e", {
        "Q4": 0.79, "Q5": 81.34, "Q6": 0.31, "Q7": 0.68, "Q10": 6.24,
    }),
    Series("MobilityDuck th3index", "#ffbb78", {
        # this-session local measurements
        "Q4": None, "Q5": None, "Q6": 0.06, "Q7": 0.08, "Q10": 1.73,
    }),
    Series("MobilitySpark th3index", "#2ca02c", {
        # No measurement yet for the MobilitySpark th3index configuration.
        "Q4": None, "Q5": None, "Q6": None, "Q7": None, "Q10": None,
    }),
]


def render(series: list[Series], queries: list[str], out: Path, title: str) -> None:
    n = len(queries)
    k = len(series)
    width = 0.8 / k
    x = np.arange(n)

    fig, ax = plt.subplots(figsize=(max(8, n * 0.9), 5.0), dpi=120)
    floor = 0.001  # log-scale lower bound (1 ms); render missing values at floor

    for i, s in enumerate(series):
        offsets = x - 0.4 + width * (i + 0.5)
        heights = []
        kinds: list[str] = []  # "value", "na", "cap"
        for q in queries:
            v = s.timings.get(q)
            if v is None:
                heights.append(floor)
                kinds.append("na")
            elif isinstance(v, str) and v == ">cap":
                heights.append(CAP_SECONDS)
                kinds.append("cap")
            elif isinstance(v, (int, float)) and v > 0:
                heights.append(float(v))
                kinds.append("value")
            else:
                heights.append(floor)
                kinds.append("value")
        bars = ax.bar(offsets, heights, width=width * 0.92, color=s.color,
                      label=s.label, edgecolor="white", linewidth=0.5)
        for bar, kind in zip(bars, kinds):
            if kind == "cap":
                bar.set_hatch("///")
                bar.set_edgecolor("#333333")
        for off, kind in zip(offsets, kinds):
            if kind == "na":
                ax.text(off, floor * 1.4, "n/a", ha="center", va="bottom",
                        fontsize=7, color="#666666", rotation=90)
            elif kind == "cap":
                ax.text(off, CAP_SECONDS * 1.05, ">cap", ha="center",
                        va="bottom", fontsize=7, color="#333333", rotation=90)

    ax.set_yscale("log")
    numeric_max = max(
        (v for s in series for v in s.timings.values()
         if isinstance(v, (int, float)) and v > 0),
        default=floor,
    )
    has_cap = any(v == ">cap"
                  for s in series for v in s.timings.values())
    top = max(numeric_max, CAP_SECONDS if has_cap else numeric_max) * 1.8
    ax.set_ylim(floor, top)
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.set_xlabel("BerlinMOD R-query")
    ax.set_ylabel("Wall-clock seconds (log scale)")
    ax.set_title(title)
    ax.grid(True, which="both", axis="y", alpha=0.3)
    ax.legend(loc="upper left", fontsize=9, ncol=2)
    fig.tight_layout()
    fig.savefig(out, format="svg")
    plt.close(fig)
    print(f"wrote {out}")


def main() -> int:
    here = Path(__file__).parent.parent
    queries_std = [f"Q{i}" for i in range(1, 18)]
    queries_th3 = ["Q4", "Q5", "Q6", "Q7", "Q10"]
    render(STANDARD, queries_std, here / "cross_platform_standard.svg",
           "BerlinMOD R-queries Q1-Q17 — standard index per platform "
           "(sf 0.005)")
    render(TH3INDEX, queries_th3, here / "cross_platform_th3index.svg",
           "BerlinMOD trip-side cross-join queries — th3index prefilter "
           "vs baseline (sf 0.005)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
