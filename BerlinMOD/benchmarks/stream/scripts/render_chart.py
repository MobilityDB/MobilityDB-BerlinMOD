#!/usr/bin/env python3
"""Render the cross-platform BerlinMOD streaming throughput as grouped bar SVGs.

Reads the throughput matrix from the literal definitions below (one source of
truth, kept in step with ``CrossPlatform_streaming_timings.md``) and
emits one SVG per streaming form:

- streaming_continuous.svg
- streaming_windowed.svg
- streaming_snapshot.svg

Each query gets one coloured bar per platform (higher is better). The y axis is
logarithmic so the O(V^2) Q5-continuous floor does not collapse the fast cells.
A ``None`` timing means the platform has not been measured for that cell and is
rendered as a flat ``n/a`` marker at the y-axis floor (the Nebula column is not
yet measured).

Run ``python3 scripts/render_streaming_chart.py`` to refresh all three SVGs.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


QUERIES = [f"Q{i}" for i in range(1, 10)]
FORMS = ["continuous", "windowed", "snapshot"]


@dataclass(frozen=True)
class Series:
    label: str
    color: str
    # form -> {query -> events/s, or None when not measured}
    throughput: dict[str, dict[str, float | None]]


# Throughput in events/s, on the 216 075-instant real BerlinMOD corpus.
FLINK = Series("Flink (MobilityFlink)", "#1f77b4", {
    "continuous": {"Q1": 87057, "Q2": 213302, "Q3": 68443, "Q4": 59971,
                   "Q5": 24105, "Q6": 95103, "Q7": 58085, "Q8": 69299,
                   "Q9": 137452},
    "windowed":   {"Q1": 187565, "Q2": 215000, "Q3": 88519, "Q4": 65438,
                   "Q5": 229623, "Q6": 97595, "Q7": 44278, "Q8": 80355,
                   "Q9": 231096},
    "snapshot":   {"Q1": 205199, "Q2": 228168, "Q3": 217598, "Q4": 69100,
                   "Q5": 230357, "Q6": 96764, "Q7": 57589, "Q8": 239286,
                   "Q9": 235376},
})

KAFKA = Series("Kafka (MobilityKafka)", "#ff7f0e", {
    "continuous": {"Q1": 78091, "Q2": 260334, "Q3": 86673, "Q4": 44387,
                   "Q5": 12544, "Q6": 52117, "Q7": 86018, "Q8": 78346,
                   "Q9": 76325},
    "windowed":   {"Q1": 201941, "Q2": 222530, "Q3": 79940, "Q4": 41859,
                   "Q5": 137018, "Q6": 51718, "Q7": 30684, "Q8": 65123,
                   "Q9": 141504},
    "snapshot":   {"Q1": 200442, "Q2": 226022, "Q3": 167372, "Q4": 40502,
                   "Q5": 173417, "Q6": 55234, "Q7": 47178, "Q8": 144824,
                   "Q9": 134880},
})

# The Nebula (MobilityNebula) column is not yet measured, so it is omitted from
# the bars; the n/a handling below stays for any future partially-measured row.
SERIES = [FLINK, KAFKA]


def render(form: str, out: Path) -> None:
    n = len(QUERIES)
    k = len(SERIES)
    width = 0.8 / k
    x = np.arange(n)
    floor = 1000.0  # log-scale lower bound (1k ev/s); render missing values here

    fig, ax = plt.subplots(figsize=(max(8, n * 0.9), 5.0), dpi=120)
    for i, s in enumerate(SERIES):
        offsets = x - 0.4 + width * (i + 0.5)
        heights, kinds = [], []
        for q in QUERIES:
            v = s.throughput[form].get(q)
            if v is None or v <= 0:
                heights.append(floor)
                kinds.append("na")
            else:
                heights.append(float(v))
                kinds.append("value")
        ax.bar(offsets, heights, width=width * 0.92, color=s.color,
               label=s.label, edgecolor="white", linewidth=0.5)
        for off, kind in zip(offsets, kinds):
            if kind == "na":
                ax.text(off, floor * 1.4, "n/a", ha="center", va="bottom",
                        fontsize=7, color="#666666", rotation=90)

    ax.set_yscale("log")
    numeric_max = max(
        (v for s in SERIES for v in s.throughput[form].values()
         if isinstance(v, (int, float)) and v > 0),
        default=floor,
    )
    ax.set_ylim(floor, numeric_max * 1.8)
    ax.set_xticks(x)
    ax.set_xticklabels(QUERIES)
    ax.set_xlabel("BerlinMOD streaming query")
    ax.set_ylabel("Throughput events/s (log scale, higher is better)")
    ax.set_title(f"BerlinMOD streaming throughput — {form} form "
                 "(216,075-instant real corpus)")
    ax.grid(True, which="both", axis="y", alpha=0.3)
    ax.legend(loc="upper right", fontsize=9, ncol=1)
    fig.tight_layout()
    fig.savefig(out, format="svg")
    plt.close(fig)
    print(f"wrote {out}")


def main() -> int:
    here = Path(__file__).parent.parent
    for form in FORMS:
        render(form, here / f"streaming_{form}.svg")
    return 0


if __name__ == "__main__":
    sys.exit(main())
