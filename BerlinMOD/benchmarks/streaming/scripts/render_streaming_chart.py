#!/usr/bin/env python3
"""Render the cross-platform BerlinMOD streaming throughput as grouped bar SVGs.

Reads the throughput matrix from the literal definitions below (one source of
truth, kept in step with ``CrossPlatform_streaming_timings_2026-05-29.md``) and
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
    "continuous": {"Q1": 86154, "Q2": 201187, "Q3": 73796, "Q4": 66403,
                   "Q5": 23586, "Q6": 90712, "Q7": 54386, "Q8": 74948,
                   "Q9": 116294},
    "windowed":   {"Q1": 166982, "Q2": 210394, "Q3": 86189, "Q4": 66814,
                   "Q5": 226494, "Q6": 81940, "Q7": 43180, "Q8": 75445,
                   "Q9": 233847},
    "snapshot":   {"Q1": 204616, "Q2": 219365, "Q3": 233342, "Q4": 67042,
                   "Q5": 236148, "Q6": 97595, "Q7": 54967, "Q8": 232839,
                   "Q9": 217818},
})

KAFKA = Series("Kafka (MobilityKafka)", "#ff7f0e", {
    "continuous": {"Q1": 113785, "Q2": 167631, "Q3": 46790, "Q4": 23095,
                   "Q5": 9440, "Q6": 24814, "Q7": 50006, "Q8": 44950,
                   "Q9": 47375},
    "windowed":   {"Q1": 130956, "Q2": 126806, "Q3": 51594, "Q4": 22182,
                   "Q5": 74612, "Q6": 34473, "Q7": 18587, "Q8": 36340,
                   "Q9": 90636},
    "snapshot":   {"Q1": 127029, "Q2": 128388, "Q3": 82883, "Q4": 20581,
                   "Q5": 107715, "Q6": 29771, "Q7": 28860, "Q8": 76487,
                   "Q9": 94729},
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
