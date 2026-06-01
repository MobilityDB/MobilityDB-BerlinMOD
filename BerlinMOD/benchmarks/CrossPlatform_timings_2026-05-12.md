# BerlinMOD three-platform DB benchmark

## What this benchmark measures

[BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD) is the de-facto
trajectory-data benchmark for moving-object databases: 17 range queries
(R-queries) over synthetic vehicle trips in Brussels, each a different shape —
relational filters, point-in-time lookups, spatial cross-joins, temporal windows.

This benchmark measures per-query latency across three databases —
[MobilityDB](https://github.com/MobilityDB/MobilityDB),
[MobilityDuck](https://github.com/MobilityDB/MobilityDuck), and
[MobilitySpark](https://github.com/MobilityDB/MobilitySpark) — that share the
same MEOS kernel and run the same portable SQL over the same dataset. The aim is
to help you choose among the three for a given workload: where each engine pays
its cost, and where the shared spatial accelerator moves the needle.

## Workload

The 17 R-queries fall into four shapes; the shape, not the platform, drives the
cost. Match your application's dominant access pattern to a shape to read the
table that matters for you.

| Shape | Queries | What it does |
|---|---|---|
| **Relational** (no spatial join) | Q1, Q2, Q3, Q8, Q9 | scalar/time filters, point-in-time value, aggregates |
| **Trip × static** | Q4, Q7, Q11, Q12, Q15, Q17 | a trip against a query point or polygon |
| **Trip × trip** | Q6, Q10 | a trip against another trip (cross-join) |
| **Trip × region** | Q13, Q14, Q16 | a trip against query regions over time periods |
| **Aggregated cross-join** | Q5 | minimum distance over the full vehicle cross-join |

The shared spatial accelerator is `th3index` — a temporal H3-cell index of each
trip, available on all three engines. The cross-join shapes (Q4–Q7, Q10) are
reported both without it and with it, so the accelerator's effect is the same
controlled variable on every engine.

## Dataset, hardware, methodology

The dataset is BerlinMOD scalefactor 0.005 — 1620 trips, 141 vehicles — the same
generated CSV files on every platform, loaded into deterministic
`ORDER BY <PrimaryKey>` parameter views so row counts are identical across the
three engines. Hardware is a single 16-core x86-64 Linux machine. Each cell is
one run except the long cross-join queries, reported as the median of three.

- **MobilityDB** — PostgreSQL 17.8.
- **MobilityDuck** — DuckDB 1.4.4 (LTS).
- **MobilitySpark** — Spark 3.5.4 (MEOS via [JMEOS](https://github.com/MobilityDB/JMEOS) as Spark SQL UDFs).

## Invariants held fixed

These hold for every (query, engine) cell.

- **Same SQL, same data.** Every engine runs the same portable SQL over the same
  generated dataset; the result row counts are identical across the three.
- **Soundness gate.** Where an accelerator is applied, the accelerated result
  equals the unaccelerated result for that cell — a cell that fails the equality
  is a failure, not a speedup. Only cells that satisfy it carry a time.

## Results — execution time (s, lower is better)

Each engine fills its column; see [Contributing your numbers](#contributing-your-numbers).
The charts below regenerate once columns land.

| Query | Shape | MobilityDB | MobilityDuck | MobilitySpark |
|---|---|---:|---:|---:|
| Q1 | relational | 0.001 | — | — |
| Q2 | relational | 10.26 | — | — |
| Q3 | relational | 5.24 | — | — |
| Q4 | trip × static | 6.25 | — | — |
| Q5 | aggregated cross-join | 76.84 | — | — |
| Q6 | trip × trip | 1.93 | — | — |
| Q7 | trip × static | 21.10 | — | — |
| Q8 | relational | 0.254 | — | — |
| Q9 | relational | 4.97 | — | — |
| Q10 | trip × trip | 33.29 | — | — |
| Q11 | trip × static | 7.14 | — | — |
| Q12 | trip × static | 7.12 | — | — |
| Q13 | trip × region | 3.34 | — | — |
| Q14 | trip × region | 4.66 | — | — |
| Q15 | trip × static | 1.70 | — | — |
| Q16 | trip × region | 33.00 | — | — |
| Q17 | trip × static | 6.41 | — | — |

### Baseline chart (log scale, lower is better)

All 17 queries under each engine's default configuration.

![Three-platform baseline](cross_platform_standard.svg)

## Acceleration

The spatial cross-join shapes (Q4, Q5, Q6, Q7, Q10) are where indexing decides
the outcome. Three axes, read in order — only the first licenses a cross-engine
statement; the others are intra-engine.

### 1. Shared accelerator — th3index on all three

The temporal H3-cell index is the one accelerator present on every engine, held
constant so the engine is the sole variable. This is the only cross-engine
comparison.

| Query | MobilityDB | MobilityDuck | MobilitySpark |
|---|---:|---:|---:|
| Q4 | 6.25 | — | — |
| Q5 | 76.84 | — | — |
| Q6 | 1.93 | — | — |
| Q7 | 21.10 | — | — |
| Q10 | 33.29 | — | — |

*Q5 has no qualifying predicate, so no row-dropping prefilter is answer-preserving
on it: its `th3index` cell is a throughput diagnostic, not an accelerated Q5.*

![th3index accelerator, all three engines](cross_platform_th3index.svg)

### 2. Engine-native indexes

Each engine's own spatial index, varied within that engine — intra-engine, not a
cross-engine comparison. MobilityDB offers GiST, SP-GiST, and MEST; MobilityDuck
its native R-tree; MobilitySpark has no native spatial index.

| Query | MobilityDB GiST | MobilityDB SP-GiST | MobilityDB MEST | MobilityDuck R-tree |
|---|---:|---:|---:|---:|
| Q4 | — | — | — | — |
| Q5 | — | — | — | — |
| Q6 | — | — | — | — |
| Q7 | — | — | — | — |
| Q10 | — | — | — | — |

![Engine-native indexes](cross_platform_native.svg)

### 3. Combined — th3index + native

The `th3index` prefilter feeding a native exact recheck, reported where the
combination beats either accelerator alone. MobilitySpark has no native index to
combine, so it does not appear here.

| Query | MobilityDB th3index + native | MobilityDuck th3index + R-tree |
|---|---:|---:|
| Q4 | — | — |
| Q5 | — | — |
| Q6 | — | — |
| Q7 | — | — |
| Q10 | — | — |

![Combined th3index + native](cross_platform_combined.svg)

## Reading the results

Oriented to the adoption choice, read top-down:

- **Baseline** (17 queries) — which engine fits a relational-heavy workload and
  which fits the spatial cross-joins. The query *shape* sets the cost class; the
  engine sets the constant factor.
- **Axis 1 (shared th3index)** — the only fair cross-engine statement: relative
  engine cost under identical acceleration on the spatial shapes.
- **Axis 2 (engine-native indexes)** — what each engine's own indexing buys it,
  read within an engine, not across.
- **Axis 3 (combined)** — whether stacking the shared prefilter on top of a
  native index pays beyond either alone — the configuration most adopters
  actually run in production.

## Parity with the stream benchmark

The same MEOS predicate underlies the
[streaming sibling](streaming/CrossPlatform_streaming_timings_2026-05-29.md): the
streaming snapshot at a watermark equals this batch result at that instant, which
is the cross-family correctness link. This batch result is the oracle.

## Contributing your numbers

Each engine fills the grids that apply to it, leaving the rest as `—`.

1. **Baseline** — run the 17 R-queries under your default config; fill your
   engine's column in the **Results** grid.
2. **Axis 1 (shared th3index)** — re-run the spatial shapes (Q4, Q5, Q6, Q7, Q10)
   with the `th3index` accelerator; fill your engine's column.
3. **Axis 2 (native indexes)** — for each native spatial index your engine offers
   (MobilityDB: GiST / SP-GiST / MEST; MobilityDuck: R-tree), fill that column.
   MobilitySpark has no native spatial index and stays `—`.
4. **Axis 3 (combined)** — where your engine has both, run `th3index` + native and
   fill the column where it beats either alone.
5. Add your series to
   [`scripts/render_bench_chart.py`](scripts/render_bench_chart.py) and run
   `python3 scripts/render_bench_chart.py` so the charts replace the placeholders.

## Reproduce

The per-engine run scripts are in [`bench/`](bench). Regenerate the charts from
[`scripts/render_bench_chart.py`](scripts/render_bench_chart.py) — run
`python3 scripts/render_bench_chart.py` to refresh both SVGs.
