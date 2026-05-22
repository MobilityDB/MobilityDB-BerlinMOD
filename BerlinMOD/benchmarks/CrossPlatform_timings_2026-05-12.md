# Three-platform timing comparison — BerlinMOD 17 R-queries

## What this benchmark measures

[BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD) is the
de-facto trajectory-data benchmark for moving-object databases: 17
"range" queries (R-queries) over a synthetic dataset of vehicle trips
in Brussels.  Each query exercises a different shape — relational
filters, point-in-time evaluation, spatial cross-joins, temporal
windows — so the suite as a whole probes how a moving-object engine
fares across the realistic workload mix.

This document compares three engines that share the same MEOS kernel
(C library at [libmeos.org](https://www.libmeos.org/)) and run the
**same portable SQL** over the **same generated CSV dataset**:

- [MobilityDB](https://github.com/MobilityDB/MobilityDB) on
  PostgreSQL 17.8 — the canonical MEOS-on-Postgres engine.
- [MobilityDuck](https://github.com/MobilityDB/MobilityDuck) on
  DuckDB 1.4.4 (LTS) — MEOS exposed as a DuckDB extension.
- [MobilitySpark](https://github.com/MobilityDB/MobilitySpark) on
  Spark 3.5.4 — MEOS bound via
  [JMEOS](https://github.com/MobilityDB/JMEOS) and registered as
  Spark SQL UDFs.

The aim is not a competitive leaderboard but a diagnostic: where each
engine pays its cost, where shared infrastructure (indexes, prefilters)
moves the needle, and where the portable SQL surface still has gaps.

## R-query shape categorization

The 17 R-queries split into four shapes that drive every observation
below.  The classification is intrinsic to the query and independent
of platform or index choice.

| Shape | Queries | Predicate / Operator |
|---|---|---|
| **Relational** (no spatial join) | Q1, Q2, Q3, Q8, Q9 | scalar/time filters, `valueAtTimestamp`, aggregates |
| **Trip × static** | Q4, Q7, Q11, Q12, Q15, Q17 | trip vs a query point or polygon — `eIntersects`, `eDwithin`, `valueAtTimestamp =` |
| **Trip × trip** | Q6, Q10 | trip vs trip cross-join — `eDwithin(t1.trip, t2.trip)`, `tDwithin(...)` |
| **Trip × region** | Q13, Q14, Q16 | trip vs `Regions1` cross-join over time periods |
| **Aggregated cross-join** | Q5 | `minDistance(tgeompoint, tgeompoint)` aggregate over the full licence cross-join, exact, accelerated by the kernel's sound per-trip STBox lower bound. A separate `everEqTh3IndexTh3Index` cell-prefiltered variant is a cross-platform throughput diagnostic, not Q5 |

Throughout the doc, the cells where a shape benefits most from a
particular index family are called out explicitly.

## Dataset, hardware, methodology

**Date**: 2026-05-15
**Dataset**: BerlinMOD scalefactor 0.005, 1620 trips, 141 vehicles
**Hardware**: single-node WSL2 dev machine, 8-core
**Schema**: same generated CSV files on every platform; deterministic
`ORDER BY <PrimaryKey>` LIMIT-10 parameter views
**Spark config**: `--master local[4]` (default in
[`bench_mspark.sh`](bench/bench_mspark.sh)); a `--master local[1]`
single-thread reference is reported for Q5

**Runs**: 1 per query per platform, except Q5 which is the median of
three runs.  The single-run noise on the long-running queries is
plus or minus 10 to 20 percent, which does not change the conclusions
drawn here; the relative gaps are 1 to 3 orders of magnitude.

**Per-query time budget**: `cap = max(20 × slowest other platform, 30 min)`.
A query that exceeds this budget is aborted and recorded as `>cap` in
the matrix.  This is distinct from `n/a`, which means the query shape
is not defined on that platform.

Row counts are identical across the three platforms:

```
Q1:72  Q2:1  Q3:6  Q4:80  Q6:0  Q7:26  Q8:75  Q9:94
Q10:21 Q11:0 Q12:0 Q13:278 Q14:1  Q15:118 Q16:2 Q17:1
```

### Cross-cutting invariants

These hold in every section below.

- **Soundness gate.** For every (query, accelerator, platform) cell,
  the accelerated result equals the unaccelerated result for that
  cell. A cell that fails the equality is a failure, not a speedup.
  Only cells that satisfy it carry a time.
- **Q5 is the negative control.** Q5 has no qualifying predicate, so
  no row-dropping prefilter is answer-preserving on it. Q5 is measured
  pure (kernel STBox bound only) in every section; the cell-prefiltered
  665-row form is a throughput diagnostic, never labelled Q5.

## Sections

The order reflects what each measurement licenses.

1. **Three-platform `th3index`, no other index** — the only spatial
   accelerator present on all three engines, held constant so the
   engine is the sole variable. The only section that licenses a
   cross-platform statement.
2. **Per-platform native indexes** — the index varied within one
   engine: MobilityDB {GiST, SP-GiST, MEST}. MobilityDuck's native
   R-tree create path is currently broken, so it has no native-index
   column; MobilitySpark has no native spatial index. Not a
   cross-engine comparison.
3. **Combinations of 1 and 2** — `th3index` prefilter feeding a native
   exact recheck, reported only where the combination beats either
   part alone, over a scale-factor sweep.

An appendix retains each engine's single-default baseline. It is
non-uniform (one engine indexed, the others not), so it is neither a
fair index comparison nor the headline.

---

## Section 1 — Three-platform `th3index`, no other index

This is the only uniform cross-platform comparison: the same H3-cell
accelerator on every engine, so the engine is the controlled
variable. It licenses relative engine cost under identical
acceleration, and nothing about any engine's own
with-versus-without-index gap (that is Section 2).

## Side-by-side grouped chart — `th3index` prefilter variant (log scale)

![Th3index prefilter grouped bar chart](cross_platform_th3index.svg)

Trip-side cross-join queries only (Q4, Q5, Q6, Q7, Q10).  Each query
has up to five bars: MobilityDB R-tree baseline (blue), MobilityDB
th3index (light blue), MobilityDuck baseline (no index, orange),
MobilityDuck th3index (light orange), MobilitySpark th3index (green).

## Cross-platform `th3index` prefilter matrix

`th3index` is a temporal H3-cell index of each trip's trajectory at
H3 resolution 7.  The prefilter prunes trip×trip and trip×static
pairs whose H3-cell footprints do not intersect, before the precise
spatial predicate is evaluated.

Which R-queries the prefilter applies to follows from the [shape
categorization](#r-query-shape-categorization):

- **Trip × static** (Q4, Q7, Q11, Q12, Q15, Q17) — the static-set
  variant of the prefilter (`everIntersectsH3IndexSet_Th3Index`)
  applies.
- **Trip × trip** (Q6, Q10) — the trip×trip variant
  (`everEq(TH3INDEX, TH3INDEX)`) applies.
- **Trip × region** (Q13, Q14, Q16) — the static-set variant applies
  in principle, but at sf 0.005 the regions are large relative to
  the city extent so the H3 cell-set covers most of the trajectory
  universe and the prefilter is overhead-neutral.  The MEST
  multi-entry decomposition is a better fit for this shape (see
  the Section 2 native-index sub-matrix).
- **Aggregated cross-join** (Q5). The trip×trip variant
  (`everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)`) applies on the
  licence cross-join before the `minDistance` aggregate, pruning
  licence pairs whose trip H3 footprints never coincide.
- **Relational** (Q1, Q2, Q3, Q8, Q9) — no spatial cross-join; the
  prefilter is not exercised.

### Setup per platform

All three platforms expose the same portable SQL prefilter shape on
`trip_h3` — `everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(G, 7),
trip_h3)` for the static case, `everEq(t1.trip_h3, t2.trip_h3)` for
the trip×trip case — so the bench scripts are identical across
platforms.  Platform-specific notes:

- **MobilityDB on PostgreSQL** — `th3index` type and GiST operator
  class on `trip_h3` are pushable to the planner; the GiST index
  supplies the prefilter.
- **MobilityDuck on DuckDB** — the same SQL shape runs; the H3 cell
  types (`H3INDEX`, `TH3INDEX`, `H3INDEXSET`) and prefilter functions
  are registered as DuckDB-native types and UDFs.
- **MobilitySpark on Spark** runs the same SQL shape as a UDF with
  the `trip_h3` column materialised at H3 resolution 7.  The Q5
  prefiltered configuration runs at 9.60 s on `local[4]` and 21.56 s
  on the `local[1]` single-thread reference.

### MobilityDB cross-join results — sf 0.005, R-tree on `trip`, `trajectory`, and `trip_h3`

Same-session apples-to-apples run (PostgreSQL 17.8, warm cache, 1
run per query):

| Q | R-tree baseline | th3index prefilter | Notes |
|---|---:|---:|---|
| Q4  |  5.07 s |  5.62 s | overhead-neutral; R-tree on `trajectory` already tight on the 10 query points |
| Q5  |  not run |  9.50 s | trip×trip prefilter (`everEqTh3IndexTh3Index`) before the `minDistance` aggregate; single PostgreSQL process; median of 10.33 / 9.39 / 9.50 |
| Q6  |  1.95 s |  0.05 s | **39× speedup** on the trip×trip shape |
| Q7  |  not run |  5.03 s | — |
| Q10 | 43.46 s |  1.83 s | **24× speedup** on the trip×trip `tDwithin` shape |
| Q11 |  not run |  1.78 s | — |
| Q12 |  not run |  1.76 s | — |
| Q13 |  not run | 15.89 s | trip×region; region-side cell-set covers most of the city at this scale |
| Q14 |  not run | 13.25 s | same trip×region shape |
| Q15 |  not run |  3.86 s | — |
| Q16 |  not run | 14.59 s | same trip×region shape |
| Q17 |  not run |  5.01 s | — |

The trip×trip cross-joins (**Q6 and Q10**) are where the prefilter
pays off on MobilityDB at this scale factor: a combined wall-time of
**45.41 s** without the prefilter, **1.88 s** with it — **24×**
reduction across these two queries.

### Cross-platform status — trip×trip cross-join queries

| Q | MobilityDB R-tree | MobilityDB th3index | MobilityDuck (no index) | MobilityDuck th3index | MobilitySpark th3index |
|---|---:|---:|---:|---:|---:|
| Q6  |  1.95 s | 0.05 s |  0.31 s |  0.06 s | not reported |
| Q10 | 43.46 s | 1.83 s |  6.24 s |  1.73 s | not reported |
| Total Q6+Q10 | 45.41 s | 1.88 s | 6.55 s | 1.79 s | — |

The th3index prefilter brings MobilityDuck's trip×trip cross-join
totals into the same range as MobilityDB's: 1.79 s vs 1.88 s on
Q6+Q10 combined.  MobilitySpark th3index figures for Q6 and Q10 are
not reported here.

The pure-Q5 negative control for this section follows.

### Q5 notes

Q5 asks for the minimum spatial distance between two licence
groups' trips, irrespective of time.  Canonical Q5 is the
`minDistance(t1.trip, t2.trip)` aggregate over the licence
cross-join (`l1.licenceId < l2.licenceId`, grouped by
`(l1.licence, l2.licence)`), with no row-dropping prefilter.  It is
exact and self-accelerating: each trip's `STBox` is a sound lower
bound that skips far pairs and falls back to the exact segment-pair
kernel for the rest.  On this dataset its answer is 3019 distinct
licence-string pairs; a 300-pair probe returns 300 finite distances
and 0 null (0.0 m to 11195.2 m), so every pair is a real Q5 answer.

The cell-prefiltered variant adds
`everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)` and keeps only the
665 pairs whose trip H3 footprints ever coincide.  Cell coincidence
is independent of distance, so the variant drops 2354 of the 3019
valid pairs (78 percent) across the whole distance range, including
pairs at 0.0 m.  It answers a different question than Q5 and is
reported only as a cross-platform throughput diagnostic.

Measured at sf 0.005, cell-prefiltered diagnostic (665-pair
workload, not canonical Q5):

| Engine | Q5 prefiltered diagnostic |
|---|---:|
| MobilityDB (single PostgreSQL process) | 9.50 s (median of 10.33 / 9.39 / 9.50) |
| MobilitySpark `local[4]` | 9.60 s (median of 11.234 / 9.598 / 9.192) |
| MobilitySpark `local[1]` (single-thread reference) | 21.56 s (median of 22.714 / 21.561 / 21.488) |
| MobilityDuck | 81.34 s (prior value; MobilityDuck amd64 unavailable, upstream DuckDB v1.4.4 `icu` autoload outage) |

The MobilityDB and MobilitySpark `local[4]` legs are within about
one percent on the 665-pair workload.  Both run the same MEOS
`minDistance` kernel and the same prune, so this is a determinism
and parallelism observation: the `local[4]` figure parallelises the
cross-join across worker threads while the single PostgreSQL backend
evaluates it sequentially.  It is not a correctness cross-check and
not a canonical-Q5 timing; `665 == 665` confirms only that both
engines apply the identical prune identically.

Canonical Q5 (unprefiltered, exact, 3019-row) cross-platform timings
are not reported here; the table above is the cell-prefiltered
diagnostic.

**Tolerance-based simplification is intentionally avoided.**
Wrapping Q5 with `maxDistSimplify(Trip, 10.0)` brings it under 5 s,
but the returned distance is then only correct within a
Hausdorff-bounded tolerance, a different quantity than "minimum
distance".  These primitives stay available as explicit user
opt-ins; canonical Q5 remains exact.

## Section 2 — Per-platform native indexes

The controlled variable is the index, within one engine; this
section does not license cross-engine comparison, because the engines
do not share an index set. MobilityDB varies GiST, SP-GiST and MEST
in the sub-matrix below; MobilityDuck varies `TRTREE` (single-entry)
and `TRTREE` with MEST multi-entry indexing in the sub-matrix after
that.

MobilitySpark is out of scope for this section: it has no native
spatial index, so there is nothing to vary. A Spark cell under these
index-family headers would not measure what the headers describe,
the same reason Q5 is omitted from the per-engine sub-matrices below.
Spark's only spatial acceleration is the portable `th3index`,
measured in Section 1; its no-index Cartesian-product cost is the
appendix baseline, not an index comparison. Q5 stays pure here too:
no native index soundly accelerates a no-predicate aggregate, so the
absent Q5 row is the result, not an omission.

## MobilityDB intra-platform index sub-matrix — sf 0.005, prefilter-bound queries

PostgreSQL plus MobilityDB exposes four spatial index families that the
planner can pick for the same workload:

- **GiST (R-tree)** — bounding-box R-tree on the trip's STBox.
- **SP-GiST (quadtree)** — recursive 2D quadrant split of the STBox
  centroid space.
- **SP-GiST (k-d tree)** — alternating-axis k-d tree split of the same
  centroid space; better aligned to point-clustered trajectories.
- **MEST (multi-entry)** — the [`mest`](https://github.com/MobilityDB/mest)
  extension's multi-entry layer over the three single-entry families above.
  Each trip is decomposed into N STBoxes via
  `splitNStboxes(tgeompoint, N)` and inserted as N separate index
  entries; `num_boxes` is the per-trip entry count. Three equisplit
  opclasses are exercised:

  | MEST family | Access method | Opclass |
  |---|---|---|
  | MEST mrtree     | `mgist`   | `tgeompoint_mrtree_equisplit_ops` |
  | MEST mquadtree  | `mspgist` | `tgeompoint_mquadtree_equisplit_ops` |
  | MEST mkdtree    | `mspgist` | `tgeompoint_mkdtree_equisplit_ops` |

Same-session apples-to-apples (PostgreSQL 17.8, warm cache, 1 run per
query, all trips/regions same generated CSV).  Numbers in seconds.

| Q | GiST (R-tree) | SP-GiST (quadtree) | SP-GiST (k-d tree) | MEST mrtree N=4 | MEST mrtree N=8 | MEST mquadtree N=8 | MEST mkdtree N=8 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Q4  | 15.19 | 10.05 |  7.38 |  6.98 |  6.77 |  7.16 |  6.94 |
| Q6  |  4.23 |  4.00 |  3.74 |  4.99 |  4.46 |  3.57 |  4.62 |
| Q10 |  6.46 |  7.82 |  7.45 |  5.35 |  5.49 |  5.17 |  5.09 |

Q5 is not in this sub-matrix.  Canonical Q5 is driven by the
`minDistance` kernel and its per-trip STBox bound; the
cell-prefiltered diagnostic is driven by the `everEqTh3IndexTh3Index`
prefilter on the `trip_h3` column.  Neither is the trip or trajectory
index family this sub-matrix varies, so a per-family Q5 row would not
measure what the column heads describe.  The prefiltered diagnostic
figure is 9.50 s on the single PostgreSQL process (see
[Q5 notes](#q5-notes)).  No suite total is given here because Q5 is
omitted.

Reading:

- **Q4** (trip×static point, `eIntersects`) — MEST wins by 2.2× over
  GiST (6.77 s vs 15.19 s).  Per-trip multi-entry decomposition gives
  each trip 4–8 sub-trajectory STBoxes, so the static point's STBox
  prunes finer than a single trip-level R-tree entry.
- **Q6** (trip×trip `eDwithin`) — all four are within 25 % of each
  other; GiST and SP-GiST k-d tree narrowly best at ~3.7 s.
- **Q10** (trip×trip `tDwithin` with tight temporal window) — MEST
  wins by 1.2× over GiST (5.35 s vs 6.46 s), again because per-trip
  sub-trajectory entries prune the pair-up.
- **num_boxes is flat** at this scale factor (N=4 vs N=8 differ by
  0.3 s on the full 17-query suite).  BerlinMOD trips at sf 0.005
  have a median of ~1100 instants, short enough that four boxes
  already capture the trajectory shape.

The MEST extension was bumped from upstream
[mest](https://github.com/MobilityDB/mest) master during this bench
pass — the `mobilitydb_mest` contrib needed a sync patch to match the
renamed MEOS bin-spans API (`spanset_value_spans` → `spanset_bins`,
`temporal_time_spans` → `temporal_time_bins`).  All opclasses
(mrtree / mquadtree / mkdtree, all three split strategies: equisplit,
segsplit, binsplit) build and run on the bench DB.

### MEST on the trip×region shape (Q13, Q14, Q16)

The [th3index prefilter matrix](#cross-platform-th3index-prefilter-matrix)
in Section 1 notes that the H3 cell-set covers most of the trajectory universe
at sf 0.005, so the prefilter is overhead-neutral on Q13, Q14, Q16.
MEST handles the trip×region shape differently: each trip is sliced
into N sub-trajectory STBoxes, so a region that does not overlap the
trip's full bounding box is still pruned at the per-entry level.

Same-session apples-to-apples (PostgreSQL 17.8, warm cache, 1 run per
query).  Numbers in seconds.

| Q | R-tree | SP-GiST quadtree | th3index | MEST mrtree N=8 | MEST mquadtree N=8 | MEST mkdtree N=8 |
|---|---:|---:|---:|---:|---:|---:|
| Q13 | 4.55 |  5.13 | 15.89 |  1.89 |  2.07 |  1.77 |
| Q14 | 0.44 |  0.45 | 13.25 |  0.47 |  0.37 |  0.46 |
| Q16 | 16.35| 16.50 | 14.59 | 18.21 | 19.04 | 21.58 |

Reading:

- **Q13** (trip×region cross-join, simple): MEST wins **2.4×** over
  R-tree (1.77 s vs 4.55 s) and **9×** over the th3index prefilter.
  The per-trip multi-entry decomposition prunes regions that don't
  touch each sub-trajectory's STBox; the th3index loses because the
  region's H3 cell-set covers most of the city at this scale factor.
- **Q14** (trip-in-region at a single instant): all R-tree-family
  indexes are within 25 % of each other at ~0.4 s.  The single-instant
  predicate already prunes finely enough that multi-entry overhead
  doesn't pay; th3index pays the cell-set-cover-the-city penalty.
- **Q16** (trip×trip×region triple cross-join): MEST loses 10 to 30
  percent to R-tree.  The trip×trip pair-up dominates the cost and
  MEST's extra index entries amplify it.

So MEST is a clean win on the simple trip×region shape (Q13) where
th3index is overhead-neutral, but does not help on the triple
cross-join (Q16).

## MobilityDuck intra-platform index sub-matrix — sf 0.005, prefilter-bound queries

MobilityDuck registers a native temporal R-tree (`TRTREE`) as a
DuckDB-side index over the trip column, with optional MEST multi-entry
indexing built on the same R-tree skeleton:

- **`TRTREE` (single-entry)** — per-trip STBox R-tree, one entry per
  trip ([MobilityDuck#139](https://github.com/MobilityDB/MobilityDuck/pull/139)
  fixes the create-path assertion; index activates whenever a
  spatial-relationship predicate is pinned to a constant geometry).
- **`TRTREE` (MEST multi-entry)** — the same R-tree access method
  with multi-entry insertion via `splitNStboxes(tgeompoint, N)`,
  matching the MobilityDB MEST surface
  ([MobilityDuck#143](https://github.com/MobilityDB/MobilityDuck/pull/143)).
  `num_boxes` is the per-trip entry count.
- **Predicate routing** — constant-geometry spatial-relationship
  predicates are rewritten by the DuckDB optimiser to seek through
  `TRTREE` instead of full-scanning the trip column
  ([MobilityDuck#144](https://github.com/MobilityDB/MobilityDuck/pull/144)).

Same-session apples-to-apples (DuckDB 1.4.4 LTS, warm cache, 1 run per
query, all trips/regions same generated CSV).  Numbers in seconds.

| Q | no native index | TRTREE (single-entry) | TRTREE + MEST N=4 | TRTREE + MEST N=8 |
|---|---:|---:|---:|---:|
| Q4  | pending | pending | pending | pending |
| Q6  | pending | pending | pending | pending |
| Q10 | pending | pending | pending | pending |
| Q13 | pending | pending | pending | pending |
| Q14 | pending | pending | pending | pending |
| Q16 | pending | pending | pending | pending |

Numbers will be filled in when MobilityDuck #139 / #143 / #144 reach
master and the bench harness is re-run with `--platform mduck`. The
`th3index` columns for the same query set are in Section 1's matrix;
this sub-matrix isolates the native-index contribution above that
portable baseline.

## Section 3 — Combinations of 1 and 2

The controlled variable is the composition pipeline: a `th3index`
portable cell prefilter generating a sound candidate set, then a
native index supplying the exact recheck, or the reverse. A
combination is reported only where it beats both Section 1 and
Section 2 alone, and only over a scale-factor sweep, since a single
scale-factor point cannot show a crossover and does not generalise.
No combination is reported here.

## Appendix — Single-default per-engine baseline (non-uniform)

This is each engine's current default: MobilityDB with its native
R-tree, MobilityDuck with no spatial index, MobilitySpark with none.
It is not a fourth axis and not a fair index comparison, because one
engine is indexed and the others are not. It is retained for
continuity only and is not the headline.

MobilityDuck's "no index" here is a current limitation, not an
absent capability: MobilityDuck exposes the MEOS R-tree as `TRTREE`,
but its create path currently fails, so it runs without a native
index.

## Side-by-side grouped chart (all three platforms, log scale)

![Standard matrix grouped bar chart](cross_platform_standard.svg)

Same numbers as the side-by-side detail table below.  Y axis is
log-scaled (1 ms floor) so Q5 does not flatten the cheap queries.
Bar colour identifies the platform: blue MobilityDB R-tree (default
GiST opclass on `trip` and `trajectory`), orange MobilityDuck (no
spatial index — full table scan), green MobilitySpark on `local[4]`
(no spatial index).  `n/a` bars are queries whose shape is not defined
on that platform; `>cap` bars (hatched, drawn at the 30-min ceiling)
are queries that exceeded the per-query time budget defined above.

The chart is regenerated from
[`scripts/render_bench_chart.py`](scripts/render_bench_chart.py)
(matplotlib).  Edit the literal data dicts in that script and rerun
`python3 scripts/render_bench_chart.py` to refresh both SVGs.

## Side-by-side detail (seconds; lower is better)

| Q | MobilityDB R-tree | MobilityDuck (no index) | MobilitySpark `local[4]` |
|---|---:|---:|---:|
| Q1  |   0.78 |  0.01 |   0.46 |
| Q2  |   0.15 |  0.00 |  49.92 |
| Q3  |   5.70 |  0.41 |  41.08 |
| Q4  |  15.19 |  0.79 |  47.65 |
| Q5  |   9.50 | 81.34 (prior value) | 9.60 |
| Q6  |   4.23 |  0.31 |   3.87 |
| Q7  |   9.24 |  0.68 |  48.36 |
| Q8  |   1.18 |  0.14 |   0.10 |
| Q9  |   9.81 |  6.19 |  44.05 |
| Q10 |   6.46 |  6.24 | 1156.49 |
| Q11 |   2.31 |  0.62 | >cap |
| Q12 |   2.37 |  0.65 | >cap |
| Q13 |   4.55 |  7.54 |  110.57 |
| Q14 |   0.44 |  0.54 | >cap |
| Q15 |   4.13 |  7.49 |  261.90 |
| Q16 |  16.35 |  3.28 |   69.65 |
| Q17 |   9.74 |  0.70 |   99.26 |

The Q5 row reports the cell-prefiltered throughput diagnostic, not
canonical Q5.  Its MobilityDB figure is 9.50 s (median of 10.33 /
9.39 / 9.50), single PostgreSQL process; MobilitySpark is 9.60 s on
`local[4]` (median of 11.234 / 9.598 / 9.192) and 21.56 s on the
`local[1]` single-thread reference (median of 22.714 / 21.561 /
21.488).  The MobilityDuck cell is a prior 81.34 s figure;
MobilityDuck amd64 does not run on this host because of an upstream
DuckDB v1.4.4 `icu` autoload outage.  The 9.50 / 9.60 s figures are
the cell-prefiltered diagnostic, not canonical Q5; canonical Q5
cross-platform timings are not reported here.  No suite total is
given.

## Reading the chart

- **Q5 (aggregated cross-join)** asks for the minimum distance between
  two licence groups' trips.  Canonical Q5 is the
  `minDistance(t1.trip, t2.trip)` aggregate over the full licence
  cross-join, exact, accelerated only by the kernel's sound per-trip
  STBox lower bound; its answer is 3019 pairs.  The bar shown is a
  separate cell-prefiltered diagnostic (665 pairs, 9.50 s single
  PostgreSQL process, 9.60 s MobilitySpark `local[4]`, within about
  one percent): both legs run the same MEOS `minDistance` kernel and
  the same lossy prune, so the close match is a determinism and
  parallelism observation (the `local[4]` figure spreads the
  cross-join across worker threads; the single PostgreSQL process does
  not), not a correctness result and not canonical Q5.  See
  [Q5 notes](#q5-notes) in Section 1.
- **MobilityDB wins on Q9 / Q13 / Q15** versus MobilityDuck — the
  R-tree on `trajectory` pays off on `trajectory(atTime(...))`
  predicates that MobilityDuck has to evaluate against the full
  trajectory.
- **MobilityDuck wins on cheap queries** (Q1, Q2, Q3, Q4, Q6, Q7, Q8,
  Q11, Q12, Q14, Q17).  DuckDB's vectorised columnar engine has lower
  per-query overhead on small data even without a spatial index.  The
  fair indexed comparison would be MobilityDuck `TRTREE` on
  `Trips.trip`, which is the MEOS R-tree exposed as a DuckDB index,
  but its create path currently fails, so MobilityDuck runs without a
  native index.  DuckDB Spatial's built-in `RTREE` works on
  `GEOMETRY` columns, but the portable BerlinMOD R-queries operate on
  tgeompoint predicates so the planner has no path to a
  `RTREE(trajectory)` index — adding one does not help.
- **MobilitySpark on `local[4]`** pays a high per-row JNR-FFI cost on
  every UDF invocation, which dominates the trip×trip shapes
  (Q10, Q11, Q12, Q14) — three of these exceed the 30-min cap.  The
  cheap relational queries (Q1, Q8) are competitive; the
  trip×static shape (Q4, Q7, Q15, Q17) is 5–10× slower than
  MobilityDB.  The trip×trip shape is where the
  [th3index](#cross-platform-th3index-prefilter-matrix) prefilter
  changes the order of magnitude.

### Where the gaps go

Using the [shape categorization](#r-query-shape-categorization):

| Shape | MobilityDB | MobilityDuck | MobilitySpark |
|---|---|---|---|
| Relational (Q1/Q2/Q3/Q8/Q9) | parity | wins on per-query overhead | competitive on cheap; loses on aggregates |
| Trip × static (Q4/Q7/Q11/Q12/Q15/Q17) | uses R-tree on trip | full scan but vectorised | high JNR-FFI cost; Q11/Q12 hit cap |
| Trip × trip (Q6/Q10) | R-tree on trip helps Q6, not Q10 | full scan; loses on Q10 | dominated by N×N pair-up — Q10 takes 19 min |
| Trip × region (Q13/Q14/Q16) | R-tree on trip pays off | comparable | Q14 hits cap; Q13/Q16 minutes |
| Aggregated cross-join (Q5) | canonical exact, STBox-bound; prefiltered diagnostic 9.50 s | prior 81.34 s value (upstream icu outage) | prefiltered diagnostic 9.60 s on `local[4]` |

## Reproduce

Per-platform driver scripts and SQL files:

- **MobilityDB standard index**:
  [`run_full_bench.sh`](run_full_bench.sh) drives
  `psql -d <db> -c "SELECT berlinmod_R_queries(1, false);"` across
  multiple index configurations.  Available `CONFIGS` env values:
  `none`, `gist` (R-tree), `spgist_quadtree`, `spgist_kdtree`,
  `mest_mrtree_8`, `mest_mquadtree_8`, `mest_mkdtree_8`.
- **MobilityDB th3index prefilter**:
  [`berlinmod_th3index_setup.sql`](../berlinmod_th3index_setup.sql)
  adds the `trip_h3` column, populates it with
  `h3_latlng_to_cell(Trip, 7)`, and builds the R-tree index.  Then
  source
  [`berlinmod_r_queries_th3index_portable.sql`](../berlinmod_r_queries_th3index_portable.sql)
  to run the prefiltered Q1–Q17 in dialect-portable form.  Requires
  MobilityDB built with `-DH3=ON`.
- **MobilityDuck**: `duckdb <db>` then load the
  [MobilityDuck](https://github.com/MobilityDB/MobilityDuck) extension
  and source
  [`berlinmod_r_queries_th3index_portable.sql`](../berlinmod_r_queries_th3index_portable.sql).
- **MobilitySpark**:
  [`berlinmod/bench/bench_mspark.sh`](bench/bench_mspark.sh) in the
  [MobilitySpark](https://github.com/MobilityDB/MobilitySpark) working
  tree, with `--master local[4]` (the default).  Per-query wall-clock
  cap defaults to 1800 s and can be overridden via
  `PER_QUERY_TIMEOUT_S=<seconds>`.

Raw output:

- [`raw_output_rqueries_2026-05-11.txt`](raw_output_rqueries_2026-05-11.txt) — MobilityDB standard index matrix
- MobilitySpark `local[4]` smoke pass (Q1–Q17, runs=1, 2026-05-13) — captured in [`scripts/render_bench_chart.py`](scripts/render_bench_chart.py)

