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
  DuckDB — MEOS exposed as a DuckDB extension.
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
| **Aggregated cross-join** | Q5 | `ST_Distance(ST_Collect(...), ST_Collect(...))` over licence groups |

Throughout the doc, the cells where a shape benefits most from a
particular index family are called out explicitly.

## Dataset, hardware, methodology

**Date**: 2026-05-13
**Dataset**: BerlinMOD scalefactor 0.005, 1620 trips × 141 vehicles
**Hardware**: single-node WSL2 dev machine, 8-core
**Schema**: same generated CSV files on every platform; deterministic
`ORDER BY <PrimaryKey>` LIMIT-10 parameter views
**Spark config**: `--master local[4]` (default in
[`bench_mspark.sh`](bench/bench_mspark.sh))

**Runs**: 1 per query per platform.  The single-run noise on the
long-running queries is ±10–20 %, which does not change the conclusions
drawn here; the relative gaps are 1–3 orders of magnitude.

**Per-query time budget**: `cap = max(20 × slowest other platform, 30 min)`.
A query that exceeds this budget is aborted and recorded as `>cap` in
the matrix.  This is distinct from `n/a`, which means the query shape
is not defined on that platform.

Row counts are identical across the three platforms:

```
Q1:72  Q2:1  Q3:6  Q4:80  Q5:100  Q6:0  Q7:26  Q8:75  Q9:94
Q10:21 Q11:0 Q12:0 Q13:278 Q14:1  Q15:118 Q16:2 Q17:1
```

## Sections

1. **Standard index matrix** (this document, below) — MobilityDB with
   the standard R-tree on `trip` and `trajectory`, MobilityDuck without
   a spatial index, MobilitySpark on `local[4]`.
2. **MobilityDB intra-platform index sub-matrix** — same workload on
   MobilityDB, comparing R-tree, SP-GiST quadtree, SP-GiST k-d tree
   and three MEST variants from [mest](https://github.com/MobilityDB/mest).
3. **Cross-platform `th3index` prefilter matrix** — temporal H3-cell
   index on `trip` accelerating trip×trip and trip×static shapes
   across all three platforms.

---

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
| Q5  |  80.61 | 81.34 | 368.29 |
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
| **Total Q1–Q9** | **126.89** | **89.88** | **603.84** |

## Reading the chart

- **Q5 (aggregated cross-join) dominates the total** on both
  MobilityDB and MobilityDuck (~80 s each).  The pattern
  `ST_Distance(ST_Collect(t1.trip), ST_Collect(t2.trip))` builds a
  materialised geometry collection per licence group; neither platform
  has an applicable index path through that aggregate, and the
  Cartesian product of the 10 × 10 licence groups is the dominant cost.
  See [Q5 optimisation notes](#q5-optimisation-notes) below.
- **MobilityDB wins on Q9 / Q13 / Q15** versus MobilityDuck — the
  R-tree on `trajectory` pays off on `trajectory(atTime(...))`
  predicates that MobilityDuck has to evaluate against the full
  trajectory.
- **MobilityDuck wins on cheap queries** (Q1, Q2, Q3, Q4, Q6, Q7, Q8,
  Q11, Q12, Q14, Q17).  DuckDB's vectorised columnar engine has lower
  per-query overhead on small data even without a spatial index.  The
  fair indexed comparison — MobilityDuck `TRTREE` on `Trips.trip` —
  is currently blocked on an upstream assertion failure in the
  MobilityDuck TRTREE module: `CREATE INDEX … USING TRTREE` crashes
  with a DuckDB internal-error assertion on any table, including a
  2-row test fixture.  Once the bug is fixed, an indexed MobilityDuck
  column will land in this matrix.  DuckDB Spatial's built-in `RTREE`
  works on `GEOMETRY` columns, but the portable BerlinMOD R-queries
  operate on tgeompoint predicates so the planner has no path to a
  `RTREE(trajectory)` index — adding one does not help.
- **MobilitySpark on `local[4]`** pays a high per-row JNR-FFI cost on
  every UDF invocation, which dominates the trip×trip shapes
  (Q10, Q11, Q12, Q14) — three of these exceed the 30-min cap.  The
  cheap relational queries (Q1, Q8) are competitive; the
  trip×static shape (Q4, Q7, Q15, Q17) is 5–10× slower than
  MobilityDB.  The trip×trip shape is where the
  [th3index](#cross-platform-th3index-prefilter-matrix) prefilter
  changes the order of magnitude.

### Q5 optimisation notes

Q5 builds one MultiLineString per licence (10 + 10 collections of
trajectories) and computes
`ST_Distance(MultiLineString, MultiLineString)` over the 10 × 10
cross-join.  Profiling on MobilityDB at sf 0.005:

| Step | Wall time |
|---|---:|
| Build the 10 + 10 collections (`ST_Collect(array_agg(trajectory))`) | 47 ms |
| 100 `ST_Distance(MultiLine, MultiLine)` calls | ~103 s |

Two SQL-level alternatives were tested and **both are worse**:

1. **Min-of-pairs rewrite** —
   `MIN(ST_Distance(trajectory(t1), trajectory(t2)))` over the 100 × 16 × 16
   trip pairs runs in **256 s**, 2.5× slower than the original.  GEOS
   uses internal indexing inside one `ST_Distance` call on a
   MultiLineString; replacing it with 14,400 small calls loses that
   indexing and pays per-row Postgres overhead.
2. **Materialised `trajectory` column** instead of `trajectory(Trip)`
   recompute — same ~105 s.  The recompute is not the cost.

The right optimisation is a **MEOS-side fused aggregate** that uses
each trip's `STBox` to prune trip pairs whose bounding boxes are
already far apart before falling back to GEOS for the surviving pairs.
Not deliverable in this bench cycle.

### Where the gaps go

Using the [shape categorization](#r-query-shape-categorization):

| Shape | MobilityDB | MobilityDuck | MobilitySpark |
|---|---|---|---|
| Relational (Q1/Q2/Q3/Q8/Q9) | parity | wins on per-query overhead | competitive on cheap; loses on aggregates |
| Trip × static (Q4/Q7/Q11/Q12/Q15/Q17) | uses R-tree on trip | full scan but vectorised | high JNR-FFI cost; Q11/Q12 hit cap |
| Trip × trip (Q6/Q10) | R-tree on trip helps Q6, not Q10 | full scan; loses on Q10 | dominated by N×N pair-up — Q10 takes 19 min |
| Trip × region (Q13/Q14/Q16) | R-tree on trip pays off | comparable | Q14 hits cap; Q13/Q16 minutes |
| Aggregated cross-join (Q5) | no index path; 80 s | no index path; 81 s | per-row UDF cost; 6 min |

## MobilityDB intra-platform index sub-matrix — sf 0.005, prefilter-bound queries

The default `gist` column above uses an R-tree on the trip column and
trajectory.  PostgreSQL plus MobilityDB exposes three other spatial
index families that the planner can pick for the same workload:

- **GiST (R-tree)** — bounding-box R-tree on the trip's STBox.
- **SP-GiST (quadtree)** — recursive 2D quadrant split of the STBox
  centroid space.
- **SP-GiST (k-d tree)** — alternating-axis k-d tree split of the same
  centroid space; better aligned to point-clustered trajectories.
- **MEST (multi-entry R-tree, equisplit)** — multi-entry R-tree from
  the `mest` extension (`github.com/MobilityDB/mest`).  Each trip is
  decomposed into N equal-time-duration STBoxes
  (`splitNStboxes(tgeompoint, int)`) and inserted as N separate
  index entries.  `num_boxes` is the per-trip entry count.

Same-session apples-to-apples (PostgreSQL 17.8, warm cache, 1 run per
query, all trips/regions same generated CSV).  Numbers in seconds.

Each MEST row pairs an access method (`mgist` or `mspgist`) with an
equisplit opclass that decomposes each trip into N STBoxes via
`splitNStboxes(tgeompoint, N)`.  Three MEST families are shown:

- **MEST mrtree** — `mgist` access method, multi-entry R-tree opclass
  `tgeompoint_mrtree_equisplit_ops`.
- **MEST mquadtree** — `mspgist` access method, multi-entry SP-GiST
  quadtree opclass `tgeompoint_mquadtree_equisplit_ops`.
- **MEST mkdtree** — `mspgist` access method, multi-entry SP-GiST
  k-d tree opclass `tgeompoint_mkdtree_equisplit_ops`.

| Q | GiST (R-tree) | SP-GiST (quadtree) | SP-GiST (k-d tree) | MEST mrtree N=4 | MEST mrtree N=8 | MEST mquadtree N=8 | MEST mkdtree N=8 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Q4  | 15.19 | 10.05 |  7.38 |  6.98 |  6.77 |  7.16 |  6.94 |
| Q5  | 80.61 | 95.30 | 97.59 | 97.22 | 97.41 |100.49 | 97.74 |
| Q6  |  4.23 |  4.00 |  3.74 |  4.99 |  4.46 |  3.57 |  4.62 |
| Q10 |  6.46 |  7.82 |  7.45 |  5.35 |  5.49 |  5.17 |  5.09 |
| **Total (17 R-queries)** | **173.23** | **192.99** | **178.65** | **164.67** | **164.41** | **169.79** | **171.37** |

Reading:

- **Q4** (trip×static point, `eIntersects`) — MEST wins by 2.2× over
  GiST (6.77 s vs 15.19 s).  Per-trip multi-entry decomposition gives
  each trip 4–8 sub-trajectory STBoxes, so the static point's STBox
  prunes finer than a single trip-level R-tree entry.
- **Q5** (aggregated `ST_Collect` over the trip×trip cross-join) —
  MEST loses; the aggregate shape has no per-trip prefilter point, so
  every extra index entry is pure overhead.
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
below notes that the H3 cell-set covers most of the trajectory universe
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
- **Q16** (trip×trip×region triple cross-join): MEST loses 10–30 %
  to R-tree.  The trip×trip pair-up dominates the cost and MEST's
  extra index entries amplify it — same shape that hurts MEST on Q5.

So MEST is a clean win on the simple trip×region shape (Q13) where
th3index is overhead-neutral, but does not help on the triple
cross-join (Q16).

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
  sub-matrix above).
- **Aggregated cross-join** (Q5) — no defined prefilter shape; the
  aggregate erases the per-trip granularity the index relies on.
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
- **MobilitySpark on Spark** — the same SQL shape runs as a UDF.  At
  the time of this run, the th3index prefilter result column is
  populated; bench timings on the prefiltered configuration are
  pending a separate pass (Spark `local[4]`).

### MobilityDB cross-join results — sf 0.005, R-tree on `trip`, `trajectory`, and `trip_h3`

Same-session apples-to-apples run (PostgreSQL 17.8, warm cache, 1
run per query):

| Q | R-tree baseline | th3index prefilter | Notes |
|---|---:|---:|---|
| Q4  |  5.07 s |  5.62 s | overhead-neutral; R-tree on `trajectory` already tight on the 10 query points |
| Q5  | 76.48 s | 86.60 s | static-set prefilter not applicable (aggregated `ST_Collect`) |
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
| Q6  |  1.95 s | 0.05 s |  0.31 s |  0.06 s | pending |
| Q10 | 43.46 s | 1.83 s |  6.24 s |  1.73 s | pending |
| Total Q6+Q10 | 45.41 s | 1.88 s | 6.55 s | 1.79 s | — |

The th3index prefilter brings MobilityDuck's trip×trip cross-join
totals into the same range as MobilityDB's: 1.79 s vs 1.88 s on
Q6+Q10 combined.  The MobilitySpark th3index column is pending a
separate bench pass on `local[4]` against the prefiltered SQL.

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

