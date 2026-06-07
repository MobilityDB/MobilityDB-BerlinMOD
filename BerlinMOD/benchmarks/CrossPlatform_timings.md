# BerlinMOD three-platform common-basis benchmark (th3index, no indexes)

## What this benchmark measures

Per-query latency of the 17 BerlinMOD R-queries plus the round-trip check (`qrt`)
across three databases that share the same MEOS kernel — **MobilityDB**,
**MobilityDuck**, **MobilitySpark** — on a **common basis**: the temporal
H3-cell prefilter (`trip_h3`) is the same on every engine, and **no spatial
indexes** are built. With indexes off, the th3index prefilter is a plain
columnar scan on all three engines, so the engine is the sole variable.

## Workload

The 17 portable R-queries with the th3index cell-set prefilter
(`everEq(geoToH3IndexSet(region, 7), trip_h3)`) as a sound pruning conjunct
alongside the exact predicate. Query shapes:

| Shape | Queries |
|---|---|
| Relational / scalar | q01, q02, q10, q11, q12, q15 |
| Trip × static (point/region/period) | q04, q08, q13, q14, q16 |
| Trip × trip (N×N) | **q05, q07** |
| Aggregate / cumulative | q03, q09, q17, qrt |

## Dataset, hardware, methodology

500 trips / 776 496 instants (a 500-trip cap of the Danish BerlinMOD generator),
the same `trips.csv` (`tripId, vehId, trip`, where `trip` is hex-EWKB at SRID
3857) loaded into each engine. **`trip_h3` is derived at ingest**, not carried in
the CSV: each engine runs `tgeompoint_to_th3index(transform(trip, 4326), 7)` once
on load. This is an O(1)-per-point conversion through the shared MEOS kernel (the
same vendored libh3), so the cells are byte-identical on every engine — the
prefilter is data, not an engine-derived artifact. **Tier 0**: the trip GiST,
trip SP-GiST, and `trip_h3` GiST indexes are all dropped before the run. One run
per query.

- **MobilityDB** — PostgreSQL 17.8, pin `67fcb0e63c`, built `-DH3=ON`.
- **MobilityDuck** — DuckDB (MEOS via the extension).
- **MobilitySpark** — Spark (MEOS via [JMEOS](https://github.com/MobilityDB/JMEOS)).

## Invariants held fixed

- **Same kernel, same pin** — `67fcb0e63c` provides `tgeompoint_to_th3index` and
  the `th3index` (Hex)WKB serializers on every engine.
- **Common prefilter** — `trip_h3` derived identically at ingest; byte-identical
  cells across engines.
- **No indexes (tier 0)** — every engine scans columnar; the prefilter never
  rides an index.
- **Natural SQL, exact predicates** — the th3index cell-set test is a pruning
  conjunct, never a replacement for the exact predicate.

## Results — execution time (ms, lower is better)

| Query | MobilityDB | MobilityDuck | MobilitySpark |
|---|---:|---:|---:|
| q01 | 12 | — | — |
| q02 | 10 | — | — |
| q03 | 6 513 | — | — |
| q04 | 11 | — | — |
| **q05** (trip × trip) | **121 754** | — | — |
| q06 | 10 | — | — |
| **q07** (trip × trip) | **64 216** | — | — |
| q08 | 1 649 | — | — |
| q09 | 9 495 | — | — |
| q10 | 10 | — | — |
| q11 | 10 | — | — |
| q12 | 10 | — | — |
| q13 | 8 615 | — | — |
| q14 | 7 045 | — | — |
| q15 | 10 | — | — |
| q16 | 1 355 | — | — |
| q17 | 18 372 | — | — |
| qrt | 3 735 | — | — |
| **Total** | **242.8 s** | — | — |

MobilityDuck and MobilitySpark run the same `cap500` dataset, pin, and query
files via [`bench/bench_mduck.sh`](bench/bench_mduck.sh) and
[`bench/bench_mspark.sh`](bench/bench_mspark.sh).

## Reading the results

With no indexes, the two **N×N trip × trip** queries — **q05 (122 s)** and
**q07 (64 s)** — dominate; together they are ~77% of the MobilityDB total. The
relational and trip × static queries are sub-second to a few seconds. The
th3index prefilter prunes the cross-join candidates on a plain scan; the residual
cost is the exact spatial/temporal evaluation on the surviving pairs.

## Reproduce

Build MobilityDB at the pin with H3 enabled, load the `cap500` dataset, and
derive `trip_h3` at ingest. [`run_bench.sh`](run_bench.sh) times each query across
the index-config matrix; the **no-index** row of its matrix drops the trip GiST,
trip SP-GiST, and `trip_h3` GiST indexes and is the tier reported here. Set the
`DB` and `PGBIN` variables at the top of the script to the loaded database.

```bash
cmake -DMEOS=OFF -DH3=ON -DPOSTGRESQL_PG_CONFIG=<pg>/bin/pg_config .. && ninja && ninja install
createdb berlinmod_bench500
psql berlinmod_bench500 -c 'CREATE EXTENSION mobilitydb CASCADE'
psql berlinmod_bench500 -c "ALTER TABLE trips ADD COLUMN trip_h3 th3index;
  UPDATE trips SET trip_h3 = tgeompoint_to_th3index(transform(trip, 4326), 7);"
bash run_bench.sh
```

Pin: `67fcb0e63c` (tag `ecosystem-pin-2026-06-06a`).
