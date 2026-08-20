# BerlinMOD batch SQL benchmark

Per-query latency reports for the BerlinMOD R-queries on MobilityDB, MobilityDuck,
and MobilitySpark. Each report covers a specific configuration matrix (indexes,
prefilters, dataset scale) and is reproducible from the scripts included alongside it.

The cross-platform q01–q17 + qrt reproducer that generated
[`CrossPlatform_timings.md`](CrossPlatform_timings.md) and the
`cross_platform_*.svg` figures lives in **[`bench/`](bench/)**.

## Reports

| Platform | Scope | Document | Reproduce |
|---|---|---|---|
| MobilityDB / PostgreSQL 17 | 17 R-queries × index matrix (none / GiST / SP-GiST), BerlinMOD scalefactor 0.005 | [`MobilityDB_rqueries.md`](MobilityDB_rqueries.md) | [`run_full_bench.sh`](run_full_bench.sh) |
| Three-platform | All 17 R-queries + qrt, th3index prefilter, no indexes | [`CrossPlatform_timings.md`](CrossPlatform_timings.md) | per-platform drivers |

## Reproducing a run

[`REPRODUCE.md`](REPRODUCE.md) lists the query files, expected row counts, and
the per-platform driver instructions for MobilityDB, MobilityDuck, and MobilitySpark.

## Three-tier index framework

The same canonical queries run on every platform; what varies across tiers is
which acceleration structures are active. This isolates the contribution of the
portable `th3index` cell-set prefilter from that of a native spatial index.

| Tier | Acceleration active | `bench_mbdb.sh` (PostgreSQL) | `bench_mduck.sh` (DuckDB) | `bench_mspark.sh` (Spark) |
|---|---|---|---|---|
| 0 | none — the prefilter column is scanned | drops the trip GiST/SP-GiST **and** the `trip_h3` GiST | — | — |
| 1 | `th3index` prefilter only | drops the native trip indexes, keeps `trip_h3` GiST | loader default; no TRTREE created | the only tier Spark runs |
| 2 | native spatial index only | drops the `trip_h3` GiST, keeps trip GiST + SP-GiST | creates TRTREE on `Trip`, NULLs `trip_h3` | — |
| 3 | both (default, production-realistic) | all indexes active — the loader default | TRTREE on `Trip` **and** `trip_h3` | — |

Tier 0 is the common basis with Spark: no platform has a native spatial index,
so the numbers are directly comparable. Tier 3 is what a production deployment
looks like, and is the default for both `--tier`-aware runners.

Spark exposes no `--tier` flag — native spatial indexes are a PostgreSQL/DuckDB
capability, so Spark always runs at Tier 1.

DuckDB Tiers 2 and 3 need a MobilityDuck build whose TRTREE index can be
serialized (MobilityDuck #285). On an older build, `CREATE INDEX … USING TRTREE`
on a file-backed database fails at commit time, and the runner degrades to
Tier 1 rather than aborting the run.

## NxN mitigations on Spark

Four queries (Q5, Q6, Q10, Q16) join `Trips` against `Trips`. PostgreSQL and
DuckDB answer these with a spatial index, so the portable form is already
optimal there and both always run `<query>.sql` unchanged.

Spark has no such index, and the portable form degenerates into an NxN
comparison. Where a Spark-optimised variant (`q05_spark.sql` and friends —
UNNEST plus an equi-join) is present, `BerlinMODBench` prefers it over the
portable form; otherwise the portable query is used as-is. These variants ship
with the Spark runner rather than with this directory.

## Portable query files

The benchmarks consume the portable query SQL files in `BerlinMOD/`:

| File | Scope |
|---|---|
| `berlinmod_r_queries_portable.sql` | Full 17 R-queries, portable dialect |
| `berlinmod_r_queries_th3index_portable.sql` | R-queries with the th3index cell-set prefilter |
| `berlinmod_chapter1_queries_portable.sql` | Textbook 6-query subset, portable dialect |
| `berlinmod_chapter1_queries_th3index_portable.sql` | Chapter 1 with the th3index cell-set prefilter |

The non-portable canonical files (`berlinmod_chapter1_queries.sql`,
`berlinmod_r_queries.sql`) remain for the textbook record.
