# BerlinMOD Benchmarks

Per-query latency reports for the BerlinMOD R-queries on each ecosystem
platform. Each report covers a specific configuration matrix (indexes,
prefilters, dataset scale) and is reproducible from the scripts
included alongside it.

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
the per-platform driver instructions for MobilityDB, MobilityDuck, and
MobilitySpark.

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
