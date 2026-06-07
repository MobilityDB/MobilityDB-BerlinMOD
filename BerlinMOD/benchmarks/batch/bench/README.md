# Cross-Platform BerlinMOD Runner

This directory is the single source of truth for the cross-platform
q01–q17 + qrt benchmark that produced the timings and bar charts in
[`CrossPlatform_timings.md`](../CrossPlatform_timings.md) and the
companion `cross_platform_*.svg` figures.

## Files

| File | Purpose |
|---|---|
| `bench.sh` | Top-level driver: runs all three platforms in sequence and writes `results/` |
| `bench_mbdb.sh` | MobilityDB / PostgreSQL per-platform runner |
| `bench_mduck.sh` | MobilityDuck / DuckDB per-platform runner |
| `bench_mspark.sh` | MobilitySpark / Apache Spark per-platform runner |
| `queries.sql` | Canonical query file — 18 queries (q01–q17 + qrt) delimited by `-- @query <id>` markers; runners split on the marker |
| `report.py` | Reads `results/` JSON and renders the timing tables and SVG bar charts |

## Canonical measurement conditions

- **MEOS pin**: `278863520b` (tag `ecosystem-pin-2026-06-06g`) — all three
  platforms were built and measured against this exact MEOS commit.
- **Dataset**: 500-trip cap (`berlinmod-3db-cap500`), th3index cell-set
  prefilter enabled, no spatial index on the trip column.

## Sibling benchmarks (separate, not this reproducer)

The top-level scripts one directory up are independent benchmarks with a
different scope:

| Script | Scope |
|---|---|
| `../run_bench.sh` | MobilityDB only — Chapter 1 (Q1–Q6) × index matrix (none / GiST / SP-GiST / GiST+h3) at scalefactor 0.005 |
| `../run_full_bench.sh` | MobilityDB only — 17 R-queries × index matrix at scalefactor 0.005 |

These two scripts are not the cross-platform reproducer and do not use
`queries.sql` from this directory.
