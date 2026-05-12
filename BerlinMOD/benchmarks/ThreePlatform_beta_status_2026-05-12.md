# Three-platform BerlinMOD beta — status 2026-05-12

All three platforms run the 17 R-queries today and the three-way
timing matrix is complete.  Beta testers can launch on any of the
platforms without harnesses or per-thread workarounds.

## What testers can run today

| Platform | Bench driver | Queries | Status |
|---|---|---|---|
| MobilityDB | `psql -d <db> -c "SELECT berlinmod_R_queries(1, false);"` | `BerlinMOD/berlinmod_r_queries.sql` (canonical PL/pgSQL) | Bench matrix in PR #26 |
| MobilityDuck | `duckdb <db>` + schema adapter + portable file | `BerlinMOD/mobilityduck_schema_adapter.sql` + `BerlinMOD/berlinmod_r_queries_portable.sql` | 17 / 17 row-count parity with MobilityDB |
| MobilitySpark | `berlinmod/bench/bench_mspark.sh` — defaults to `--master local[4]` | `MobilitySpark-parity/berlinmod/q01.sql … q17.sql` | 17 / 17 row-count parity with MobilityDB; `local[4]` validated end-to-end |

## Per-query parity (Brussels SF 0.005)

After the deterministic `ORDER BY` fix on the LIMIT-10 parameter
views in `berlinmod_load.sql`, the 17 R-queries return identical
row counts on all three platforms:

| Q | rows | Q | rows |
|---|---:|---|---:|
| Q1 | 72 | Q10 | 21 |
| Q2 | 1 | Q11 | 0 |
| Q3 | 6 | Q12 | 0 |
| Q4 | 80 | Q13 | 278 |
| Q5 | 100 | Q14 | 1 |
| Q6 | 0 | Q15 | 118 |
| Q7 | 26 | Q16 | 2 |
| Q8 | 75 | Q17 | 1 |
| Q9 | 94 | | |

## Beta launch readiness — summary

| Item | MobilityDB | MobilityDuck | MobilitySpark |
|---|---|---|---|
| 17 R-queries runnable end-to-end | ✅ | ✅ | ✅ |
| UDF surface complete (standard) | ✅ | ✅ | ✅ |
| Row-count parity (17/17) | ✅ | ✅ | ✅ |
| Bench driver | ✅ | ✅ | ✅ |
| Multi-threaded run validated | ✅ (Postgres workers) | ✅ (DuckDB threadpool) | ✅ (`local[4]` end-to-end) |
| h3 prefilter variant | ✅ | open: h3 port | open: MobilitySpark PR #9 |

**Beta testers can launch on all three platforms today.**

## Underlying fixes that unblocked MobilitySpark

| MEOS area | PR | What changed |
|---|---|---|
| lwgeom WKT parser + GMT bootstrap + MEOS-owned state TLS | MobilityDB #815 (merged) | Per-thread state for parser globals, timezone bootstrap, errno, GSL RNGs, PROJ context, ways cache |
| GEOS reentrant API + JVM-safe error handler | MobilityDB #949 (ready) | Per-thread `GEOSContextHandle_t` via `geos_get_context()` (MEOS) and `lwgeom_geos_context()` (vendored liblwgeom).  Every GEOS call uses `GEOSXxx_r`.  `meos_initialize_noexit_error_handler` for JVM/JNR consumers. |
| Spark bench harness defaults | MobilitySpark #5 | `bench_mspark.sh` defaults to `--master local[4]` with `SPARK_MASTER` env override; `MeosThread.MEOS_READY` relies on `meos_initialize` for the per-thread GEOS context. |

## Open work (h3 prefilter variant — optional, post-standard-beta)

1. **MobilityDuck th3index port** — ~4–5 person-days; unblocks the
   h3 prefilter variant on DuckDB.
2. **MobilitySpark PR #9** — h3 prefilter port; CI gates on JMEOS regen.

The standard 17-query beta runs without the h3 variant on every
platform.
