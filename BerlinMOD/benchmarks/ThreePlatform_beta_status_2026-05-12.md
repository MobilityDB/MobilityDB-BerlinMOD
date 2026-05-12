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

## h3 prefilter variant (out of scope for the standard beta)

The standard 17-query beta runs without the h3 variant on every
platform.  The h3 variant is in scope on MobilityDB and out of scope
on MobilityDuck and MobilitySpark for this beta.
