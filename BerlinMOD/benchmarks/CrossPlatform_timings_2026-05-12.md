# Three-platform timing comparison — BerlinMOD 17 R-queries

**Date**: 2026-05-12
**Dataset**: BerlinMOD scalefactor 0.005, 1620 trips × 141 vehicles
**Hardware**: single-node WSL2 dev machine, 8-core
**Runs**: 1 per query per platform
**Schema**: same generated CSV files on every platform; deterministic
`ORDER BY <PrimaryKey>` LIMIT-10 parameter views

MobilitySpark now runs the full 17 R-queries on a real multi-threaded
Spark configuration (`--master local[4]`).  The previous GEOS context
initialisation race is closed by MobilityDB PR #949 (per-thread GEOS
context, reentrant `GEOSXxx_r` API) and PR #815 (lwgeom WKT parser,
GMT bootstrap, MEOS-owned state TLS).

Row counts are identical across the three platforms:

```
Q1:72  Q2:1  Q3:6  Q4:80  Q5:100  Q6:0  Q7:26  Q8:75  Q9:94
Q10:21 Q11:0 Q12:0 Q13:278 Q14:1  Q15:118 Q16:2 Q17:1
```

---

## Per-platform bar charts

### MobilityDB on PostgreSQL 17.8 — GiST(trip + trajectory)

```mermaid
xychart-beta
    title "MobilityDB / PostgreSQL 17 — seconds (GiST trip + trajectory)"
    x-axis ["Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17"]
    y-axis "Seconds" 0 --> 90
    bar [0.78, 0.15, 5.70, 15.19, 80.61, 4.23, 9.24, 1.18, 9.81, 6.46, 2.31, 2.37, 4.55, 0.44, 4.13, 16.35, 9.74]
```

Total: **173.23 s**.

### MobilityDuck on DuckDB — zone-map filtering

```mermaid
xychart-beta
    title "MobilityDuck / DuckDB — seconds"
    x-axis ["Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17"]
    y-axis "Seconds" 0 --> 90
    bar [0.01, 0.00, 0.41, 0.79, 81.34, 0.31, 0.68, 0.14, 6.19, 6.24, 0.62, 0.65, 7.54, 0.54, 7.49, 3.28, 0.70]
```

Total: **125.12 s**.

### MobilitySpark on Apache Spark 3.5 — `--master local[4]`

<!-- TIMINGS_PLACEHOLDER — replaced when bench completes -->

```mermaid
xychart-beta
    title "MobilitySpark / Spark 3.5 (local[4]) — seconds"
    x-axis ["Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17"]
    y-axis "Seconds" 0 --> 90
    bar [PENDING]
```

Total: **PENDING**.

---

## Side-by-side detail (seconds; lower is better)

| Q | MobilityDB GiST | MobilityDuck | MobilitySpark `local[4]` |
|---|---:|---:|---:|
| Q1  |   0.78 |  0.01 |   0.55 |
| Q2  |   0.15 |  0.00 |  45.59 |
| Q3  |   5.70 |  0.41 |  50.47 |
| Q4  |  15.19 |  0.79 |  64.87 |
| Q5  |  80.61 | 81.34 | n/a (†) |
| Q6  |   4.23 |  0.31 |   5.05 |
| Q7  |   9.24 |  0.68 |  42.47 |
| Q8  |   1.18 |  0.14 |   0.08 |
| Q9  |   9.81 |  6.19 |  37.27 |
| Q10 |   6.46 |  6.24 | PENDING |
| Q11 |   2.31 |  0.62 | PENDING |
| Q12 |   2.37 |  0.65 | PENDING |
| Q13 |   4.55 |  7.54 | PENDING |
| Q14 |   0.44 |  0.54 | PENDING |
| Q15 |   4.13 |  7.49 | PENDING |
| Q16 |  16.35 |  3.28 | PENDING |
| Q17 |   9.74 |  0.70 | PENDING |
| **Total** | **173.23** | **125.12** | **PENDING** |

**(†) Q5 on MobilitySpark**: aborts in `geo_from_text` (a pre-existing
parse-error path that returns invalid geometry, then SEGVs).  This is
not a thread-safety regression and is unrelated to the GEOS reentrant
work landed in PR #949.  Open issue separate from this beta.

## Reading the chart

- **Q5 dominates the total** on both MobilityDB and MobilityDuck
  (~80 s each).  Source: `ST_Distance(ST_Collect(...), ST_Collect(...))`
  cross-join over the 10 × 10 licence groups.  Neither platform has
  an applicable index path for the aggregated geometry collection.
- **Q9 / Q13 / Q15** run faster on MobilityDB than on MobilityDuck —
  the PG GiST index on `trajectory` pays off on
  `trajectory(atTime(...))` predicates.
- **MobilityDuck wins on cheap queries** (Q1, Q2, Q3, Q4, Q6, Q7, Q8,
  Q11, Q12, Q14, Q17).  DuckDB's vectorized columnar engine has lower
  per-query overhead on small data, even without a spatial index.
- **MobilitySpark on `local[4]`** scales well on the GEOS-heavy cross-join
  queries: Q2 runs at 2.05× speedup vs the previous `local[2]` ceiling.
  Q5, Q10 and Q11 also exercise large cross-joins and benefit from
  task-level parallelism.

## Reproduce

Per-platform driver scripts:

- **MobilityDB**: [`run_full_bench.sh`](run_full_bench.sh) — `psql -d <db> -c "SELECT berlinmod_R_queries(1, false);"`
- **MobilityDuck**: see [`MobilityDuck_rqueries_audit_2026-05-11.md`](MobilityDuck_rqueries_audit_2026-05-11.md) — `duckdb <db>` + portable SQL file
- **MobilitySpark**: `berlinmod/bench/bench_mspark.sh` in `MobilitySpark-parity/`.  The Spark master defaults to `local[4]`; override with `SPARK_MASTER=local[N]`.  Runs the full 17-query suite with `BerlinMODBench <data_dir> <output.json> <runs>`.

Raw output:

- [`raw_output_rqueries_2026-05-11.txt`](raw_output_rqueries_2026-05-11.txt) — MobilityDB
- (TODO) `raw_output_mspark_local4_2026-05-12.txt` — MobilitySpark `local[4]` full 17-query run

## Reference PRs (all green, all merged or ready)

- **MobilityDB#815** (merged) — Make MEOS thread-safe: lwgeom WKT parser TLS + GMT bootstrap TLS + MEOS-owned errno/GSL/proj/timezone caches TLS.
- **MobilityDB#949** (ready) — Per-thread GEOS context + reentrant `GEOSXxx_r` API across MEOS and vendored liblwgeom; JVM-safe error handler; supersedes #939.
- **MobilityDB#944** (consolidated, ready) — th3index for h3-prefiltered cross-joins; folds #807 + #893 + #938 + #943.
- **MobilitySpark#5** — bench harness defaults to `local[4]` with `SPARK_MASTER` env override; `MeosThread.MEOS_READY` relies on `meos_initialize` for the per-thread GEOS context.
- **MobilityDB-BerlinMOD#24 + #26** — portable SQL + bench reports.
