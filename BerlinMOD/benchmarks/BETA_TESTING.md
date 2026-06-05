# BerlinMOD ecosystem benchmark — Beta testing guide

Audience: privileged testers running the BerlinMOD benchmark on
MobilityDB, MobilityDuck, and MobilitySpark to validate cross-platform
parity and performance.

This guide is for **testers**.  Engineering details and PR links live
in the bench reports under `BerlinMOD/benchmarks/`.

---

## What to run

For each platform, the benchmark covers the 17 BerlinMOD R-queries
plus the 6 chapter-1 queries.  Two query files exist:

| File | Purpose |
|---|---|
| `berlinmod_r_queries_portable.sql` | Cross-platform reference; same SQL on all three platforms. |
| `berlinmod_r_queries_th3index_portable.sql` | Same queries, with the h3 cell-set prefilter on spatial predicates.  Drop-in replacement for performance comparison. |
| `berlinmod_chapter1_queries_portable.sql` | Existing 6-query subset (textbook reference). |
| `berlinmod_chapter1_queries_th3index_portable.sql` | Chapter-1 + h3 prefilter. |

Per platform:

### MobilityDB / PostgreSQL

```
psql -d <bench-db> -f BerlinMOD/berlinmod_th3index_setup.sql
psql -d <bench-db> -f BerlinMOD/berlinmod_r_queries_portable.sql
psql -d <bench-db> -f BerlinMOD/berlinmod_r_queries_th3index_portable.sql
```

For MobilityDB, the canonical PL/pgSQL driver
`berlinmod_r_queries.sql` (call `SELECT berlinmod_R_queries(1, false);`)
is also available and captures detailed `EXPLAIN ANALYZE` timings into
`execution_tests_explain`.

### MobilityDuck / DuckDB

```
duckdb <bench.db> -c ".read BerlinMOD/berlinmod_r_queries_portable.sql"
duckdb <bench.db> -c ".read BerlinMOD/berlinmod_r_queries_th3index_portable.sql"
```

Capture wall-clock time per statement via DuckDB's `.timer on` directive.

### MobilitySpark / Spark SQL

```
bash berlinmod/bench/bench_mspark.sh                    # defaults to --master local[4]
SPARK_MASTER=local[8] bash berlinmod/bench/bench_mspark.sh  # tune per host
```

The bench harness defaults to `--master local[4]` (validated end-to-
end against MobilityDB#815 + MobilityDB#949 + MobilitySpark#5).
Higher counts work on the C-level threaded smoke test (16 threads ×
20k × 5 = 0 errors) but are not formally validated in the bench
matrix; set `SPARK_MASTER=local[N]` to tune for your host.

Q5 is currently skipped on Spark — a pre-existing `geo_from_text`
parse path crashes the JVM (separate from the thread-safety work).

---

## Expected row counts

All three platforms must return identical row counts.  Reference numbers
(BerlinMOD scalefactor 0.005 — 1620 trips):

| Query | Rows |
|---:|---:|
| Q1 | 72 |
| Q2 | 1 |
| Q3 | 6 |
| Q4 | 80 |
| Q5 | 100 |
| Q6 | 0 |
| Q7 | 26 |
| Q8 | 75 |
| Q9 | 94 |
| Q10 | 21 |
| Q11 | 0 |
| Q12 | 0 |
| Q13 | 278 |
| Q14 | 1 |
| Q15 | 118 |
| Q16 | 2 |
| Q17 | 1 |

Both the standard portable variant and the th3index variant must
produce the **same** row counts — the h3 prefilter is sound.

Any row-count divergence is a test failure and should be reported (see
"How to report" below).

---

## What to measure

For each platform and each query, record:

1. Wall-clock execution time
2. Returned row count
3. (PG only, optional) `EXPLAIN ANALYZE` plan

Three platform comparisons are then:

- **Same rows across all three platforms** → correctness signal.
- **Same plan shape on PG with/without h3 prefilter** → soundness signal.
- **Speedup of h3 prefilter variant vs standard variant** → performance signal.

The maintained reference numbers for MobilityDB are in
`BerlinMOD/benchmarks/MobilityDB_rqueries_2026-05-11.md`.  Sibling reports
for the other two platforms will be added once the corresponding ports
land.

---

## How to report

Open one tracking issue per platform on the corresponding repository:

| Platform | Repo | Issue title (example) |
|---|---|---|
| MobilityDB | `MobilityDB/MobilityDB` | `bench(beta): BerlinMOD R-queries — <tester ID> — <YYYY-MM-DD>` |
| MobilityDuck | `MobilityDB/MobilityDuck` | same |
| MobilitySpark | `MobilityDB/MobilitySpark` | same |

In the issue body, attach:

- Platform version (extension version, JAR version, DuckDB version)
- Hardware (CPU, RAM, disk type)
- BerlinMOD scalefactor used
- Raw output (CSV or table) of the per-query timings and row counts
- Diff vs the reference row counts above

If any query fails to execute, attach the error message.

---

## Common test environments

| Field | Value (reference) |
|---|---|
| Scalefactor | 0.005 (1620 trips) |
| OS | Ubuntu 24.04 |
| MobilityDB | master + PR #815 (merged) + PR #949 (thread-safe MEOS + GEOS reentrant) + PR #944 (consolidated th3index) |
| MobilityDuck | th3index branch (when ready) |
| MobilitySpark | PR #5 (BerlinMOD bench, `local[4]` default) + PR #9 (Th3IndexUDFs, h3 variant) |
| JMEOS | 1.4 (regen against MEOS 1.4) |
| PG version | 17.8 |
| DuckDB version | 1.x with `spatial` extension |
| Spark version | 3.5.x |

If your environment differs materially from the reference, note it in
the tracking issue so the comparison stays apples-to-apples.

---

## Out of scope for beta

- Trip-side h3 prefilter densification (Nyquist sampling between
  instants) is a known open work item.  At BerlinMOD scalefactor 0.005
  with H3 resolution 7, trip-side coverage is sufficient.
- The h3 prefilter's selectivity is workload-dependent.  Treat the
  variants as alternative plans; the absolute speedup figure is not
  the primary signal — correctness is.
