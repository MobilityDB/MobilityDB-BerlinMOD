# BerlinMOD benchmark — reproduction guide

---

## What to run

The benchmark covers the 17 BerlinMOD R-queries in two variants:

| File | Purpose |
|---|---|
| `berlinmod_r_queries_portable.sql` | Cross-platform reference; runs on all three platforms. |
| `berlinmod_r_queries_th3index_portable.sql` | Same queries with the th3index cell-set prefilter on spatial predicates. |
| `berlinmod_chapter1_queries_portable.sql` | Textbook 6-query subset, portable dialect. |
| `berlinmod_chapter1_queries_th3index_portable.sql` | Chapter-1 queries with the th3index prefilter. |

Both variants return identical row counts — the cell-set prefilter is sound (no false negatives).

### MobilityDB / PostgreSQL

```bash
psql -d <bench-db> -f BerlinMOD/berlinmod_th3index_setup.sql
psql -d <bench-db> -f BerlinMOD/berlinmod_r_queries_portable.sql
psql -d <bench-db> -f BerlinMOD/berlinmod_r_queries_th3index_portable.sql
```

The canonical PL/pgSQL driver captures `EXPLAIN ANALYZE` timings per query:

```sql
SELECT berlinmod_R_queries(1, false);
```

### MobilityDuck / DuckDB

```bash
duckdb <bench.db> -c ".timer on" -c ".read BerlinMOD/berlinmod_r_queries_portable.sql"
duckdb <bench.db> -c ".timer on" -c ".read BerlinMOD/berlinmod_r_queries_th3index_portable.sql"
```

### MobilitySpark / Spark SQL

```bash
bash BerlinMOD/benchmarks/batch/bench/bench_mspark.sh
```

The bench harness defaults to `--master local[4]`; set `SPARK_MASTER=local[N]` to tune for your host.

---

## Expected row counts

Reference numbers for BerlinMOD scalefactor 0.005 (1620 trips):

| Query | Rows |
|---:|---:|
| Q1  | 72  |
| Q2  | 1   |
| Q3  | 6   |
| Q4  | 80  |
| Q5  | 100 |
| Q6  | 0   |
| Q7  | 26  |
| Q8  | 75  |
| Q9  | 94  |
| Q10 | 21  |
| Q11 | 0   |
| Q12 | 0   |
| Q13 | 278 |
| Q14 | 1   |
| Q15 | 118 |
| Q16 | 2   |
| Q17 | 1   |

Any row-count divergence from the reference is a test failure.

---

## What to measure

For each platform and each query, record:

1. Wall-clock execution time
2. Returned row count
3. (PG only, optional) `EXPLAIN ANALYZE` plan

Comparisons of interest:

- **Same rows across platforms** — correctness signal.
- **Same rows for standard vs th3index variant** — prefilter soundness.
- **Speedup of th3index variant vs standard variant** — performance signal.

Reference MobilityDB numbers are in [`MobilityDB_rqueries.md`](MobilityDB_rqueries.md).

---

## Reference environment

| Field | Value |
|---|---|
| BerlinMOD scalefactor | 0.005 (1620 trips) |
| OS | Ubuntu 24.04 |
| MobilityDB | current master, built `-DH3=ON` |
| MobilityDuck | current master |
| MobilitySpark | current master |
| PostgreSQL | 17.8 |
| H3 resolution | 7 (cell edge ≈ 1.2 km) |
