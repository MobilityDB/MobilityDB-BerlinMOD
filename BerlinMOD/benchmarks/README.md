# BerlinMOD Benchmarks

Dated benchmark reports for the BerlinMOD query sets on each ecosystem
platform.  Each report measures a specific configuration matrix
(indexes, prefilters, dataset scale) and is reproducible from the
script included alongside it.

## Reports

| Date | Platform | Scope | Document | Reproduce |
|---|---|---|---|---|
| 2026-05-11 | MobilityDB / PostgreSQL 17 | Chapter 1 (Q1–Q6) × index matrix (none / GiST / SP-GiST / GiST(trip_h3)) at BerlinMOD scalefactor 0.005 | [`MobilityDB_chapter1_th3index_2026-05-11.md`](MobilityDB_chapter1_th3index_2026-05-11.md) | [`run_bench.sh`](run_bench.sh) |
| 2026-05-11 | MobilityDB / PostgreSQL 17 | 17 R-queries × index matrix (none / GiST / SP-GiST) at BerlinMOD scalefactor 0.005 | [`MobilityDB_rqueries_2026-05-11.md`](MobilityDB_rqueries_2026-05-11.md) | [`run_full_bench.sh`](run_full_bench.sh) |
| 2026-05-11 | Cross-platform readiness | Chapter 1 — work for MobilityDuck and MobilitySpark | [`CrossPlatform_th3index_readiness_2026-05-11.md`](CrossPlatform_th3index_readiness_2026-05-11.md) | — |
| 2026-05-11 | Cross-platform readiness | R-queries — work for MobilityDuck and MobilitySpark | [`CrossPlatform_rqueries_readiness_2026-05-11.md`](CrossPlatform_rqueries_readiness_2026-05-11.md) | — |
| 2026-05-11 | MobilityDuck | R-queries UDF audit — all required functions registered; supersedes the MobilityDuck function-gap entries in the readiness doc | [`MobilityDuck_rqueries_audit_2026-05-11.md`](MobilityDuck_rqueries_audit_2026-05-11.md) | — |
| 2026-05-11 | MobilitySpark | R-queries UDF audit — all required functions registered on mainline; th3index variant on PR #9 (CI-blocked on JMEOS regen) | [`MobilitySpark_rqueries_audit_2026-05-11.md`](MobilitySpark_rqueries_audit_2026-05-11.md) | — |

## Beta testing

[`BETA_TESTING.md`](BETA_TESTING.md) is the entry point for privileged
testers running the benchmark on any of the three platforms.  It lists
the query files, expected row counts, and the report-back template.

## Naming convention

`<Platform>_<scope>_<YYYY-MM-DD>.md`.  Adding a new report does not
invalidate an old one — dated files act as historical record.

The reproduce script alongside each report carries the exact query and
index-configuration matrix used; rerunning it on the same MEOS /
MobilityDB build should reproduce the timings within run-to-run
noise.

## Portable query files

The benchmarks consume the portable query SQL files that live one
directory up, in `BerlinMOD/`:

| File | Scope |
|---|---|
| `berlinmod_chapter1_queries_portable.sql` | Textbook 6-query subset, portable dialect |
| `berlinmod_chapter1_queries_th3index_portable.sql` | Chapter 1 with h3 cell-set prefilter |
| `berlinmod_r_queries_portable.sql` | Full 17 R-queries, portable dialect |
| `berlinmod_r_queries_th3index_portable.sql` | R-queries with h3 cell-set prefilter |

The non-portable canonical files (`berlinmod_chapter1_queries.sql`,
`berlinmod_r_queries.sql`) remain unchanged for the textbook record.
