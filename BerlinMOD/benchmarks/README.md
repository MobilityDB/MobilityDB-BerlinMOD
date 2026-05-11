# BerlinMOD Benchmarks

Dated benchmark reports for the BerlinMOD chapter-1 query set on each
ecosystem platform.  Each report measures a specific configuration
matrix (indexes, prefilters, dataset scale) and is reproducible from
the script included alongside it.

## Reports

| Date | Platform | Scope | Document | Reproduce |
|---|---|---|---|---|
| 2026-05-11 | MobilityDB / PostgreSQL 17 | Chapter 1 (Q1–Q6) × index matrix (none / GiST / SP-GiST / GiST(trip_h3)) at BerlinMOD scalefactor 0.005 | [`MobilityDB_chapter1_th3index_2026-05-11.md`](MobilityDB_chapter1_th3index_2026-05-11.md) | [`run_bench.sh`](run_bench.sh) |
| 2026-05-11 | Cross-platform readiness | Work needed to replicate the above on MobilityDuck and MobilitySpark | [`CrossPlatform_th3index_readiness_2026-05-11.md`](CrossPlatform_th3index_readiness_2026-05-11.md) | — |

## Naming convention

`<Platform>_<scope>_<topic>_<YYYY-MM-DD>.md`.  Adding a new report does
not invalidate an old one — dated files act as historical record.

The reproduce script alongside each report carries the exact query and
index-configuration matrix used; rerunning it on the same MEOS / MobilityDB
build should reproduce the timings within run-to-run noise.
