# Cross-platform readiness — BerlinMOD 17 R-queries benchmark

Sibling of `MobilityDB_rqueries_2026-05-11.md` (the MobilityDB-only 17-query
matrix).  Inventory of what each platform needs to reproduce the same
benchmark and converge on the same row counts.

The chapter-1 readiness assessment
(`CrossPlatform_th3index_readiness_2026-05-11.md`) covered four of these
queries.  The full 17-query suite uses a broader function surface and
requires a portable harness to replace the PL/pgSQL driver.

---

## Function / operator surface required

### MEOS temporal — used by R1–R17

| Function | Used by | Status on MobilityDuck | Status on MobilitySpark |
|---|---|---|---|
| `atTime(tgeompoint, tstzspan)` | Q8, Q9, Q13, Q15, Q16 | registered | UDF in DistanceUDFs |
| `atValues(tgeompoint, geometry)` | Q7 | registered | UDF |
| `valueAtTimestamp(tgeompoint, timestamptz)` | Q3, Q11, Q12, Q14 | registered | UDF |
| `trajectory(tgeompoint)` | Q4, Q5, Q7, Q13, Q15, Q16, Q17 | registered | UDF in GeoUDFs |
| `length(tgeompoint)` | Q8, Q9 | registered | UDF |
| `startTimestamp(tgeompoint)` | Q7 | registered | UDF in AccessorUDFs |
| `stbox(tgeompoint)` | Q11, Q12, Q13, Q14, Q15, Q16 | registered | UDF |
| `eDwithin(tgeompoint, geometry, float)` | Q6 | registered | UDF |
| `tDwithin(tgeompoint, tgeompoint, float)` | Q10 | **not yet registered** | UDF in DistanceUDFs |
| `whenTrue(tbool)` | Q10 | **not yet registered** | **not yet registered** |
| `expandSpace(stbox, float)` | Q6, Q10 | registered | UDF |
| `aDisjoint(tgeompoint, geometry)` | Q16 | registered | UDF |

### PostGIS spatial

| Function | Used by | Status on MobilityDuck | Status on MobilitySpark |
|---|---|---|---|
| `ST_Intersects(geom, geom)` | Q4, Q7, Q13, Q15, Q16, Q17 | DuckDB `spatial` extension | sedona / fallback UDF |
| `ST_Contains(geom, geom)` | Q14 | DuckDB `spatial` extension | sedona / fallback UDF |
| `minDistance(tgeompoint[], tgeompoint[])` | Q5 | MobilityDuck native | MobilitySpark native |

### PG operators

| Operator | Used by | Portable equivalent |
|---|---|---|
| `t.Trip && stbox(…)` (bbox overlap) | Q11–Q15 | `eIntersects(t.Trip, stbox)` or `overlaps_stbox(stbox(t.Trip), …)` |
| `t.Trip @> point` (contains) | Q3, Q11, Q12 | `eContains(t.Trip, point)` or `valueAtTimestamp` check |
| `length(t.Trip) <-> point` (distance) | distance ordering | function-call form available |

### PG harness (not portable as-is)

| PG-specific | Cross-platform substitute |
|---|---|
| `PL/pgSQL` driver `berlinmod_R_queries(times, detailed)` | Shell or Python wrapper that runs each query with timing |
| `EXPLAIN (ANALYZE, FORMAT JSON)` | DuckDB `EXPLAIN ANALYZE` (different output) / Spark `EXPLAIN COST` / wall-clock around the SELECT |
| `clock_timestamp()`, `make_interval()`, `RAISE INFO` | Driver-side |
| `execution_tests_explain` results table | Driver-collected JSON / CSV |

---

## What's missing per platform

### MobilityDuck

**Already in place**
- Temporal / geo parity inventory shows full coverage for the chapter-1
  function surface (per `project_mobilityduck_full_parity_inventory.md`,
  35 files / 1186 assertions passing).
- DuckDB's `spatial` extension provides the PostGIS functions used by
  R-queries.

**Required for R-queries**
| Item | Estimate | Notes |
|---|---|---|
| Register `tDwithin(tgeompoint, tgeompoint, float)` | 0.5 day | UDF wrapper; the spatial-temporal equivalent of eDwithin already exists. |
| Register `whenTrue(tbool)` | 0.5 day | Returns the tstzspan(set) when the tbool is true.  Needed only by Q10. |
| Portable R-queries SQL (no PL/pgSQL) | 1–2 days | Extract 17 inner `SELECT`s from `berlinmod_r_queries.sql`; rewrite PG operators to function-call form (`&&` → `overlaps`, `@>` → `valueAtTimestamp` check, etc.). |
| Bench driver (shell/Python) | 0.5 day | Loop over the 17 queries, run each via DuckDB CLI, capture wall-clock, JSON results. |
| Chapter-1 readiness items not yet done | 4–5 days (carry-over) | th3index registration, h3_latlng_to_cell, geoToH3IndexSet — same list as the chapter-1 readiness document. |

**Estimated incremental over chapter 1**: ~3 person-days.
**Estimated standalone (chapter 1 not done first)**: ~7–8 person-days.

### MobilitySpark

**Already in place**
- PR #5 / PR #9 have full UDF parity for the chapter-1 function surface
  plus Th3IndexUDFs (per `project_mobilityspark_th3index_port_plan.md`).
- `DistanceUDFs`, `GeoUDFs`, `AccessorUDFs` together cover all 12 MEOS
  temporal functions in the R-query surface.
- `local[2]` correctness floor remains (per
  `feedback_mobilityspark_local2_constraint.md`).
- BerlinMOD `bench.sh` driver and `BerlinMODBench.java` exist but
  currently only run the chapter-1 query subset.

**Required for R-queries**
| Item | Estimate | Notes |
|---|---|---|
| Register `tDwithin(tgeompoint, tgeompoint, float)` | already in DistanceUDFs (verify exposure) | Q10 dependency. |
| Register `whenTrue(tbool)` | 0.5 day | New UDF in TemporalUDFs.  Q10 only. |
| Portable R-queries Spark SQL | 1–2 days | Mostly identical to the MobilityDuck portable variant; Spark SQL differs from DuckDB on a few syntactic points (no infix custom operators — already known). |
| Bench driver in `BerlinMODBench.java` | 0.5 day | Extend the existing Java driver to dispatch all 17 queries with per-query timing. |
| JMEOS regen against latest MEOS | in flight (parallel session) | Brings PR #940 lift fix and PR #938 polygon-coverage fix to the jar. |
| Chapter-1 readiness items not yet done | 1.5 days (carry-over) | PR #9 CI unblock + `preprocessForSpark` cleanup. |

**Estimated incremental over chapter 1**: ~3 person-days.
**Estimated standalone**: ~4.5 person-days.

---

## Shared work (write once, both platforms consume)

The biggest item is **a portable R-queries SQL file**.  It belongs in
`MobilityDB-BerlinMOD/BerlinMOD/`, sibling of
`berlinmod_chapter1_queries_portable.sql`:

```
BerlinMOD/
├── berlinmod_r_queries.sql                          ← unchanged (PG / PL/pgSQL)
├── berlinmod_r_queries_portable.sql                 ← NEW (target this PR)
└── berlinmod_r_queries_th3index_portable.sql        ← NEW (th3index prefilter variant)
```

Translation rules (mechanical):

| PG | Portable |
|---|---|
| `t.Trip && stbox(P.Geom, …)` | `eIntersects(t.Trip, stbox(P.Geom, …))` |
| `t.Trip @> P.Geom` | `valueAtTimestamp(t.Trip, i.Instant) = P.Geom` (Q3/Q11/Q12 use it for instant containment, not full-trajectory containment) |
| `length(t.Trip) <->` ... | function-call distance |
| `_ST_Intersects` | `ST_Intersects` (drop the underscore-prefixed bbox-bypass form) |

Plus inject the h3 prefilter clause per spatial query in the
`_th3index_portable.sql` variant:

```sql
WHERE everIntersectsH3IndexSet_Th3Index(
        geoToH3IndexSet(R.Geom, 7), T.trip_h3)
  AND <semantic predicate>
```

---

## Sequencing

```
                    MobilityDB R-queries matrix
                    (this run, in progress)
                                │
                                ▼
                   Portable R-queries SQL  ← write once
                   (mostly mechanical port)
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
         MobilityDuck                    MobilitySpark
         • register 2 UDFs              • register 1 UDF (whenTrue)
           (tDwithin temporal,            + verify tDwithin
            whenTrue)
         • shell/Python driver           • extend BerlinMODBench.java
         • run matrix                    • run matrix
                                │
                                ▼
                Three-platform full 17-query
                comparison (apples-to-apples)
```

## Tracking pointers

- MobilityDB **PR #938** — open — bench-driving branch with the sound
  polygon prefilter.
- MobilityDB **PR #940** — open — lift framework helper.
- MobilityDB-BerlinMOD **PR #24** — open — shared CSV ships `trip_h3`
  + chapter-1 th3index-variant SQL.
- MobilityDB-BerlinMOD **PR #26** — open — bench docs.
- MobilitySpark **PR #9** — open — Th3IndexUDFs.
- MobilityDuck th3index port — not yet filed; needed for chapter 1
  and for the R-query matrix.
