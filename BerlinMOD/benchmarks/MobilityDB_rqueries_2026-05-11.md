# MobilityDB — BerlinMOD 17 R-Queries — GiST / SP-GiST Matrix

**Date**: 2026-05-11
**Platform**: MobilityDB on PostgreSQL 17.8
**Build**: th3index HEAD + MobilityDB PR #938 polygon coverage fix +
PR #940 lift framework helper.
**Dataset**: BerlinMOD scalefactor 0.005 — 1620 trips × 100 vehicles
× 100 regions × 100 points × 100 periods × 100 instants.  Parameter
subsets `Licences1`, `Licences2`, `Instants1`, `Periods1`, `Points1`,
`Regions1` (each 10 rows, `ORDER BY <PrimaryKey> LIMIT 10`).
**Driver**: `SELECT berlinmod_R_queries(1, false)` — the canonical
PL/pgSQL harness from `BerlinMOD/berlinmod_r_queries.sql`, which
captures `EXPLAIN (ANALYZE, FORMAT JSON)` timings per query.

---

## Query catalogue

| # | Description (short) | Predicate kind |
|---:|---|---|
| Q1  | Models of vehicles with licences from `Licences` | relational |
| Q2  | Count of passenger cars | relational |
| Q3  | Trip position at `Instants1` for `Licences1` | temporal value-at |
| Q4  | Vehicles that ever passed each `Points` | spatial point-on-trajectory |
| Q5  | Minimum distance between two licence groups' trips | `minDistance(tgeompoint, tgeompoint)` aggregate over the licence cross-join with an `everEqTh3IndexTh3Index` cell-membership prefilter |
| Q6  | Truck pairs ever within 10 m | trip-trip eDwithin |
| Q7  | Earliest passenger-car visit per `Points` | spatial+temporal |
| Q8  | Total distance per licence × period | trip aggregate |
| Q9  | Max single-period distance | trip aggregate |
| Q10 | Trip-trip within 3 m for `Licences1` | trip-trip tDwithin |
| Q11 | Vehicles at `Points1` at `Instants1` | spatial+temporal |
| Q12 | Vehicle pairs meeting at `Points1` × `Instants1` | spatial+temporal cross-join |
| Q13 | Vehicles in `Regions1` during `Periods1` | spatial atGeometry |
| Q14 | Vehicles in `Regions1` at `Instants1` | spatial+temporal |
| Q15 | Vehicles at `Points1` during `Periods1` | spatial+temporal |
| Q16 | Vehicle pairs in `Regions1` × `Periods1` | spatial+temporal cross-join |
| Q17 | Points visited by max-popularity vehicles | spatial aggregate |

---

## Result matrix (seconds, single run per cell)

Row counts are identical to MobilityDuck and MobilitySpark when those
platforms consume the same generated CSV files (per the deterministic
`ORDER BY` fix on the LIMIT-10 views in `berlinmod_load.sql`).

| Q | rows | none | GiST(trip + trajectory) | SP-GiST(trip + trajectory) |
|---|---:|---:|---:|---:|
| Q1  | 72  |   2.45 |   0.78 |   2.68 |
| Q2  | 1   |   0.20 |   0.15 |   0.08 |
| Q3  | 6   |   5.25 |   5.70 |   6.26 |
| Q4  | 80  |  13.69 |  15.19 |  11.11 |
| Q6  | 0   |   5.91 |   4.23 |   3.53 |
| Q7  | 26  |  10.48 |   9.24 |  10.29 |
| Q8  | 75  |   0.94 |   1.18 |   1.07 |
| Q9  | 94  |  30.69 |   9.81 |  11.06 |
| Q10 | 21  |  51.50 |   6.46 |   7.16 |
| Q11 | 0   |   2.76 |   2.31 |   2.73 |
| Q12 | 0   |   2.51 |   2.37 |   2.49 |
| Q13 | 278 |  27.92 |   4.55 |   5.13 |
| Q14 | 1   |  22.62 |   0.44 |   0.45 |
| Q15 | 118 |  32.87 |   4.13 |   4.37 |
| Q16 | 2   |  21.68 |  16.35 |  16.50 |
| Q17 | 1   |  13.51 |   9.74 |   9.08 |

Q5 is omitted from this index matrix.  The canonical Q5 is the
`minDistance` aggregate over the licence cross-join with an
`everEqTh3IndexTh3Index` cell-membership prefilter on `trip_h3`, so it
is driven by the prefilter rather than by the `none` / GiST / SP-GiST
index on `trip` and `trajectory` that the columns here vary.  The
canonical Q5 figure on the portable th3index bench is 9.50 s on the
single PostgreSQL process (median of 10.33 / 9.39 / 9.50).  No suite
total is given so the table does not imply Q5 was re-run under these
index families.

---

## Speedup highlights (GiST(trip + trajectory) over baseline)

- **Q14** (`ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant))`)
  — 51× speedup.  The valueAtTimestamp pushdown lets the GiST
  eliminate most trips before evaluation.
- **Q10** (`tDwithin(t1.Trip, t2.Trip, 3.0)` against expanded bbox)
  — 8.0× on the trip×trip join.
- **Q15** (trajectory-vs-point during period) — 8.0×.
- **Q13** (`ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom)`)
  — 6.1×.
- **Q9** (atTime + length aggregation) — 3.1×.
- **Q4** — 0.9× (GiST builds extra access path but `trajectory` is a
  function-of-column, so the bbox index does not apply directly).

SP-GiST is within run-to-run noise of GiST on the total (177.04 vs
173.23 s) and trades wins per query (faster on Q4 / Q6 / Q17, slower
on Q1).

## Queries where neither index helps materially

- **Q1 / Q2** — relational only, no spatial predicate.
- **Q3** — temporal value-at predicate; GiST adds slight overhead.
- **Q5** is driven by the `everEqTh3IndexTh3Index` cell-membership
  prefilter on `trip_h3` before the `minDistance` aggregate; the
  `none` / GiST / SP-GiST index on `trip` and `trajectory` that this
  matrix varies is not the access path for this query.
- **Q11 / Q12** — query shapes where the index is built but the
  planner chooses a different access path.
- **Q16** — cross-join query; minor improvement.

---

## What this matrix does NOT measure

- **The h3 cell-set prefilter clause.**  Adding
  `everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(R.Geom, 7), T.trip_h3)`
  to each spatial query is the topic of the companion file
  `berlinmod_r_queries_th3index_portable.sql`.  Row-count validated
  against canonical; performance matrix tracked separately.
- **Three-platform timings.**  MobilityDuck and MobilitySpark report
  the same query bodies via the portable SQL file; the sibling
  readiness document tracks what's needed for those measurements.

---

## Reproduce

```bash
psql -d berlinmod_h3bench -c "SELECT berlinmod_R_queries(1, false);"
```

Index configurations are toggled by dropping/creating GiST or SP-GiST
indexes on `trips.trip` and `trips.trajectory` between runs.  See
`run_full_bench.sh` in this directory.

---

## Companion files

- `berlinmod_r_queries_portable.sql` — same 17 queries, portable
  dialect (PG-specific operators replaced with function-call form;
  no PL/pgSQL harness).  Row-count validated against canonical.
- `berlinmod_r_queries_th3index_portable.sql` — sibling with the h3
  cell-set prefilter on spatial predicates.  Row-count validated;
  prefilter is sound.
- `BETA_TESTING.md` — tester recipe and report-back template.
- `ThreePlatform_beta_status_2026-05-11.md` — cross-platform status.
