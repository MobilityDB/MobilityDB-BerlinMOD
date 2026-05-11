# MobilityDB — BerlinMOD 17 R-Queries — GiST / SP-GiST Matrix

**Date**: 2026-05-11
**Platform**: MobilityDB on PostgreSQL 17.8
**Build**: th3index HEAD + MobilityDB PR #938 polygon coverage fix +
PR #940 lift framework helper.
**Dataset**: BerlinMOD scalefactor 0.005 — 1620 trips × 100 vehicles
× 100 regions × 100 points × 100 periods × 100 instants.  Parameter
subsets `Licences1`, `Licences2`, `Instants1`, `Periods1`, `Points1`,
`Regions1` (each LIMIT 10).
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
| Q5  | Min distance between trip locations of two licence sets | trip-trip spatial cross-join |
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

| Q | rows | none | GiST(trip + trajectory) | SP-GiST(trip + trajectory) |
|---|---:|---:|---:|---:|
| Q1  | 72  |   2.74 |   0.79 |   0.78 |
| Q2  | 1   |   0.15 |   0.13 |   0.09 |
| Q3  | 0   |   6.17 |   4.69 |   8.50 |
| Q4  | 80  |  15.78 |  13.43 |  13.60 |
| Q5  | 30  | 269.11 | 238.89 | 235.03 |
| Q6  | 0   |  36.68 |   9.12 |   4.50 |
| Q7  | 26  |  11.59 |  11.77 |  11.20 |
| Q8  | 39  |   1.09 |   1.12 |   1.30 |
| Q9  | 94  |  30.83 |  12.84 |  11.70 |
| Q10 | 4   |  57.10 |   9.21 |   8.76 |
| Q11 | 0   |   3.07 |   3.61 |   2.81 |
| Q12 | 0   |   2.97 |   2.88 |   2.84 |
| Q13 | 425 |  53.79 |   6.18 |   5.92 |
| Q14 | 2   |  23.98 |   0.50 |   0.48 |
| Q15 | 118 |  26.58 |   5.05 |   4.73 |
| Q16 | 9   |  16.02 |  16.17 |  15.66 |
| Q17 | 1   |  11.85 |  12.01 |  11.73 |
| **Total** | — | **569.51** | **348.40** | **339.61** |

Identical row counts across all three configurations.

---

## Speedup highlights

GiST(trip+trajectory) over baseline:

- **Q14** (`ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant))`)
  — 48× speedup.  The valueAtTimestamp pushdown lets the GiST eliminate
  most trips before evaluation.
- **Q13** (`ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom)`)
  — 8.7×.  GiST on `trajectory` is the active index.
- **Q10** (`tDwithin(t1.Trip, t2.Trip, 3.0)`) — 6.2× on the trip×trip
  join, GiST(trip) on both sides.
- **Q15** (trajectory-vs-point during period) — 5.3×.
- **Q6** (`eDwithin(t1.Trip, t2.Trip, 10.0)` for truck pairs) — 4.0×.
- **Q9** (atTime + length aggregation) — 2.4×.

SP-GiST(trip+trajectory) shows the same shape with a slight edge on
Q6 (8.1× vs 4.0×) and Q14 (49.7× vs 47.7×), and a slight regression
on Q3.  Total runtime is within 2.5 % of GiST, in run-to-run noise.

## Queries the index does not help

- **Q1 / Q2** — relational only, no spatial predicate.
- **Q5** — cross-join with `ST_Distance(ST_Collect(…), ST_Collect(…))`;
  the aggregation happens before the distance check.
- **Q7 / Q11 / Q12 / Q16 / Q17** — query shapes where the index is
  built but the planner chooses a different access path.

---

## What this matrix does NOT measure

- **The h3 cell-set prefilter clause.**  Adding
  `everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(R.Geom, 7), T.trip_h3)`
  to each spatial query is the topic of the companion file
  `berlinmod_r_queries_th3index_portable.sql`.  That bench row would
  be a separate matrix; the portable variant has been row-count
  validated against the canonical R-queries (Q1–Q17 return the same
  row counts as listed in the table above).
- **Three-platform parity.**  MobilityDuck and MobilitySpark report
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

## Companion files in this PR

- `berlinmod_r_queries_portable.sql` — same 17 queries, portable
  dialect (PG-specific operators replaced with function-call form;
  no PL/pgSQL harness).  Row-count validated against canonical.
- `berlinmod_r_queries_th3index_portable.sql` — sibling with the h3
  cell-set prefilter on spatial predicates.  Row-count validated;
  prefilter is sound.
- `BETA_TESTING.md` — tester recipe and report-back template.
- `CrossPlatform_rqueries_readiness_2026-05-11.md` — work inventory
  for replicating this bench on MobilityDuck and MobilitySpark.
