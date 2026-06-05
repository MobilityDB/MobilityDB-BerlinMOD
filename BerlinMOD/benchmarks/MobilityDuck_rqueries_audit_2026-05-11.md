# MobilityDuck — R-queries UDF audit (2026-05-11)

End-to-end audit of MobilityDuck's function surface against the
`berlinmod_r_queries_portable.sql` requirements.  This audit
supersedes the gap entries in
`CrossPlatform_rqueries_readiness_2026-05-11.md` for the standard
R-queries (the th3index prefilter variant remains separate work).

## Function registration audit

Every MEOS temporal function and PostGIS spatial function used by
the 17 R-queries is registered in the current MobilityDuck
extension:

| Function | MobilityDuck status |
|---|---|
| `atTime` | registered (12 overloads) |
| `atValues` | registered (12) |
| `valueAtTimestamp(tgeompoint, timestamptz)` | registered |
| `trajectory(tgeompoint)` | registered |
| `length(tgeompoint)` | registered |
| `startTimestamp(tgeompoint)` | registered |
| `stbox(tgeompoint)` | registered (10 overloads) |
| `eDwithin(tgeompoint, tgeompoint, double)` | registered |
| `tDwithin(tgeompoint, tgeompoint, double)` | registered |
| `whenTrue(tbool)` | registered |
| `expandSpace(stbox, double)` | registered |
| `aDisjoint(tgeompoint, tgeompoint)` | registered (3 overloads) |
| `ST_Intersects`, `ST_Contains`, `ST_Distance`, `ST_Collect` | DuckDB `spatial` extension |
| `&&` infix on stbox / tgeompoint / span | registered as named scalar function (DuckDB accepts in infix position) |

## End-to-end smoke validation

Run against an existing MobilityDuck DB loaded with the
cross-platform BerlinMOD CSV schema:

| Query | Result | Notes |
|---|---|---|
| Q4 (`ST_Intersects(trajectory(t.Trip), p.geom)`) | 80 rows | matches PG-native Q4 (80) |
| Q10 (`whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0))`) | 21 rows | both target UDFs execute correctly; row count differs from PG canonical (4) because the cross-platform CSV loads Trips at trip-granularity while PG-native splits per seqno — this is a data layout difference, not a function gap |

## Remaining gap for full row-count parity

The cross-platform CSV-loaded Trips table groups each trip into a
single tgeompoint sequence, while the PG-native
`berlinmod_load.sql` splits trips by (vehicleid, startdate, seqno)
into multiple per-segment tgeompoints.  This causes Q5/Q6/Q8/Q9/Q10
(which iterate over `Trips t` directly) to see different row
counts.

Two paths to converge:

1. **Load the cross-platform schema on PG too** — extend the
   PG-side bench DB to use the same single-tgeompoint-per-trip
   schema MobilityDuck and MobilitySpark use.  Run the same
   queries with the same expected counts.
2. **Add a tripsinput.csv loader path on MobilityDuck** that
   replicates the per-seqno splitting from `berlinmod_load.sql`.
   The MobilityDuck `load_data.py` infrastructure already
   supports a tripsinput.csv format; the splitter would be a
   data-prep step.

Either way, this is a data-loading alignment, not a function
parity gap.

## What this audit replaces in the readiness doc

The "MobilityDuck — Required" table in
`CrossPlatform_rqueries_readiness_2026-05-11.md` listed:

- "Register `tDwithin(tgeompoint, tgeompoint, float)`" — **already registered**
- "Register `whenTrue(tbool)`" — **already registered**

Updated estimate for MobilityDuck beta-readiness on the standard
R-queries:

- Function registration: **0 person-days** (complete).
- Data loading alignment: **0.5–1 person-day**.
- Bench driver (port `run_queries.py` for full 17-query matrix): **0.5 day**.

th3index prefilter variant remains the substantial open work
(~4–5 person-days) — separate scope from the standard R-queries
beta surface.
