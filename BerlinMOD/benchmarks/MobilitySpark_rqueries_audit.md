# MobilitySpark — R-queries UDF audit

Companion to `MobilityDuck_rqueries_audit.md`.  End-to-end
audit of MobilitySpark's UDF surface against the
`berlinmod_r_queries_portable.sql` requirements.

## Function registration audit (mainline `MobilitySpark-parity` repo)

Every MEOS temporal function and PostGIS spatial function used by
the 17 R-queries is registered via `spark.udf().register(...)`:

| Function | MobilitySpark status |
|---|---|
| `atTime` | registered (TemporalUDFs.java) |
| `atValues` | registered (RestrictionUDFs.java) |
| `valueAtTimestamp` | registered (TemporalUDFs.java) |
| `trajectory(tgeompoint)` | registered (GeoUDFs.java) |
| `length(tgeompoint)` | registered (GeoAnalyticsUDFs.java) |
| `startTimestamp` | registered (AccessorUDFs.java) |
| `stbox(...)` | registered (STBoxUDFs.java) |
| `eDwithin(tgeompoint, tgeompoint, double)` | registered (DistanceUDFs.java) |
| `tDwithin(tgeompoint, tgeompoint, double)` | registered, 2 overloads (DistanceUDFs.java) |
| `whenTrue(tbool)` | registered (TemporalUDFs.java) |
| `expandSpace(stbox, double)` | registered (STBoxUDFs.java) |
| `aDisjoint` | registered, 2 overloads (AlwaysSpatialRelsUDFs.java) |
| `ST_Intersects` / `ST_Contains` / `ST_Distance` / `ST_Collect` | Spark SQL / Sedona |

## What's NOT on mainline yet (th3index prefilter variant only)

These are on PR #9 (open) in `/tmp/mspark-perf/.../h3/Th3IndexUDFs.java`,
CI-blocked on the JMEOS regen against latest MEOS:

- `everEqH3IndexTh3Index`, `everEqTh3IndexH3Index`, `everEqTh3IndexTh3Index`
- `tgeompointToTh3Index`
- `geoToH3IndexSet`
- `everIntersectsH3IndexSetTh3Index`

These UDFs are required by `berlinmod_r_queries_th3index_portable.sql`
(the h3-prefilter variant of the bench).  They are NOT required by
the standard `berlinmod_r_queries_portable.sql`.

## What this audit replaces in the readiness doc

The "MobilitySpark — Required" table in
`CrossPlatform_rqueries_readiness.md` listed:

- "Register `tDwithin(tgeompoint, tgeompoint, float)`" — **already
  registered** (DistanceUDFs.java).
- "Register `whenTrue(tbool)`" — **already registered**
  (TemporalUDFs.java).

Updated estimate for MobilitySpark beta-readiness on the standard
R-queries:

- Function registration: **0 person-days** (complete on mainline).
- Run-driver work: **0.5 person-day** (extend `BerlinMODBench.java`
  to dispatch all 17 queries via the portable SQL).
- JMEOS regen + Maven rebuild: **0 person-days** (only required for
  the th3index variant — the standard variant runs against current
  JMEOS).

th3index prefilter variant remains gated on PR #9's CI unblock
(JMEOS regen, parallel session).

## Combined cross-platform summary

| Platform | Standard R-queries beta status | th3index variant status |
|---|---|---|
| MobilityDB | Bench published (PR #26) | h3 prefilter pushed; sound polygon coverage |
| MobilityDuck | 0 functions missing; data-loading alignment to do (~1–1.5 days) | h3 port not started (~4–5 days) |
| MobilitySpark | 0 functions missing on mainline; run-driver port (~0.5 day) | PR #9 open, CI-blocked on JMEOS regen |

Beta testers can run the **standard R-queries portable file** on
**all three platforms** today.  The h3 prefilter variant runs on
MobilityDB today and on the other two platforms once their
respective h3 work lands.
