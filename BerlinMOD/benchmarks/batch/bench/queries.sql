-- BerlinMOD/R benchmark queries — single canonical source shared by all
-- three runners (PostgreSQL psql, DuckDB, MobilitySpark). Each query is
-- delimited by a `-- @query <id>` marker; runners split on the marker and
-- execute each section. The SQL is the portable expression of the fixed
-- BerlinMOD intent; per-engine adaptation (e.g. Spark's preprocessForSpark)
-- is a dialect transform, not a query rewrite.


-- @query q01
-- BerlinMOD Q1: Models of vehicles with licences from Licences.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used: none (pure relational join — baseline portability test).

SELECT l.licence, v.model
FROM   Licences l
JOIN   Vehicles v ON v.licence = l.licence
ORDER  BY l.licence;


-- @query q02
-- BerlinMOD Q2: Licence plates of vehicles that ever entered a query region.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- eIntersects(trip, geom) is true whenever the moving vehicle was inside
-- or on the boundary of the polygon at any instant.
--
-- Spatial prefilter (th3index, polygon-side): geoToH3IndexSet covers the
-- query region with H3 cells at resolution 7; eIntersects
-- tests whether the trip's th3index path ever lies in any of those cells.
-- Sound for the eIntersects predicate at any resolution — a trip can only
-- intersect the region if it ever passes through a cell that covers part
-- of it.  On MobilityDB the GiST index on Trips(trip_h3) accelerates the
-- prefilter; on DuckDB / Spark the column is the prefilter mechanism.

SELECT DISTINCT v.licence
FROM   Vehicles v
JOIN   Trips t    ON  t.VehicleId = v.VehicleId
JOIN   Regions r ON
   eEq(geoToH3IndexSet(r.geom, 7), t.trip_h3)
   AND eIntersects(t.trip, r.geom)
ORDER  BY v.licence;


-- @query q03
-- BerlinMOD Q3: Position of query-licence vehicles at each query instant.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Output convention (binary return):
--   pos is the MEOS hex-WKB encoding of the tgeompoint instant, produced by
--   asHexWKB().  All three platforms call the same MEOS C function
--   (temporal_as_hexwkb, variant 0 = little-endian NDR) so the output is
--   byte-for-byte identical across platforms.

SELECT v.VehicleId     AS VehicleId,
       v.licence,
       i.instantId AS instantid,
       asHexWKB(atTime(t.trip, i.instant)) AS pos
FROM   Licences l
JOIN   Vehicles v  ON  v.licence = l.licence
JOIN   Trips    t  ON  t.VehicleId   = v.VehicleId
JOIN   Instants i ON true
WHERE  atTime(t.trip, i.instant) IS NOT NULL
ORDER  BY v.VehicleId, i.instantId;


-- @query q04
-- BerlinMOD Q4: Licence plates of vehicles that ever passed a query point.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   eIntersects(tgeompoint, geometry) → boolean, true if the trip ever intersects geom
--
-- Spatial prefilter (th3index): the trip's th3index sequence (a temporal H3
-- cell index, materialised as the trip_h3 column) must contain the query
-- point's H3 cell at the chosen resolution.  This is a sound prefilter for
-- a point-geometry intersection at any H3 resolution — a trip can only
-- intersect a point if it ever passes through the point's cell.
--
--   COALESCE(eEq(geoToH3Cell(p.geom, 7), t.trip_h3), TRUE)
--
-- The COALESCE guards against non-POINT geometries (geoToH3Cell returns
-- NULL for those) — falls through to the exact eIntersects.
--
-- MobilityDB operator equivalent:  t.trip && p.geom  (ever-intersects shorthand)
--   On PostgreSQL the GiST index on Trips(trip_h3) accelerates the prefilter;
--   on DuckDB / Spark the th3index column itself is the prefilter mechanism.

SELECT DISTINCT v.licence
FROM   Vehicles v
JOIN   Trips t      ON t.VehicleId  = v.VehicleId
JOIN   Points p ON
   COALESCE(eEq(geoToH3Cell(p.geom, 7), t.trip_h3), TRUE)
   AND eIntersects(t.trip, p.geom)
ORDER  BY v.licence;


-- @query q05
-- BerlinMOD Q5: For each pair of query-licence vehicles, the minimum
-- spatial distance ever reached between their trips, irrespective of time.
-- The BerlinMOD spec asks for the minimum distance between the places each
-- vehicle has been; the answer is the spatial-min over the two trajectory sets.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operation used:
--   minDistance(tgeompoint[], tgeompoint[]) → float8
--     The set-set spatial minimum distance: the minimum reached between any
--     trip in the first set and any trip in the second, ignoring time.  The
--     kernel prunes far trip pairs by their STBox lower bound, so the N×N is
--     resolved inside one aggregate call rather than a SQL Cartesian join.
--     This is the exact minimum -- the prune never drops the witness pair.

WITH LicTrips AS (
  SELECT l.licence,
         l.licenceId,
         array_agg(t.trip) AS trips
  FROM   Licences l
  JOIN   Vehicles      v ON v.licence = l.licence
  JOIN   Trips         t ON t.VehicleId   = v.VehicleId
  GROUP  BY l.licence, l.licenceId )
SELECT a.licence AS licence1,
       b.licence AS licence2,
       minDistance(a.trips, b.trips) AS min_dist
FROM   LicTrips a
JOIN   LicTrips b ON a.licenceId < b.licenceId
ORDER  BY a.licence, b.licence;


-- @query q06
-- BerlinMOD Q6: Pairs of trucks that ever came within 10 m of each other.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operation used:
--   eDwithinPairs(tgeompoint[], tgeompoint[], float8) → setof(i, j)
--     The set-set ever-within join: the qualifying (i, j) index pairs whose
--     trips ever came within the distance.  The kernel prunes far and
--     temporally-disjoint trip pairs by their STBox before the exact eDwithin,
--     so the N×N is resolved inside one call rather than a SQL Cartesian join.
--
-- Index base: the kernel returns 0-based indexes; Spark array access is 0-based
-- (g.lic[p.i]).  PostgreSQL/DuckDB array access is 1-based (g.lic[p.i + 1]).

WITH TruckTrips AS (
  SELECT array_agg(t.trip)    AS trips,
         array_agg(v.licence) AS lic
  FROM   Vehicles v
  JOIN   Trips    t ON t.VehicleId = v.VehicleId
  WHERE  v.type = 'truck' )
SELECT DISTINCT g.lic[p.i] AS licence1, g.lic[p.j] AS licence2
FROM   TruckTrips g,
       LATERAL eDwithinPairs(g.trips, g.trips, 10.0) AS p(i, j)
WHERE  p.i < p.j
ORDER  BY licence1, licence2;


-- @query q07
-- BerlinMOD Q7: Trip portions of query-licence vehicles during each query period.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Output convention (binary return):
--   pos is the MEOS hex-WKB encoding of the restricted tgeompoint sequence,
--   produced by asHexWKB(). All three platforms call the same MEOS C function
--   so the output is byte-for-byte identical across platforms.

SELECT v.VehicleId     AS VehicleId,
       v.licence,
       p.periodId  AS periodid,
       asHexWKB(atTime(t.trip, p.period)) AS pos
FROM   Licences l
JOIN   Vehicles v  ON  v.licence = l.licence
JOIN   Trips    t  ON  t.VehicleId   = v.VehicleId
JOIN   Periods p ON true
WHERE  atTime(t.trip, p.period) IS NOT NULL
ORDER  BY v.VehicleId, p.periodId, t.TripId;


-- @query q08
-- BerlinMOD Q8: Trajectory of each vehicle as a hex-WKB geometry string.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- trajectory() collapses a tgeompoint sequence into its spatial path
-- (LINESTRING for a sequence, POINT for a single instant).  Both PostgreSQL
-- COPY and DuckDB COPY serialize the GEOMETRY type as hex WKB in CSV output,
-- and MobilitySpark's trajectory() UDF produces the same format via
-- geo_as_hexewkb(), so the output is byte-for-byte identical across platforms.

SELECT TripId AS TripId,
       trajectory(trip) AS traj
FROM   Trips
ORDER  BY TripId;


-- @query qrt
-- BerlinMOD QRT: Binary roundtrip verification.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Protocol: text in, binary out, byte-equal on reception.
--   Each trip was loaded from WKT text (CSV input).
--   asHexWKB() serializes it to the canonical MEOS hex-WKB (variant 0,
--   little-endian NDR) — the same C function on all three platforms.
--   The hex-WKB strings must be byte-for-byte identical across platforms.

SELECT TripId AS TripId,
       asHexWKB(trip) AS trip_hexwkb
FROM   Trips
ORDER  BY TripId;


-- @query q09
-- BerlinMOD Q9: What is the longest distance travelled by a vehicle during
-- each of the periods from Periods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint   (restrict to period)
--   length(tgeompoint) → float8                  (Euclidean path length)

WITH Distances AS (
  SELECT p.periodId, p.period, t.VehicleId,
         SUM(length(atTime(t.trip, p.period))) AS dist
  FROM   Trips t, Periods p
  WHERE  atTime(t.trip, p.period) IS NOT NULL
  GROUP  BY p.periodId, p.period, t.VehicleId
)
SELECT periodId, period, MAX(dist) AS maxDist
FROM   Distances
GROUP  BY periodId, period
ORDER  BY periodId;


-- @query q10
-- BerlinMOD Q10: When did the vehicles with licences from Licences meet
-- other vehicles (within 3 m) and what are the other vehicle IDs?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operation used:
--   tDwithinPairs(tgeompoint[], tgeompoint[], float) → setof(i, j, periods)
--     The set-set when-within join: per qualifying (i, j) trip pair, the
--     whenTrue(tDwithin) spanset of the intervals the two trips were within the
--     distance.  The kernel prunes far and temporally-disjoint pairs by their
--     STBox before the exact tDwithin, so the N×M is resolved inside one call
--     rather than a SQL Cartesian join.  Only qualifying pairs are returned, so
--     periods is never NULL.
--
-- Index base: the kernel returns 0-based indexes (Spark array access is 0-based).

WITH LicTrips AS (
  SELECT array_agg(t1.trip)   AS trips,
         array_agg(l.licence) AS lic,
         array_agg(t1.VehicleId)  AS veh
  FROM   Licences l
  JOIN   Vehicles v1 ON v1.licence = l.licence
  JOIN   Trips    t1 ON t1.VehicleId   = v1.VehicleId ),
AllTrips AS (
  SELECT array_agg(t2.trip)  AS trips,
         array_agg(t2.VehicleId) AS veh
  FROM   Trips t2 )
SELECT a.lic[p.i] AS licence1, b.veh[p.j] AS car2Id, p.periods AS periods
FROM   LicTrips a, AllTrips b,
       LATERAL tDwithinPairs(a.trips, b.trips, 3.0) AS p(i, j, periods)
WHERE  a.veh[p.i] <> b.veh[p.j]
ORDER  BY licence1, car2Id;


-- @query q11
-- BerlinMOD Q11: Which vehicles passed a point from Points at one of
-- the instants from Instants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT p.pointId, p.geom, p.geomWKT, i.instantId, i.instant, t.VehicleId
  FROM   Trips t, Points p, Instants i
  WHERE  COALESCE(eEq(geoToH3Cell(p.geom, 7), t.trip_h3), TRUE)
    AND  valueAtTimestamp(t.trip, i.instant) = p.geom
)
SELECT t.pointId, t.geomWKT AS geom, t.instantId, t.instant, v.licence
FROM   Temp t
JOIN   Vehicles v ON t.VehicleId = v.VehicleId
ORDER  BY t.pointId, t.instantId, v.licence;


-- @query q12
-- BerlinMOD Q12: Which pairs of vehicles were at the same point from
-- Points at the same instant from Instants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT p.pointId, p.geom, p.geomWKT, i.instantId, i.instant, t.VehicleId
  FROM   Trips t, Points p, Instants i
  WHERE  COALESCE(eEq(geoToH3Cell(p.geom, 7), t.trip_h3), TRUE)
    AND  valueAtTimestamp(t.trip, i.instant) = p.geom
)
SELECT DISTINCT t1.pointId, t1.geomWKT AS geom,
       t1.instantId, t1.instant,
       v1.licence AS licence1, v2.licence AS licence2
FROM   Temp t1
JOIN   Vehicles v1 ON t1.VehicleId = v1.VehicleId
JOIN   Temp     t2 ON t1.VehicleId < t2.VehicleId
                  AND t1.pointId   = t2.pointId
                  AND t1.instantId = t2.instantId
JOIN   Vehicles v2 ON t2.VehicleId = v2.VehicleId
ORDER  BY t1.pointId, t1.instantId, licence1, licence2;


-- @query q13
-- BerlinMOD Q13: Which vehicles travelled within a region from Regions
-- during a period from Periods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 Regions × 100 Periods is ~100× more expensive.
-- This query mirrors the original by using only the first 10 regions and 10 periods.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT r.regionId, p.periodId, p.period, t.VehicleId
  FROM   Trips t, Regions r, Periods p
  WHERE  r.regionId <= 10 AND p.periodId <= 10
    AND  eEq(geoToH3IndexSet(r.geom, 7), t.trip_h3)
    AND  eIntersects(atTime(t.trip, p.period), r.geom)
)
SELECT DISTINCT t.regionId, t.periodId, t.period, v.licence
FROM   Temp t, Vehicles v
WHERE  t.VehicleId = v.VehicleId
ORDER  BY t.regionId, t.periodId, v.licence;


-- @query q14
-- BerlinMOD Q14: Which vehicles were inside a region from Regions at
-- one of the instants from Instants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT r.regionId, i.instantId, i.instant, t.VehicleId
  FROM   Trips t, Regions r, Instants i
  WHERE  eEq(geoToH3IndexSet(r.geom, 7), t.trip_h3)
    AND  ST_Contains(r.geom, valueAtTimestamp(t.trip, i.instant))
)
SELECT DISTINCT t.regionId, t.instantId, t.instant, v.licence
FROM   Temp t
JOIN   Vehicles v ON t.VehicleId = v.VehicleId
ORDER  BY t.regionId, t.instantId, v.licence;


-- @query q15
-- BerlinMOD Q15: Which vehicles passed a point from Points during a
-- period from Periods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 Points × 100 Periods is ~100× more expensive.
-- This query mirrors the original by using only the first 10 points and 10 periods.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT pt.pointId, pt.geom, pt.geomWKT, pr.periodId, pr.period, t.VehicleId
  FROM   Trips t, Points pt, Periods pr
  WHERE  pt.pointId  <= 10 AND pr.periodId <= 10
    AND  COALESCE(eEq(geoToH3Cell(pt.geom, 7), t.trip_h3), TRUE)
    AND  eIntersects(atTime(t.trip, pr.period), pt.geom)
)
SELECT DISTINCT t.pointId, t.geomWKT AS geom, t.periodId, t.period, v.licence
FROM   Temp t, Vehicles v
WHERE  t.VehicleId = v.VehicleId
ORDER  BY t.pointId, t.periodId, v.licence;


-- @query q16
-- BerlinMOD Q16: Which pairs of query-licence vehicles were both within a
-- region from Regions during a period from Periods, but never at
-- the same location at the same time (always disjoint)?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 Licences × 100 Periods × 100 Regions is
-- ~10,000× more expensive.  This query mirrors the original by using only the
-- first 10 licences, 10 periods, and 10 regions.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter)
--   aDisjointPairs(tgeompoint[], tgeompoint[]) → setof(i, j)
--     The set-set always-disjoint join: the qualifying (i, j) index pairs whose
--     period-restricted trips never share a location (= ¬eIntersects).  Per
--     (period, region) the trips that intersect the region during the period
--     are arrayed once; the kernel resolves the pair join inside one call
--     (non-overlapping STBoxes are trivially disjoint), so there is no SQL
--     licence × licence Cartesian.
--
-- Index base: the kernel returns 0-based indexes (Spark array access is 0-based).

WITH PR AS (
  SELECT p.periodId, p.period, r.regionId,
         array_agg(atTime(t.trip, p.period)) AS trips,
         array_agg(l.licence)                AS lic,
         array_agg(l.licenceId)              AS lid
  FROM   Licences l
  JOIN   Vehicles v ON v.licence = l.licence
  JOIN   Trips    t ON t.VehicleId   = v.VehicleId
  JOIN   Periods p ON true
  JOIN   Regions r ON true
  WHERE  l.licenceId <= 10 AND p.periodId <= 10 AND r.regionId <= 10
    AND  eEq(geoToH3IndexSet(r.geom, 7), t.trip_h3)
    AND  eIntersects(atTime(t.trip, p.period), r.geom)
  GROUP  BY p.periodId, p.period, r.regionId )
SELECT g.periodId, g.period, g.regionId,
       g.lic[q.i] AS licence1, g.lic[q.j] AS licence2
FROM   PR g,
       LATERAL aDisjointPairs(g.trips, g.trips) AS q(i, j)
WHERE  g.lid[q.i] < g.lid[q.j]
ORDER  BY g.periodId, g.regionId, licence1, licence2;


-- @query q17
-- BerlinMOD Q17: Which point(s) from Points have been visited by the
-- maximum number of distinct vehicles?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)

WITH PointCount AS (
  SELECT p.pointId, COUNT(DISTINCT t.VehicleId) AS hits
  FROM   Trips t, Points p
  WHERE  eIntersects(t.trip, p.geom)
  GROUP  BY p.pointId
)
SELECT pointId, hits
FROM   PointCount
WHERE  hits = (SELECT MAX(hits) FROM PointCount)
ORDER  BY pointId;
