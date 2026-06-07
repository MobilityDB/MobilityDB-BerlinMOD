/*-----------------------------------------------------------------------------
-- BerlinMOD Range Queries — Portable Dialect (Q1–Q17)
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

Portable variant of `berlinmod_r_queries.sql` for the cross-platform
edge-to-cloud benchmark.  The PG-specific PL/pgSQL harness, the
`EXPLAIN (ANALYZE, FORMAT JSON)` capture, and the
`execution_tests_explain` results table belong to the canonical file
and are not reproduced here.  This file is the 17 query bodies as
runnable, platform-agnostic SQL.

Operator-to-function mapping (PG → portable):
  &&   (bbox overlap)         → drop as a prefilter; rely on semantic
                                predicate.  The portable form retains
                                correctness; per-platform performance
                                comes from each platform's own
                                indexing / pushdown.
  @>   (bbox contains)        → drop; rely on `valueAtTimestamp IS NOT NULL`
                                or `eIntersects` for the actual containment.
  _ST_Intersects              → `ST_Intersects` (bbox-bypass form is
                                a PG-only optimization).

Schema:
  Trips(TripId, VehicleId, Trip tgeompoint, Trajectory geometry, …)
  Vehicles(VehicleId, Licence, VehicleType, Model)
  Licences(LicenceId, Licence, VehicleId)
  Licences1, Licences2 (subsets)
  Points(PointId, PosX, PosY, Geom)
  Points1 (subset)
  Regions(RegionId, RegionName, Geom)
  Regions1 (subset)
  Instants(InstantId, Instant)
  Instants1 (subset)
  Periods(PeriodId, Period)
  Periods1 (subset)

Expected row counts (BerlinMOD scalefactor 0.005, Brussels) — identical
on PostgreSQL, DuckDB, and Spark when the LIMIT-10 parameter views use
deterministic `ORDER BY <PrimaryKey>` (per the matching fix in
`berlinmod_load.sql`):
  Q1:72  Q2:1  Q3:6  Q4:80  Q5:100 Q6:0 Q7:26 Q8:75 Q9:94
  Q10:21 Q11:0 Q12:0 Q13:278 Q14:1 Q15:118 Q16:2 Q17:1

-----------------------------------------------------------------------------*/

-- Q1: Models of vehicles with licences from Licences
SELECT DISTINCT l.Licence, v.Model AS Model
FROM Vehicles v, Licences l
WHERE v.Licence = l.Licence
ORDER BY l.Licence;

-- Q2: How many vehicles are passenger cars?
SELECT COUNT(Licence)
FROM Vehicles v
WHERE VehicleType = 'passenger';

-- Q3: Position of each Licences1 vehicle at each Instants1 instant
SELECT DISTINCT l.Licence, i.InstantId, i.Instant AS Instant,
  valueAtTimestamp(t.Trip, i.Instant) AS Location
FROM Trips t, Licences1 l, Instants1 i
WHERE t.VehicleId = l.VehicleId
  AND valueAtTimestamp(t.Trip, i.Instant) IS NOT NULL
ORDER BY l.Licence, i.InstantId;

-- Q4: Which vehicles passed any Points?
SELECT DISTINCT p.PointId, p.Geom, v.Licence
FROM Trips t, Vehicles v, Points p
WHERE t.VehicleId = v.VehicleId
  AND ST_Intersects(trajectory(t.Trip), p.Geom)
ORDER BY p.PointId, v.Licence;

-- Q5: Minimum distance between trip locations of two licence sets
-- Each side aggregates its licence-group of trips into a tgeompoint
-- array; `minDistance(tgeompoint[], tgeompoint[])` then returns the
-- exact set-set spatial minimum distance, equivalent to
-- `ST_Distance(ST_Collect(trajectory(...)), ST_Collect(trajectory(...)))`
-- but with each trip's STBox used as a sound lower-bound prefilter so
-- trip pairs whose bounding boxes are already farther apart than the
-- running minimum are skipped.  The per-pair distance still uses
-- liblwgeom's segment-pair sweep so the answer is bit-identical to the
-- aggregated form (see MobilityDB PR #1007).
WITH Temp1(Licence1, Trips) AS (
  SELECT l1.Licence, array_agg(t1.Trip)
  FROM Trips t1, Licences1 l1
  WHERE t1.VehicleId = l1.VehicleId
  GROUP BY l1.Licence),
Temp2(Licence2, Trips) AS (
  SELECT l2.Licence, array_agg(t2.Trip)
  FROM Trips t2, Licences2 l2
  WHERE t2.VehicleId = l2.VehicleId
  GROUP BY l2.Licence)
SELECT Licence1, Licence2, minDistance(t1.Trips, t2.Trips) AS MinDist
FROM Temp1 t1, Temp2 t2
ORDER BY Licence1, Licence2;

-- Q6: Truck pairs that ever met within 10 m
WITH Temp(Licence, VehicleId, Trip) AS (
  SELECT v.Licence, t.VehicleId, t.Trip
  FROM Trips t, Vehicles v
  WHERE t.VehicleId = v.VehicleId
    AND v.VehicleType = 'truck')
SELECT DISTINCT t1.Licence, t2.Licence
FROM Temp t1, Temp t2
WHERE t1.VehicleId < t2.VehicleId
  AND eDwithin(t1.Trip, t2.Trip, 10.0)
ORDER BY t1.Licence, t2.Licence;

-- Q7: For each Points geometry, earliest passenger-car visit
WITH Temp AS (
  SELECT DISTINCT v.Licence, p.PointId, p.Geom,
    MIN(startTimestamp(atValues(t.Trip, p.Geom))) AS Instant
  FROM Trips t, Vehicles v, Points p
  WHERE t.VehicleId = v.VehicleId
    AND v.VehicleType = 'passenger'
    AND ST_Intersects(trajectory(t.Trip), p.Geom)
  GROUP BY v.Licence, p.PointId, p.Geom)
SELECT t1.Licence, t1.PointId, t1.Geom, t1.Instant
FROM Temp t1
WHERE t1.Instant <= ALL (
  SELECT t2.Instant
  FROM Temp t2
  WHERE t1.PointId = t2.PointId)
ORDER BY t1.PointId, t1.Licence;

-- Q8: Total distance per licence × Periods1 period
SELECT l.Licence, p.PeriodId, p.Period,
  SUM(length(atTime(t.Trip, p.Period))) AS Dist
FROM Trips t, Licences1 l, Periods1 p
WHERE t.VehicleId = l.VehicleId
  AND atTime(t.Trip, p.Period) IS NOT NULL
GROUP BY l.Licence, p.PeriodId, p.Period
ORDER BY l.Licence, p.PeriodId;

-- Q9: Longest single-period vehicle distance per Periods period
WITH Distances AS (
  SELECT p.PeriodId, p.Period, t.VehicleId,
    SUM(length(atTime(t.Trip, p.Period))) AS Dist
  FROM Trips t, Periods p
  WHERE atTime(t.Trip, p.Period) IS NOT NULL
  GROUP BY p.PeriodId, p.Period, t.VehicleId)
SELECT PeriodId, Period, MAX(Dist) AS MaxDist
FROM Distances
GROUP BY PeriodId, Period
ORDER BY PeriodId;

-- Q10: When and where each Licences1 vehicle was within 3 m of another
WITH Temp AS (
  SELECT l1.Licence AS Licence1, t2.VehicleId AS Car2Id,
    whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) AS Periods
  FROM Trips t1, Licences1 l1, Trips t2, Vehicles v
  WHERE t1.VehicleId = l1.VehicleId
    AND t2.VehicleId = v.VehicleId
    AND t1.VehicleId <> t2.VehicleId)
SELECT Licence1, Car2Id, Periods
FROM Temp
WHERE Periods IS NOT NULL;

-- Q11: Vehicles passing a Points1 at an Instants1 instant
WITH Temp AS (
  SELECT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
  FROM Trips t, Points1 p, Instants1 i
  WHERE valueAtTimestamp(t.Trip, i.Instant) = p.Geom)
SELECT t.PointId, t.Geom, t.InstantId, t.Instant, v.Licence
FROM Temp t JOIN Vehicles v ON t.VehicleId = v.VehicleId
ORDER BY t.PointId, t.InstantId, v.Licence;

-- Q12: Pairs of vehicles meeting at a Points1 at an Instants1 instant
WITH Temp AS (
  SELECT DISTINCT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
  FROM Trips t, Points1 p, Instants1 i
  WHERE valueAtTimestamp(t.Trip, i.Instant) = p.Geom)
SELECT DISTINCT t1.PointId, t1.Geom, t1.InstantId, t1.Instant,
  v1.Licence AS Licence1, v2.Licence AS Licence2
FROM Temp t1
JOIN Vehicles v1 ON t1.VehicleId = v1.VehicleId
JOIN Temp t2 ON t1.VehicleId < t2.VehicleId
  AND t1.PointId = t2.PointId
  AND t1.InstantId = t2.InstantId
JOIN Vehicles v2 ON t2.VehicleId = v2.VehicleId
ORDER BY t1.PointId, t1.InstantId, v1.Licence, v2.Licence;

-- Q13: Vehicles ever in a Regions1 during a Periods1 period
WITH Temp AS (
  SELECT DISTINCT r.RegionId, p.PeriodId, p.Period, t.VehicleId
  FROM Trips t, Regions1 r, Periods1 p
  WHERE ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom))
SELECT DISTINCT t.RegionId, t.PeriodId, t.Period, v.Licence
FROM Temp t, Vehicles v
WHERE t.VehicleId = v.VehicleId
ORDER BY t.RegionId, t.PeriodId, v.Licence;

-- Q14: Vehicles inside a Regions1 at an Instants1 instant
WITH Temp AS (
  SELECT DISTINCT r.RegionId, i.InstantId, i.Instant, t.VehicleId
  FROM Trips t, Regions1 r, Instants1 i
  WHERE ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant)))
SELECT DISTINCT t.RegionId, t.InstantId, t.Instant, v.Licence
FROM Temp t JOIN Vehicles v ON t.VehicleId = v.VehicleId
ORDER BY t.RegionId, t.InstantId, v.Licence;

-- Q15: Vehicles passing a Points1 during a Periods1 period
WITH Temp AS (
  SELECT DISTINCT pt.PointId, pt.Geom, pr.PeriodId, pr.Period, t.VehicleId
  FROM Trips t, Points1 pt, Periods1 pr
  WHERE ST_Intersects(trajectory(atTime(t.Trip, pr.Period)), pt.Geom))
SELECT DISTINCT t.PointId, t.Geom, t.PeriodId, t.Period, v.Licence
FROM Temp t, Vehicles v
WHERE t.VehicleId = v.VehicleId
ORDER BY t.PointId, t.PeriodId, v.Licence;

-- Q16: Pairs of vehicles that ever met in a Regions1 during a Periods1
SELECT p.PeriodId, p.Period, r.RegionId,
  l1.Licence AS Licence1, l2.Licence AS Licence2
FROM Trips t1, Licences1 l1, Trips t2, Licences2 l2, Periods1 p, Regions1 r
WHERE t1.VehicleId = l1.VehicleId
  AND t2.VehicleId = l2.VehicleId
  AND l1.Licence < l2.Licence
  AND ST_Intersects(trajectory(atTime(t1.Trip, p.Period)), r.Geom)
  AND ST_Intersects(trajectory(atTime(t2.Trip, p.Period)), r.Geom)
  AND aDisjoint(atTime(t1.Trip, p.Period), atTime(t2.Trip, p.Period))
ORDER BY PeriodId, RegionId, Licence1, Licence2;

-- Q17: Points visited by the maximum number of vehicles
WITH PointCount AS (
  SELECT p.PointId, COUNT(DISTINCT t.VehicleId) AS Hits
  FROM Trips t, Points p
  WHERE ST_Intersects(trajectory(t.Trip), p.Geom)
  GROUP BY p.PointId)
SELECT PointId, Hits
FROM PointCount AS p
WHERE p.Hits = (SELECT MAX(Hits) FROM PointCount);
