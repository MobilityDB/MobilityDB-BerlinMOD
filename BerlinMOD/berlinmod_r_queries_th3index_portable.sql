/*-----------------------------------------------------------------------------
-- BerlinMOD Range Queries — Portable Dialect with th3index Prefilter
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

th3index-accelerated sibling of `berlinmod_r_queries_portable.sql`.
Same Q1–Q17 as the portable variant, with one extra clause per
spatial-against-static query:

    everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(G, 7), T.trip_h3)
      AND <semantic predicate>

`geoToH3IndexSet` covers POINT / LINESTRING / POLYGON / MULTI* /
GeometryCollection in one walker, so the same prefilter shape applies
to all spatial-static predicates.  Q1 and Q2 are relational (no spatial
predicate) so they are unchanged.  Q3 and Q11 / Q12 use
`valueAtTimestamp` (a point lookup at a single instant) — the
prefilter applies via the point form.

Prerequisites:
  - The `Trips` table carries a `trip_h3 th3index` column populated at
    H3 resolution 7 via `h3_latlng_to_cell(transform(Trip, 4326), 7)`.
    The shared CSV produced by `berlinmod_portability_export()`
    contains it; loaders on each platform unpack it.
  - The static-geometry → H3 cell-set public API is registered:
    `geoToH3Cell`, `geoToH3IndexSet`,
    `everIntersectsH3IndexSet_Th3Index`.
  - For MobilityDB specifically, the GiST index on `trip_h3` makes
    the prefilter pushable to the planner.  Other platforms use the
    columnar `trip_h3` value directly.

Expected row counts must match the non-h3 portable variant (the
prefilter is sound — see the chapter-1 bench report).

The h3 prefilter uses the 4326-reprojected geometry of the static
input.  In schemas where the canonical geometry is metric (e.g.
EPSG:3857), the prefilter argument should be transformed:
`geoToH3IndexSet(ST_Transform(G, 4326), 7)`.

-----------------------------------------------------------------------------*/

-- Q1: Models of vehicles with licences (relational, no prefilter)
SELECT DISTINCT l.Licence, v.Model AS Model
FROM Vehicles v, Licences l
WHERE v.Licence = l.Licence
ORDER BY l.Licence;

-- Q2: How many vehicles are passenger cars? (relational, no prefilter)
SELECT COUNT(Licence)
FROM Vehicles v
WHERE VehicleType = 'passenger';

-- Q3: Position at instants (no geometry input, prefilter not applicable)
SELECT DISTINCT l.Licence, i.InstantId, i.Instant AS Instant,
  valueAtTimestamp(t.Trip, i.Instant) AS Location
FROM Trips t, Licences1 l, Instants1 i
WHERE t.VehicleId = l.VehicleId
  AND valueAtTimestamp(t.Trip, i.Instant) IS NOT NULL
ORDER BY l.Licence, i.InstantId;

-- Q4: Trips passing each Points geometry
SELECT DISTINCT p.PointId, p.Geom, v.Licence
FROM Trips t, Vehicles v, Points p
WHERE t.VehicleId = v.VehicleId
  AND everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(p.Geom, 4326), 7), t.trip_h3)
  AND ST_Intersects(trajectory(t.Trip), p.Geom)
ORDER BY p.PointId, v.Licence;

-- Q5: Trip-trip min distance (cross-join, no static geometry — prefilter
--     not applicable to static-vs-temporal; trip-vs-trip prefilter would
--     need everEqTh3IndexTh3Index but the static-set form does not apply)
WITH Temp1(Licence1, Trajs) AS (
  SELECT l1.Licence, ST_Collect(trajectory(t1.Trip))
  FROM Trips t1, Licences1 l1
  WHERE t1.VehicleId = l1.VehicleId
  GROUP BY l1.Licence),
Temp2(Licence2, Trajs) AS (
  SELECT l2.Licence, ST_Collect(trajectory(t2.Trip))
  FROM Trips t2, Licences2 l2
  WHERE t2.VehicleId = l2.VehicleId
  GROUP BY l2.Licence)
SELECT Licence1, Licence2, ST_Distance(t1.Trajs, t2.Trajs) AS MinDist
FROM Temp1 t1, Temp2 t2
ORDER BY Licence1, Licence2;

-- Q6: Truck pairs that ever met within 10 m (trip-trip cross-join)
WITH Temp(Licence, VehicleId, Trip, trip_h3) AS (
  SELECT v.Licence, t.VehicleId, t.Trip, t.trip_h3
  FROM Trips t, Vehicles v
  WHERE t.VehicleId = v.VehicleId
    AND v.VehicleType = 'truck')
SELECT DISTINCT t1.Licence, t2.Licence
FROM Temp t1, Temp t2
WHERE t1.VehicleId < t2.VehicleId
  AND ever_eq(t1.trip_h3, t2.trip_h3)
  AND eDwithin(t1.Trip, t2.Trip, 10.0)
ORDER BY t1.Licence, t2.Licence;

-- Q7: Earliest passenger-car visit per Points geometry
WITH Temp AS (
  SELECT DISTINCT v.Licence, p.PointId, p.Geom,
    MIN(startTimestamp(atValues(t.Trip, p.Geom))) AS Instant
  FROM Trips t, Vehicles v, Points p
  WHERE t.VehicleId = v.VehicleId
    AND v.VehicleType = 'passenger'
    AND everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(p.Geom, 4326), 7), t.trip_h3)
    AND ST_Intersects(trajectory(t.Trip), p.Geom)
  GROUP BY v.Licence, p.PointId, p.Geom)
SELECT t1.Licence, t1.PointId, t1.Geom, t1.Instant
FROM Temp t1
WHERE t1.Instant <= ALL (
  SELECT t2.Instant
  FROM Temp t2
  WHERE t1.PointId = t2.PointId)
ORDER BY t1.PointId, t1.Licence;

-- Q8: Total distance per licence × Periods1 (no spatial geometry input)
SELECT l.Licence, p.PeriodId, p.Period,
  SUM(length(atTime(t.Trip, p.Period))) AS Dist
FROM Trips t, Licences1 l, Periods1 p
WHERE t.VehicleId = l.VehicleId
  AND atTime(t.Trip, p.Period) IS NOT NULL
GROUP BY l.Licence, p.PeriodId, p.Period
ORDER BY l.Licence, p.PeriodId;

-- Q9: Max single-period vehicle distance (no spatial geometry input)
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

-- Q10: Licences1 vehicles within 3 m of other vehicles (trip-trip)
WITH Temp AS (
  SELECT l1.Licence AS Licence1, t2.VehicleId AS Car2Id,
    whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) AS Periods
  FROM Trips t1, Licences1 l1, Trips t2, Vehicles v
  WHERE t1.VehicleId = l1.VehicleId
    AND t2.VehicleId = v.VehicleId
    AND t1.VehicleId <> t2.VehicleId
    AND ever_eq(t1.trip_h3, t2.trip_h3))
SELECT Licence1, Car2Id, Periods
FROM Temp
WHERE Periods IS NOT NULL;

-- Q11: Vehicles passing a Points1 at an Instants1 instant
WITH Temp AS (
  SELECT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
  FROM Trips t, Points1 p, Instants1 i
  WHERE everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(p.Geom, 4326), 7), t.trip_h3)
    AND valueAtTimestamp(t.Trip, i.Instant) = p.Geom)
SELECT t.PointId, t.Geom, t.InstantId, t.Instant, v.Licence
FROM Temp t JOIN Vehicles v ON t.VehicleId = v.VehicleId
ORDER BY t.PointId, t.InstantId, v.Licence;

-- Q12: Pairs of vehicles meeting at a Points1 at an Instants1 instant
WITH Temp AS (
  SELECT DISTINCT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
  FROM Trips t, Points1 p, Instants1 i
  WHERE everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(p.Geom, 4326), 7), t.trip_h3)
    AND valueAtTimestamp(t.Trip, i.Instant) = p.Geom)
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
  WHERE everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(r.Geom, 4326), 7), t.trip_h3)
    AND ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom))
SELECT DISTINCT t.RegionId, t.PeriodId, t.Period, v.Licence
FROM Temp t, Vehicles v
WHERE t.VehicleId = v.VehicleId
ORDER BY t.RegionId, t.PeriodId, v.Licence;

-- Q14: Vehicles inside a Regions1 at an Instants1 instant
WITH Temp AS (
  SELECT DISTINCT r.RegionId, i.InstantId, i.Instant, t.VehicleId
  FROM Trips t, Regions1 r, Instants1 i
  WHERE everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(r.Geom, 4326), 7), t.trip_h3)
    AND ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant)))
SELECT DISTINCT t.RegionId, t.InstantId, t.Instant, v.Licence
FROM Temp t JOIN Vehicles v ON t.VehicleId = v.VehicleId
ORDER BY t.RegionId, t.InstantId, v.Licence;

-- Q15: Vehicles passing a Points1 during a Periods1 period
WITH Temp AS (
  SELECT DISTINCT pt.PointId, pt.Geom, pr.PeriodId, pr.Period, t.VehicleId
  FROM Trips t, Points1 pt, Periods1 pr
  WHERE everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(pt.Geom, 4326), 7), t.trip_h3)
    AND ST_Intersects(trajectory(atTime(t.Trip, pr.Period)), pt.Geom))
SELECT DISTINCT t.PointId, t.Geom, t.PeriodId, t.Period, v.Licence
FROM Temp t, Vehicles v
WHERE t.VehicleId = v.VehicleId
ORDER BY t.PointId, t.PeriodId, v.Licence;

-- Q16: Vehicle pairs ever met in a Regions1 during a Periods1 (cross-join)
SELECT p.PeriodId, p.Period, r.RegionId,
  l1.Licence AS Licence1, l2.Licence AS Licence2
FROM Trips t1, Licences1 l1, Trips t2, Licences2 l2, Periods1 p, Regions1 r
WHERE t1.VehicleId = l1.VehicleId
  AND t2.VehicleId = l2.VehicleId
  AND l1.Licence < l2.Licence
  AND everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(r.Geom, 4326), 7), t1.trip_h3)
  AND everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(r.Geom, 4326), 7), t2.trip_h3)
  AND ST_Intersects(trajectory(atTime(t1.Trip, p.Period)), r.Geom)
  AND ST_Intersects(trajectory(atTime(t2.Trip, p.Period)), r.Geom)
  AND aDisjoint(atTime(t1.Trip, p.Period), atTime(t2.Trip, p.Period))
ORDER BY PeriodId, RegionId, Licence1, Licence2;

-- Q17: Points visited by the maximum number of vehicles
WITH PointCount AS (
  SELECT p.PointId, COUNT(DISTINCT t.VehicleId) AS Hits
  FROM Trips t, Points p
  WHERE everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(p.Geom, 4326), 7), t.trip_h3)
    AND ST_Intersects(trajectory(t.Trip), p.Geom)
  GROUP BY p.PointId)
SELECT PointId, Hits
FROM PointCount AS p
WHERE p.Hits = (SELECT MAX(Hits) FROM PointCount);
