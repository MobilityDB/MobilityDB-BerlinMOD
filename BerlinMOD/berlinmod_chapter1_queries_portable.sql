/*-----------------------------------------------------------------------------
-- BerlinMOD Chapter 1 Queries -- Portable Dialect
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

This file contains a portable version of the BerlinMOD Chapter 1 queries
(Q1-Q6) using named functions instead of MobilityDB-specific infix operators.
It targets the cross-platform SQL dialect established for the edge-to-cloud
portability initiative, compatible with MobilityDB (PostgreSQL) and
MobilityDuck (DuckDB).

Operator-to-function mapping applied:
  &&  (stbox overlap)    -> eIntersects() or overlaps()
  @>  (contains)        -> contains()
  <-> (distance)        -> distance()

The schema assumed is:
  Trips(TripId, VehId, Trip tgeompoint, Licence, VehicleType, Model)
  Vehicles(VehicleId, Licence, VehicleType, Model)
  Points(PointId, PosX, PosY, Geom geometry(Point,3857))
  Regions(RegionId, RegionName, Geom geometry(Polygon,3857))
  Instants(InstantId, Instant timestamptz)
  Periods(PeriodId, Period tstzspan)

-----------------------------------------------------------------------------*/

-- Create indexes

DROP INDEX IF EXISTS Trips_rtree_idx;
DROP INDEX IF EXISTS Regions_rtree_idx;
DROP INDEX IF EXISTS Periods_rtree_idx;

CREATE INDEX Trips_rtree_idx ON Trips USING GIST(trip);
CREATE INDEX Regions_rtree_idx ON Regions USING GIST(geom);
CREATE INDEX Periods_rtree_idx ON Periods USING GIST(period);

-- Create views selecting a small sample for the query parameters to minimize
-- execution time during testing

DROP VIEW IF EXISTS Trips100;
DROP VIEW IF EXISTS Regions10;
DROP VIEW IF EXISTS Points10;
DROP VIEW IF EXISTS Periods10;

CREATE VIEW Trips100 AS ( SELECT * FROM Trips LIMIT 100 );
CREATE VIEW Regions10 AS ( SELECT * FROM Regions LIMIT 10 );
CREATE VIEW Points10 AS ( SELECT * FROM Points LIMIT 10 );
CREATE VIEW Periods10 AS ( SELECT * FROM Periods LIMIT 10 );

-- Collect statistics

ANALYZE;

-- Show the timing of every query

\timing ON

-- Set the pager off

\pset pager 0


-- Range Queries

-- Q1. List the vehicles that have passed at a region from Regions.
--
-- Original:  stbox(T.Trip) && stbox(R.Geom) AND ST_Intersects(trajectory(T.Trip), R.Geom)
-- Portable:  eIntersects(trajectory(T.Trip), R.Geom) subsumes the bbox filter

\echo '-----------'
\echo '| Query 1 |'
\echo '-----------'

SELECT DISTINCT R.RegionId, T.VehId
FROM Trips T, Regions10 R
WHERE eIntersects(trajectory(T.Trip), R.Geom)
ORDER BY R.RegionId, T.VehId;

-- Q2. List the vehicles that were within a region from Regions during a period
-- from Periods.
--
-- Original:  T.Trip && stbox(R.Geom, P.Period) AND eintersects(atTime(T.Trip, P.Period), R.Geom)
-- Portable:  eIntersects(atTime(T.Trip, P.Period), R.Geom)

\echo '-----------'
\echo '| Query 2 |'
\echo '-----------'

SELECT R.RegionId, P.PeriodId, T.VehId
FROM Trips T, Regions10 R, Periods10 P
WHERE eIntersects(atTime(T.Trip, P.Period), R.Geom)
ORDER BY R.RegionId, P.PeriodId, T.VehId;

-- Q3. List the pairs of vehicles that were both located within a region from
-- Regions during a period from Periods.
--
-- Original:  T1.Trip && stbox(...) AND eintersects(atTime(T1.Trip, P.Period), R.Geom) AND ...
-- Portable:  eIntersects(atTime(...), R.Geom) for both trips

\echo '-----------'
\echo '| Query 3 |'
\echo '-----------'

SELECT DISTINCT T1.VehId AS VehId1, T2.VehId AS VehId2, R.RegionId, P.PeriodId
FROM Trips T1, Trips100 T2, Regions10 R, Periods10 P
WHERE T1.VehId < T2.VehId
  AND eIntersects(atTime(T1.Trip, P.Period), R.Geom)
  AND eIntersects(atTime(T2.Trip, P.Period), R.Geom)
ORDER BY T1.VehId, T2.VehId, R.RegionId, P.PeriodId;

-- Q4. List the first time at which a vehicle visited a point in Points.
--
-- Original:  T.Trip && stbox(P.Geom) AND ST_Contains(trajectory(T.Trip), P.Geom)
-- Portable:  eContains(trajectory(T.Trip), P.Geom)

\echo '-----------'
\echo '| Query 4 |'
\echo '-----------'

SELECT T.VehId, P.PointId,
  MIN(startTimestamp(atValues(T.Trip, P.Geom))) AS Instant
FROM Trips T, Points10 P
WHERE eContains(trajectory(T.Trip), P.Geom)
GROUP BY T.VehId, P.PointId;

-- Temporal Aggregate Queries

-- Q5. Compute how many vehicles were active at each period in Periods.
--
-- The overlap operator && on tstzspan is standard across platforms.

\echo '-----------'
\echo '| Query 5 |'
\echo '-----------'

SELECT P.PeriodId, COUNT(*),
  numInstants(tcount(atTime(T.Trip, P.Period)))
FROM Trips T, Periods10 P
WHERE timeSpan(T.Trip) && P.Period
GROUP BY P.PeriodId
ORDER BY P.PeriodId;

-- Q6. For each region in Regions, give the window temporal count of trips
-- with a 10-minute interval.
--
-- atGeometry and wCount are available in MobilityDB and MobilityDuck.

\echo '-----------'
\echo '| Query 6 |'
\echo '-----------'

SELECT R.RegionId,
  numInstants(wCount(atGeometry(T.Trip, R.Geom), interval '10 min'))
FROM Trips T, Regions10 R
WHERE eIntersects(trajectory(T.Trip), R.Geom)
GROUP BY R.RegionId
HAVING wCount(atGeometry(T.Trip, R.Geom), interval '10 min') IS NOT NULL
ORDER BY R.RegionId;

\echo '-----------'
\echo '| The End |'
\echo '-----------'
