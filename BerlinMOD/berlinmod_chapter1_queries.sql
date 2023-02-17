-- Create indexes

DROP INDEX IF EXISTS Trips_rtree_idx;
DROP INDEX IF EXISTS Regions_rtree_idx;
DROP INDEX IF EXISTS Periods_rtree_idx;

CREATE INDEX Trips_rtree_idx ON Trips USING GIST(trip);
CREATE INDEX Regions_rtree_idx ON Regions USING GIST(geom);
CREATE INDEX Periods_rtree_idx ON Periods USING GIST(period);

-- Create views selecting 10 rows for the query parameters to minimize the excecution time

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
-- 1. List the vehicles that have passed at a region from Regions.

\echo '-----------'
\echo '| Query 1 |'
\echo '-----------'

SELECT DISTINCT R.RegionId, T.VehId
FROM Trips T, Regions10 R
WHERE stbox(T.Trip) && stbox(R.Geom) AND ST_Intersects(trajectory(T.Trip), R.Geom)
ORDER BY R.RegionId, T.VehId;

-- 2. List the vehicles that were within a region from Regions during a period from Periods.

\echo '-----------'
\echo '| Query 2 |'
\echo '-----------'

SELECT R.RegionId, P.PeriodId, T.VehId
FROM Trips T, Regions10 R, Periods10 P
WHERE T.Trip && stbox(R.Geom, P.Period) AND
  eintersects(atTime(T.Trip, P.Period), R.Geom)
ORDER BY R.RegionId, P.PeriodId, T.VehId;

-- 3. List the pairs of vehicles that were both located within a region from Regions during a period from Periods.

\echo '-----------'
\echo '| Query 3 |'
\echo '-----------'

SELECT DISTINCT T1.VehId AS VehId1, T2.VehId AS VehId2, R.RegionId, P.PeriodId
FROM Trips T1, Trips100 T2, Regions10 R, Periods10 P
WHERE T1.VehId < T2.VehId AND T1.Trip && stbox(R.Geom, P.Period) AND
  T2.Trip && stbox(R.Geom, P.Period) AND
  eintersects(atTime(T1.Trip, P.Period), R.Geom) AND
  eintersects(atTime(T2.Trip, P.Period), R.Geom)
ORDER BY T1.VehId, T2.VehId, R.RegionId, P.PeriodId;

-- 4. List the first time at which a vehicle visited a point in Points.

\echo '-----------'
\echo '| Query 4 |'
\echo '-----------'

SELECT T.VehId, P.PointId,
  MIN(startTimestamp(atValues(T.Trip, P.Geom))) AS Instant
FROM Trips T, Points10 P
WHERE T.Trip && stbox(P.Geom) AND ST_Contains(trajectory(T.Trip), P.Geom)
GROUP BY T.VehId, P.PointId;

-- Temporal Aggregate Queries

-- 5. Compute how many vehicles were active at each period in Periods.

\echo '-----------'
\echo '| Query 5 |'
\echo '-----------'

SELECT P.PeriodID, COUNT(*), 
  -- Query modified to show the number of instants to minimize the screen output
  -- TCOUNT(atTime(T.Trip, P.Period))
  numInstants(tcount(atTime(T.Trip, P.Period)))
FROM Trips T, Periods10 P
WHERE T.Trip && P.Period
GROUP BY P.PeriodID
ORDER BY P.PeriodID;

-- 6. For each region in Regions, give the window temporal count of trips with a 10-minute interval.

\echo '-----------'
\echo '| Query 6 |'
\echo '-----------'

SELECT R.RegionID, 
  -- Query modified to show the number of instants to minimize the screen output
  -- WCOUNT(atGeometry(T.Trip, R.Geom), interval '10 min')
  numInstants(WCOUNT(atGeometry(T.Trip, R.Geom), interval '10 min'))
FROM Trips T, Regions10 R
WHERE T.Trip && stbox(R.Geom)
GROUP BY R.RegionID
HAVING WCOUNT(atGeometry(T.Trip, R.Geom), interval '10 min') IS NOT NULL
ORDER BY R.RegionID;

-- 7. Count the number of trips that were active during each hour in June 2, 2020.

\echo '-----------'
\echo '| Query 7 |'
\echo '-----------'

WITH TimeSplit(Period) AS (
  SELECT span(H, H + interval '1 hour')
  FROM generate_series(timestamptz '2020-06-01 00:00:00',
    timestamptz '2020-06-01 23:00:00', interval '1 hour') AS H )
SELECT Period, COUNT(*)
FROM TimeSplit S, Trips T
WHERE S.Period && T.Trip AND atTime(Trip, Period) IS NOT NULL
GROUP BY S.Period
ORDER BY S.Period;

-- Distance Queries

-- 8. List the overall traveled distances of the vehicles during the periods from Periods.

\echo '-----------'
\echo '| Query 8 |'
\echo '-----------'

SELECT T.VehId, P.PeriodId, P.Period,
  SUM(length(atTime(T.Trip, P.Period))) AS Distance
FROM Trips T, Periods10 P
WHERE T.Trip && P.Period
GROUP BY T.VehId, P.PeriodId, P.Period
ORDER BY T.VehId, P.PeriodId;

-- 9. List the minimum distance ever between each vehicle and each point from Points.

\echo '-----------'
\echo '| Query 9 |'
\echo '-----------'

SELECT T.VehId, P.PointId, MIN(trajectory(T.Trip) <-> P.Geom) AS MinDistance
FROM Trips T, Points10 P
GROUP BY T.VehId, P.PointId
ORDER BY T.VehId, P.PointId;

-- 10. List the minimum temporal distance between each pair of vehicles.

\echo '------------'
\echo '| Query 10 |'
\echo '------------'

SELECT T1.VehId AS Car1Id, T2.VehId AS Car2Id,
  -- Query modified to show the number of instants to minimize the screen output
  -- tmin(T1.Trip <-> T2.Trip) AS MinDistance
  numInstants(tmin(T1.Trip <-> T2.Trip)) AS NumInstantsMinDistance
FROM Trips T1, Trips100 T2
WHERE T1.VehId < T2.VehId AND timeSpan(T1.Trip) && timeSpan(T2.Trip)
GROUP BY T1.VehId, T2.VehId
ORDER BY T1.VehId, T2.VehId;

-- 11. List the nearest approach time, distance, and shortest line between each pair of trips.

\echo '------------'
\echo '| Query 11 |'
\echo '------------'

SELECT T1.VehId AS Car1Id, T1.TripId AS Trip1Id, T2.VehId AS Car2Id,
  T2.TripId AS Trip2Id, getTime(NearestApproachInstant(T1.Trip, T2.Trip)) AS Time,
  NearestApproachDistance(T1.Trip, T2.Trip) AS Distance,
  ShortestLine(T1.Trip, T2.Trip) AS Line
FROM Trips T1, Trips10 T2
WHERE T1.VehId < T2.VehId AND timeSpan(T1.Trip) && timeSpan(T2.Trip)
ORDER BY T1.VehId, T1.TripId, T2.VehId, T2.TripId;

-- 12. List when and where a pairs of vehicles have been at 10 m or less from each other.

\echo '------------'
\echo '| Query 12 |'
\echo '------------'

SELECT T1.VehId AS VehId1, T2.VehId AS VehId2,
  whenTrue(tdwithin(T1.Trip, T2.Trip, 10.0)) AS PeriodSet
FROM Trips T1, Trips100 T2
WHERE T1.VehId < T2.VehId AND T1.Trip && expandSpace(T2.Trip, 10) AND
tdwithin(T1.Trip, T2.Trip, 10.0, TRUE) IS NOT NULL
ORDER BY T1.VehId, T2.VehId, PeriodSet;

\echo '-----------'
\echo '| The End |'
\echo '-----------'
