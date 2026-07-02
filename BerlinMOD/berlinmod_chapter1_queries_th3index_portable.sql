/*-----------------------------------------------------------------------------
-- BerlinMOD Chapter 1 Queries -- Portable Dialect with th3index Prefilter
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

This is a th3index-accelerated sibling of
`berlinmod_chapter1_queries_portable.sql`.  The published portable
file is kept unchanged so the book's listings remain authoritative.
This variant adds an H3-cell prefilter on top of the same predicates
for the cross-platform edge-to-cloud benchmark.

Prerequisites:
  - Run `berlinmod_th3index_setup.sql` once to add and populate the
    `trip_h3 th3index` column and its GiST index.
  - The canonical `geoToH3IndexSet(geometry, integer)` builder and the
    `eEq(h3indexset, th3index)` cell-overlap predicate must be installed.

Prefilter pattern (single recipe across geometry types):

  eEq(geoToH3IndexSet(G, 7), T.trip_h3)
    AND <semantic predicate on the moving point>

  `geoToH3IndexSet` accepts any GEOMETRY (POINT/LINESTRING/
  POLYGON/MULTI*/GeometryCollection), building an H3 cell set at the chosen
  resolution; `eEq(h3indexset, th3index)` returns TRUE iff the trip's
  th3index path ever lies in any of those cells.  The prefilter is
  sound for `eIntersects`/`eContains`/spatial-overlap predicates at
  any resolution — a trip can only satisfy them if it ever crosses a
  cell that covers part of the static geometry.

  On MobilityDB the GiST index on `Trips(trip_h3)` accelerates the
  prefilter.  On DuckDB / Spark the column itself is the prefilter
  mechanism (no spatial index API).

The schema assumed is the same as the published portable file plus
the `trip_h3` column added by the setup script:
  Trips(TripId, VehId, Trip tgeompoint, trip_h3 th3index,
        Licence, VehicleType, Model)
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

CREATE VIEW Trips100 AS ( SELECT * FROM Trips ORDER BY TripId LIMIT 100 );
CREATE VIEW Regions10 AS ( SELECT * FROM Regions ORDER BY RegionId LIMIT 10 );
CREATE VIEW Points10 AS ( SELECT * FROM Points ORDER BY PointId LIMIT 10 );
CREATE VIEW Periods10 AS ( SELECT * FROM Periods ORDER BY PeriodId LIMIT 10 );

-- Collect statistics

ANALYZE;

-- Show the timing of every query

\timing ON

-- Set the pager off

\pset pager 0


-- Range Queries

-- Q1. List the vehicles that have passed at a region from Regions.
--
-- Published portable: eIntersects(trajectory(T.Trip), R.Geom)
-- th3index-accel:     same + cell-set prefilter on R.Geom × T.trip_h3

\echo '-----------'
\echo '| Query 1 |'
\echo '-----------'

SELECT DISTINCT R.RegionId, T.VehId
FROM Trips T, Regions10 R
WHERE eEq(geoToH3IndexSet(R.Geom, 7), T.trip_h3)
  AND eIntersects(trajectory(T.Trip), R.Geom)
ORDER BY R.RegionId, T.VehId;

-- Q2. List the vehicles that were within a region from Regions during a period
-- from Periods.
--
-- Published portable: eIntersects(atTime(T.Trip, P.Period), R.Geom)
-- th3index-accel:     same + cell-set prefilter on R.Geom × T.trip_h3
--
-- The prefilter compares the full trip's th3index to the region cell
-- set — sound because if the trip clipped to P.Period intersects R.Geom,
-- then the unclipped trip's cells also intersect.  Refinement by Period
-- is left to the `atTime` call.

\echo '-----------'
\echo '| Query 2 |'
\echo '-----------'

SELECT R.RegionId, P.PeriodId, T.VehId
FROM Trips T, Regions10 R, Periods10 P
WHERE eEq(geoToH3IndexSet(R.Geom, 7), T.trip_h3)
  AND eIntersects(atTime(T.Trip, P.Period), R.Geom)
ORDER BY R.RegionId, P.PeriodId, T.VehId;

-- Q3. List the pairs of vehicles that were both located within a region from
-- Regions during a period from Periods.
--
-- Published portable: eIntersects on both trips against R.Geom
-- th3index-accel:     same + cell-set prefilter on BOTH trips

\echo '-----------'
\echo '| Query 3 |'
\echo '-----------'

SELECT DISTINCT T1.VehId AS VehId1, T2.VehId AS VehId2, R.RegionId, P.PeriodId
FROM Trips T1, Trips100 T2, Regions10 R, Periods10 P
WHERE T1.VehId < T2.VehId
  AND eEq(geoToH3IndexSet(R.Geom, 7), T1.trip_h3)
  AND eEq(geoToH3IndexSet(R.Geom, 7), T2.trip_h3)
  AND eIntersects(atTime(T1.Trip, P.Period), R.Geom)
  AND eIntersects(atTime(T2.Trip, P.Period), R.Geom)
ORDER BY T1.VehId, T2.VehId, R.RegionId, P.PeriodId;

-- Q4. List the first time at which a vehicle visited a point in Points.
--
-- Published portable: eContains(trajectory(T.Trip), P.Geom)
-- th3index-accel:     same + cell-set prefilter on P.Geom × T.trip_h3
--                     (POINT geometry → singleton cell set)

\echo '-----------'
\echo '| Query 4 |'
\echo '-----------'

SELECT T.VehId, P.PointId,
  MIN(startTimestamp(atValues(T.Trip, P.Geom))) AS Instant
FROM Trips T, Points10 P
WHERE eEq(geoToH3IndexSet(P.Geom, 7), T.trip_h3)
  AND eContains(trajectory(T.Trip), P.Geom)
GROUP BY T.VehId, P.PointId;

-- Temporal Aggregate Queries

-- Q5. Compute how many vehicles were active at each period in Periods.
--
-- Time-only query — no spatial geometry to prefilter against.  Identical
-- to the published portable form.

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
-- Published portable: eIntersects(trajectory(T.Trip), R.Geom) + atGeometry
-- th3index-accel:     same + cell-set prefilter on R.Geom × T.trip_h3

\echo '-----------'
\echo '| Query 6 |'
\echo '-----------'

SELECT R.RegionId,
  numInstants(wCount(atGeometry(T.Trip, R.Geom), interval '10 min'))
FROM Trips T, Regions10 R
WHERE eEq(geoToH3IndexSet(R.Geom, 7), T.trip_h3)
  AND eIntersects(trajectory(T.Trip), R.Geom)
GROUP BY R.RegionId
HAVING wCount(atGeometry(T.Trip, R.Geom), interval '10 min') IS NOT NULL
ORDER BY R.RegionId;

\echo '-----------'
\echo '| The End |'
\echo '-----------'
