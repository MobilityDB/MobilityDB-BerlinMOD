/*-----------------------------------------------------------------------------
-- MobilityDuck — Schema adapter for the portable R-queries
-------------------------------------------------------------------------------

Bridges the cross-platform CSV-loaded BerlinMOD schema to the canonical
column names the portable R-queries expect.  Run once before sourcing
`berlinmod_r_queries_portable.sql`.

Native cross-platform schema:
  Trips(tripid, vehid, trip)
  Vehicles(vehid, licence, type, model)
  QueryLicences(licenceid, licence)
  QueryInstants(instantid, instant)
  QueryPeriods(periodid, period)
  QueryPoints(pointid, geom, geomwkt)
  QueryRegions(regionid, geom, geomwkt)

What the portable R-queries expect:
  Trips(TripId, VehicleId, Trip)
  Vehicles(VehicleId, Licence, VehicleType, Model)
  Licences(LicenceId, Licence, VehicleId)
  Licences1, Licences2 (subsets)
  Instants1(InstantId, Instant)
  Periods1(PeriodId, Period)
  Points, Points1(PointId, Geom)
  Regions, Regions1(RegionId, Geom)

The adapter creates views with the canonical names.  Columns map
case-insensitively in DuckDB / MobilityDB / MobilitySpark, so the
portable SQL's `T.VehicleId` reads as `t.vehid` on each platform.

-----------------------------------------------------------------------------*/

-- Preparation: rename the loaded tables to expose canonical column
-- names through views.  Run once on a fresh load:
--   ALTER TABLE Vehicles RENAME TO VehiclesRaw;
--   ALTER TABLE Trips    RENAME TO TripsRaw;

-- Vehicles: view exposing canonical names.
DROP VIEW IF EXISTS Vehicles;
CREATE VIEW Vehicles AS
  SELECT vehid AS VehicleId, licence AS Licence,
         type AS VehicleType, model AS Model
  FROM VehiclesRaw;

-- Trips: view exposing canonical names.  No StartDate / SeqNo / Trajectory
-- in the cross-platform schema; the R-queries that reference them get a
-- NULL but the predicates that matter (vehicle / trip) still resolve.
DROP VIEW IF EXISTS Trips;
CREATE VIEW Trips AS
  SELECT tripid AS TripId, vehid AS VehicleId, trip AS Trip
  FROM TripsRaw;

-- Licences: the full canonical PG table is the parameter set
-- `QueryLicences` here (the cross-platform export does not retain
-- the wider Licences table; it carries only the query parameters).
DROP VIEW IF EXISTS Licences;
CREATE VIEW Licences AS
  SELECT q.licenceid AS LicenceId, q.licence AS Licence, v.vehid AS VehicleId
  FROM QueryLicences q JOIN VehiclesRaw v ON q.licence = v.licence;

-- Licences1, Licences2: subsets via QueryLicences (parameter sample).
DROP VIEW IF EXISTS Licences1;
CREATE VIEW Licences1 AS
  SELECT q.licenceid AS LicenceId, q.licence AS Licence, v.vehid AS VehicleId
  FROM QueryLicences q JOIN VehiclesRaw v ON q.licence = v.licence
  ORDER BY q.licenceid LIMIT 10;

DROP VIEW IF EXISTS Licences2;
CREATE VIEW Licences2 AS
  SELECT q.licenceid AS LicenceId, q.licence AS Licence, v.vehid AS VehicleId
  FROM QueryLicences q JOIN VehiclesRaw v ON q.licence = v.licence
  ORDER BY q.licenceid OFFSET 10 LIMIT 10;

-- Instants, Instants1
DROP VIEW IF EXISTS Instants;
CREATE VIEW Instants AS
  SELECT instantid AS InstantId, instant AS Instant FROM QueryInstants;
DROP VIEW IF EXISTS Instants1;
CREATE VIEW Instants1 AS
  SELECT instantid AS InstantId, instant AS Instant
  FROM QueryInstants ORDER BY instantid LIMIT 10;

-- Periods, Periods1
DROP VIEW IF EXISTS Periods;
CREATE VIEW Periods AS
  SELECT periodid AS PeriodId, period AS Period FROM QueryPeriods;
DROP VIEW IF EXISTS Periods1;
CREATE VIEW Periods1 AS
  SELECT periodid AS PeriodId, period AS Period
  FROM QueryPeriods ORDER BY periodid LIMIT 10;

-- Points, Points1
DROP VIEW IF EXISTS Points;
CREATE VIEW Points AS
  SELECT pointid AS PointId, geom AS Geom FROM QueryPoints;
DROP VIEW IF EXISTS Points1;
CREATE VIEW Points1 AS
  SELECT pointid AS PointId, geom AS Geom
  FROM QueryPoints ORDER BY pointid LIMIT 10;

-- Regions, Regions1: the cross-platform export keeps polygons in
-- EPSG:4326 while Trips and Points are reprojected to EPSG:3857 at
-- load time.  Match the SRID of Trips for spatial predicates.
DROP VIEW IF EXISTS Regions;
CREATE VIEW Regions AS
  SELECT regionid AS RegionId,
         ST_Transform(geom, 'EPSG:4326', 'EPSG:3857', always_xy := true) AS Geom
  FROM QueryRegions;
DROP VIEW IF EXISTS Regions1;
CREATE VIEW Regions1 AS
  SELECT regionid AS RegionId,
         ST_Transform(geom, 'EPSG:4326', 'EPSG:3857', always_xy := true) AS Geom
  FROM QueryRegions ORDER BY regionid LIMIT 10;
