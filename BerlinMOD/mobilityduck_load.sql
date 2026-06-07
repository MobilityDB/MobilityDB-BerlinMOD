/*-----------------------------------------------------------------------------
-- BerlinMOD Load — MobilityDuck (DuckDB)
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

-------------------------------------------------------------------------------

Loads the cross-platform BerlinMOD CSV dataset into the canonical schema the
portable R-queries expect, directly under the canonical table and column names
(Trips, Vehicles, Licences, Instants, Periods, Points, Regions and their *1
parameter subsets) — the same schema berlinmod_load.sql builds for MobilityDB.
No native-to-canonical renaming layer is used.

Set the dataset directory (with a trailing slash) then run this file:
    SET VARIABLE data_dir = '/path/to/berlinmod-3db-canonical/';
    .read mobilityduck_load.sql
-----------------------------------------------------------------------------*/

LOAD mobilityduck;
INSTALL spatial; LOAD spatial;

-- Vehicles
CREATE OR REPLACE TABLE Vehicles AS
  SELECT vehid AS VehicleId, licence AS Licence, type AS VehicleType, model AS Model
  FROM read_csv(getvariable('data_dir') || 'vehicles.csv', header = true);

-- Trips: Trip (tgeompoint, SRID 3857) from the EWKB hex; the th3index cell
-- index trip_h3 is COMPUTED here (H3 res 7) from the trip, not materialized.
CREATE OR REPLACE TABLE Trips AS
  SELECT tripid AS TripId, vehid AS VehicleId,
         tgeompointFromHexEWKB(trip) AS Trip
  FROM read_csv(getvariable('data_dir') || 'trips.csv', header = true, all_varchar = true);
ALTER TABLE Trips ADD COLUMN trip_h3 TH3INDEX;
UPDATE Trips SET trip_h3 = th3index(transform(Trip, 4326), 7);
CREATE OR REPLACE VIEW Trips1 AS SELECT * FROM Trips LIMIT 100;

-- Licences: the cross-platform export carries only the query-parameter licences;
-- join to Vehicles to recover VehicleId.
CREATE OR REPLACE TABLE QueryLicences AS
  SELECT licenceid AS LicenceId, licence AS Licence
  FROM read_csv(getvariable('data_dir') || 'query_licences.csv', header = true);
CREATE OR REPLACE VIEW Licences AS
  SELECT q.LicenceId, q.Licence, v.VehicleId
  FROM QueryLicences q JOIN Vehicles v ON q.Licence = v.Licence;
CREATE OR REPLACE VIEW Licences1 AS SELECT * FROM Licences ORDER BY LicenceId LIMIT 10;
CREATE OR REPLACE VIEW Licences2 AS SELECT * FROM Licences ORDER BY LicenceId LIMIT 10 OFFSET 10;

-- Instants
CREATE OR REPLACE TABLE Instants AS
  SELECT instantid AS InstantId, instant::TIMESTAMPTZ AS Instant
  FROM read_csv(getvariable('data_dir') || 'query_instants.csv', header = true, all_varchar = true);
CREATE OR REPLACE VIEW Instants1 AS SELECT * FROM Instants ORDER BY InstantId LIMIT 10;

-- Periods
CREATE OR REPLACE TABLE Periods AS
  SELECT periodid AS PeriodId, period::TSTZSPAN AS Period
  FROM read_csv(getvariable('data_dir') || 'query_periods.csv', header = true, all_varchar = true);
CREATE OR REPLACE VIEW Periods1 AS SELECT * FROM Periods ORDER BY PeriodId LIMIT 10;

-- Points (SRID 3857)
CREATE OR REPLACE TABLE Points AS
  SELECT pointid AS PointId, ST_GeomFromText(geom) AS Geom
  FROM read_csv(getvariable('data_dir') || 'query_points.csv', header = true, all_varchar = true);
CREATE OR REPLACE VIEW Points1 AS SELECT * FROM Points ORDER BY PointId LIMIT 10;

-- Regions: the export keeps polygons in EPSG:4326; reproject to EPSG:3857 to
-- match Trips and Points.
CREATE OR REPLACE TABLE Regions AS
  SELECT regionid AS RegionId,
         ST_Transform(ST_GeomFromText(geom), 'EPSG:4326', 'EPSG:3857', always_xy := true) AS Geom
  FROM read_csv(getvariable('data_dir') || 'query_regions.csv', header = true, all_varchar = true);
CREATE OR REPLACE VIEW Regions1 AS SELECT * FROM Regions ORDER BY RegionId LIMIT 10;
