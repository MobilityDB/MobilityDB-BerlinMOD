/*-----------------------------------------------------------------------------
-- BerlinMOD Batch Load — MobilityDB / PostgreSQL
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

-------------------------------------------------------------------------------

Loads the cross-platform BerlinMOD portability export (produced by
berlinmod_portability_export() in berlinmod_export.sql) into the schema the
shared batch queries (bench/queries.sql) expect.  The same CSV set feeds the
MobilityDuck and MobilitySpark runners, so all three platforms benchmark the
identical corpus with no per-tool reprojection or post-processing.

Input CSVs (DATADIR is substituted by bench_mbdb.sh):
    vehicles.csv       : vehId, licence, type, model
    trips.csv          : tripId, vehId, trip        -- tgeompoint as hex-EWKB
    query_licences.csv : licenceId, licence
    query_instants.csv : instantId, instant
    query_points.csv   : pointId, geom              -- geometry as EWKT
    query_periods.csv  : periodId, period           -- tstzspan as text
    query_regions.csv  : regionId, geom             -- geometry as EWKT

The export SRID-tags every geometry (default 4326), so the H3 prefilter
th3index(trip, 7) / geoToH3IndexSet(geom, 7) reads lat/lon directly, exactly as
a real ingest of raw GPS/AIS would build its cell index at load time.
-----------------------------------------------------------------------------*/

\set ON_ERROR_STOP on
\timing off

CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

-- ── Vehicles ─────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS Vehicles CASCADE;
CREATE TABLE Vehicles (
  vehId   integer PRIMARY KEY,
  licence text,
  type    text,
  model   text
);
COPY Vehicles(vehId, licence, type, model)
  FROM 'DATADIR/vehicles.csv' DELIMITER ',' CSV HEADER;

-- ── Trips ────────────────────────────────────────────────────────────────────
-- trip arrives as hex-EWKB (SRID embedded); the th3index prefilter column is
-- built here from the lat/lon trajectory, not shipped in the CSV.
DROP TABLE IF EXISTS Trips CASCADE;
DROP TABLE IF EXISTS TripsInput CASCADE;
CREATE TABLE TripsInput (tripId integer, vehId integer, trip text);
COPY TripsInput(tripId, vehId, trip)
  FROM 'DATADIR/trips.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE Trips AS
  SELECT tripId, vehId, tgeompointFromHexEWKB(trip) AS trip
  FROM   TripsInput;
DROP TABLE TripsInput;

ALTER TABLE Trips ADD PRIMARY KEY (tripId);
ALTER TABLE Trips ADD COLUMN trip_h3 th3index;
UPDATE Trips SET trip_h3 = th3index(transform(trip, 4326), 7);

-- ── Query parameter tables (100-row selectors used by queries.sql) ───────────
DROP TABLE IF EXISTS QueryLicences CASCADE;
CREATE TABLE QueryLicences (licenceId integer PRIMARY KEY, licence text);
COPY QueryLicences(licenceId, licence)
  FROM 'DATADIR/query_licences.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS QueryInstants CASCADE;
CREATE TABLE QueryInstants (instantId integer PRIMARY KEY, instant timestamptz);
COPY QueryInstants(instantId, instant)
  FROM 'DATADIR/query_instants.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS QueryPeriods CASCADE;
CREATE TABLE QueryPeriods (periodId integer PRIMARY KEY, period tstzspan);
COPY QueryPeriods(periodId, period)
  FROM 'DATADIR/query_periods.csv' DELIMITER ',' CSV HEADER;

-- Points carry both the parsed geometry (for the spatial predicate) and the
-- SRID-tagged EWKT text (queries.sql surfaces p.geomWKT in the result
-- projection).  The exported geom column is already asEWKT output, so geomWKT
-- is that text verbatim — no ST_AsText round-trip.
DROP TABLE IF EXISTS QueryPoints CASCADE;
DROP TABLE IF EXISTS QueryPointsInput CASCADE;
CREATE TABLE QueryPointsInput (pointId integer, geom text);
COPY QueryPointsInput(pointId, geom)
  FROM 'DATADIR/query_points.csv' DELIMITER ',' CSV HEADER;
CREATE TABLE QueryPoints AS
  SELECT pointId,
         ST_GeomFromEWKT(geom) AS geom,
         geom                  AS geomWKT
  FROM   QueryPointsInput;
DROP TABLE QueryPointsInput;

DROP TABLE IF EXISTS QueryRegions CASCADE;
DROP TABLE IF EXISTS QueryRegionsInput CASCADE;
CREATE TABLE QueryRegionsInput (regionId integer, geom text);
COPY QueryRegionsInput(regionId, geom)
  FROM 'DATADIR/query_regions.csv' DELIMITER ',' CSV HEADER;
CREATE TABLE QueryRegions AS
  SELECT regionId, ST_GeomFromEWKT(geom) AS geom
  FROM   QueryRegionsInput;
DROP TABLE QueryRegionsInput;

-- ── Indexes (names match the tier-drop logic in bench_mbdb.sh) ───────────────
CREATE INDEX Trips_Trip_gist_idx    ON Trips USING GIST(trip);
CREATE INDEX Trips_Trip_spgist_idx  ON Trips USING SPGIST(trip);
CREATE INDEX Trips_trip_h3_gist_idx ON Trips USING GIST(trip_h3);

ANALYZE;
