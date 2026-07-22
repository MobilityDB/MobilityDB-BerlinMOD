--------------------------------------------------------------------------------
-- BerlinMOD Batch Load — MobilityDuck / DuckDB
--------------------------------------------------------------------------------
--
-- This file is part of MobilityDB.
-- Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
-- contributors
--
--------------------------------------------------------------------------------
--
-- Loads the cross-platform BerlinMOD portability export (produced by
-- berlinmod_portability_export() in berlinmod_export.sql) into the schema the
-- shared batch queries (bench/queries.sql) expect — the SAME CSV set the
-- MobilityDB and MobilitySpark runners consume, so all platforms benchmark the
-- identical corpus.
--
-- Input CSVs (DATADIR is injected by bench_mduck.sh):
--   vehicles.csv       : vehId, licence, type, model
--   trips.csv          : tripId, vehId, trip        -- tgeompoint as hex-EWKB
--   query_licences.csv : licenceId, licence
--   query_instants.csv : instantId, instant
--   query_points.csv   : pointId, geom             -- geometry as EWKT
--   query_periods.csv  : periodId, period          -- tstzspan as text
--   query_regions.csv  : regionId, geom            -- geometry as EWKT
--
-- SRID is carried by the serialization and resolved by MEOS parsers — never
-- parsed or reprojected by hand: the trip comes as hex-EWKB
-- (tgeompointFromHexEWKB) and the static geometries as SRID-tagged EWKT
-- (ST_GeomFromText accepts the SRID= prefix).  The th3index prefilter column is
-- built in-engine from the lat/lon trajectory, exactly as a real ingest would.
--------------------------------------------------------------------------------

-- Default for standalone use; bench_mduck.sh strips this line and injects the
-- real data directory.
SET VARIABLE DATADIR='./data/';

-- The runner selects `SET search_path='portable,main'`; create the schema so that
-- selection resolves (the tables live in main and are found via the fallback).
CREATE SCHEMA IF NOT EXISTS portable;

-- ── Vehicles ──────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE Vehicles AS
  SELECT vehId, licence, type, model
  FROM read_csv(getvariable('DATADIR') || 'vehicles.csv', header = true);

-- ── Trips ─────────────────────────────────────────────────────────────────────
-- trip arrives as hex-EWKB (SRID embedded); trip_h3 is computed here.
CREATE OR REPLACE TABLE Trips AS
  SELECT tripId, vehId, tgeompointFromHexEWKB(trip) AS trip
  FROM read_csv(getvariable('DATADIR') || 'trips.csv', header = true, all_varchar = true);
ALTER TABLE Trips ADD COLUMN trip_h3 TH3INDEX;
UPDATE Trips SET trip_h3 = th3index(transform(trip, 4326), 7);

-- ── Query parameter tables (100-row selectors used by queries.sql) ────────────
CREATE OR REPLACE TABLE QueryLicences AS
  SELECT licenceId, licence
  FROM read_csv(getvariable('DATADIR') || 'query_licences.csv', header = true);

CREATE OR REPLACE TABLE QueryInstants AS
  SELECT instantId, instant::TIMESTAMPTZ AS instant
  FROM read_csv(getvariable('DATADIR') || 'query_instants.csv', header = true, all_varchar = true);

CREATE OR REPLACE TABLE QueryPeriods AS
  SELECT periodId, period::TSTZSPAN AS period
  FROM read_csv(getvariable('DATADIR') || 'query_periods.csv', header = true, all_varchar = true);

-- Points carry both the parsed geometry and the SRID-tagged EWKT text
-- (queries.sql surfaces p.geomWKT); geomWKT is the export text verbatim.
CREATE OR REPLACE TABLE QueryPoints AS
  SELECT pointId, ST_GeomFromText(geom) AS geom, geom AS geomWKT
  FROM read_csv(getvariable('DATADIR') || 'query_points.csv', header = true, all_varchar = true);

CREATE OR REPLACE TABLE QueryRegions AS
  SELECT regionId, ST_GeomFromText(geom) AS geom
  FROM read_csv(getvariable('DATADIR') || 'query_regions.csv', header = true, all_varchar = true);
