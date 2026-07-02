/*-----------------------------------------------------------------------------
-- BerlinMOD Export
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

-----------------------------------------------------------------------------*/

/******************************************************************************
 * Exports the Brussels synthetic dataset obtained from the BerlinMOD generator
 * in CSV format
 * https://github.com/MobilityDB/MobilityDB-BerlinMOD
 * into MobilityDB using projected (2D) coordinates with SRID 3857
 * https://epsg.io/3857
 * Parameters:
 * - fullpath: states the full path in which the CSV files are located.
 * - gist: states whether GiST or SP-GiST indexes are created on the tables.
 *     By default it is set to TRUE and thus creates GiST indexes.
 * The following files are exported to the given path
 * - instants.csv
 * - periods.csv
 * - points.csv
 * - regions.csv
 * - vehicles.csv
 * - licences.csv
 * - municipalities.csv
 * - roadsegments.csv
 * - tripsinput.csv
 *
 * Example of usage using psql on a database with the BerlinMOD generated data:
 *     \i berlinmod_export.sql
 *     SELECT berlinmod_export('/home/mobilitydb/data/');
 *****************************************************************************/

DROP FUNCTION IF EXISTS berlinmod_export;
CREATE OR REPLACE FUNCTION berlinmod_export(fullpath text)
RETURNS text AS $$
DECLARE
  startTime timestamptz;
  endTime timestamptz;
BEGIN
--------------------------------------------------------------

  startTime = clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Exporting synthetic data from the BerlinMOD data generator';
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO '------------------------------------------------------------------';

--------------------------------------------------------------

  RAISE INFO 'Exporting table Instants';
  EXECUTE format('COPY (SELECT InstantId, Instant FROM Instants ORDER BY InstantId)
    TO ''%sinstants.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Periods';
  EXECUTE format('COPY (SELECT PeriodId, Period FROM Periods ORDER BY PeriodId)
    TO ''%speriods.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Points';
  EXECUTE format('COPY (SELECT PointId, Geom FROM Points ORDER BY PointId)
  TO ''%spoints.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Regions';
  EXECUTE format('COPY (SELECT RegionId, Geom FROM Regions ORDER BY RegionId)
  TO ''%sregions.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Vehicles';
  EXECUTE format('COPY (SELECT VehicleId, Licence, VehicleType, Model FROM Vehicles ORDER BY VehicleId)
  TO ''%svehicles.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Licences';
  EXECUTE format('COPY (SELECT LicenceId, Licence, VehicleId FROM Licences ORDER BY LicenceId)
  TO ''%slicences.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Municipalities';
  EXECUTE format('COPY (SELECT MunicipalityId, MunicipalityName, Population,
    PercPop, PopDensityKm2, NoEnterp, PercEnterp, 
    ST_AsEWKT(MunicipalityGeo) AS MunicipalityGeo
    FROM Municipalities ORDER BY MunicipalityId)
  TO ''%smunicipalities.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table RoadSegments';
  EXECUTE format('COPY (SELECT * FROM RoadSegments ORDER BY SegmentId)
  TO ''%sroadsegments.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Trips transformed into TripsInput';
  EXECUTE format('COPY (
    WITH Instants(TripId, VehicleId, StartDate, SeqNo, Inst) AS (
      SELECT TripId, VehicleId, StartDate, SeqNo, unnest(instants(Trip))
      FROM Trips )
    SELECT TripId, VehicleId, StartDate, SeqNo, getValue(Inst) AS Point, 
      getTimestamp(Inst) AS T
    FROM Instants
    ORDER BY TripId, VehicleId, StartDate, SeqNo, T )
    TO ''%stripsinput.csv'' DELIMITER '','' CSV HEADER', fullpath);

--------------------------------------------------------------

  endTime = clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Execution finished at %', endTime;
  RAISE INFO 'Execution time %', endTime - startTime;
  RAISE INFO '------------------------------------------------------------------';

-------------------------------------------------------------------------------

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------

/******************************************************************************
 * Exports a cross-platform–compatible subset of the BerlinMOD dataset in CSV
 * format, suitable for loading into MobilityDuck (DuckDB) and MobilitySpark
 * (Apache Spark) using the portable SQL dialect (RFC #861).
 *
 * This is the SINGLE, self-sufficient generation entry point: it writes every
 * CSV the three platforms need, with all geometries reprojected to one SRID and
 * SRID-tagged, so the consumer repositories load the data directly with no
 * per-tool post-processing or reprojection.
 *
 * The CSV is RAW trajectory/geometry data only — no H3 columns. H3 is an index
 * each consumer builds at LOAD from the lat/lon data (th3index(trip,R) /
 * geoToH3IndexSet(geom,R)), exactly as a real ingest of raw GPS/AIS would: the
 * source never ships precomputed cells.
 *
 * Schema produced (all geometries in the chosen output SRID):
 *   vehicles.csv       : vehId, licence, type, model
 *   trips.csv          : tripId, vehId, trip       -- tgeompoint as hex-EWKB
 *                        (Extended WKB — SRID embedded, never lost).
 *   query_licences.csv : licenceId, licence
 *   query_instants.csv : instantId, instant
 *   query_points.csv   : pointId, geom             -- geom as EWKT (SRID-tagged)
 *   query_periods.csv  : periodId, period          -- tstzspan as text
 *   query_regions.csv  : regionId, geom            -- geom as EWKT (SRID-tagged)
 *
 * Parameters:
 * - fullpath:    directory path (with trailing slash) where CSV files are written.
 * - srid:        output SRID for the exported geometries (default 4326, WGS84).
 *                The trips and points are reprojected here, in PostgreSQL, and
 *                emitted as EWKT (SRID=N;...) so the SRID travels with the data
 *                and no consumer ever needs to reproject — useful for engines
 *                whose spatial layer cannot transform temporal geometries
 *                (e.g. DuckDB).  Choosing the SRID:
 *                  - 3812  ETRS89 / Belgian Lambert 2008 — the current official
 *                          Brussels metric CRS.  True metres ⇒ the 3 m / 10 m
 *                          dWithin thresholds are correct.  RECOMMENDED for
 *                          planar-metre distance queries (supersedes the legacy
 *                          31370 / Belgian Lambert 72).
 *                  - 3857  Web Mercator — the SOURCE CRS, so srid := 3857 is a
 *                          no-op (zero reprojection).  But Mercator scale grows
 *                          as 1/cos(lat) ≈ 1.58 at Brussels (≈50.8°N), so the
 *                          thresholds are ~58 % larger than true ground metres.
 *                          Fine for cross-engine PARITY (all engines agree),
 *                          not for metric truth.
 *                  - 4326  geographic (default) — distances need a geodetic
 *                          (geography) reading; on planar engines they are
 *                          degrees, not metres.  Use a projected SRID instead.
 *
 * Example:
 *     \i berlinmod_export.sql
 *     SELECT berlinmod_portability_export('/home/mobilitydb/portability/');
 *     SELECT berlinmod_portability_export('/home/mobilitydb/portability/', 3812);
 *****************************************************************************/

DROP FUNCTION IF EXISTS berlinmod_portability_export;
CREATE OR REPLACE FUNCTION berlinmod_portability_export(
    fullpath      text,
    srid          integer DEFAULT 4326)
RETURNS text AS $$
DECLARE
  startTime timestamptz;
  endTime   timestamptz;
BEGIN
  startTime = clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Exporting BerlinMOD data in cross-platform portability schema';
  RAISE INFO 'Target: %', fullpath;
  RAISE INFO 'Output SRID: % (geometries reprojected from the source CRS)', srid;
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO '------------------------------------------------------------------';

  RAISE INFO 'Exporting vehicles.csv';
  EXECUTE format(
    'COPY (SELECT VehicleId AS vehId, Licence AS licence,
                  VehicleType AS type, Model AS model
           FROM Vehicles ORDER BY VehicleId)
     TO ''%svehicles.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting trips.csv (trip as hex-EWKB in SRID %)', srid;
  -- Reproject the trip to the requested output SRID and serialise it as hex-EWKB
  -- (Extended WKB) so the SRID travels embedded in the binary: consumers load it
  -- already in the target CRS.  The CSV carries only the RAW trajectory — no H3
  -- column.  H3 is an index every consumer builds at LOAD from the lat/lon data
  -- (th3index(trip, R)), exactly as a real ingest of raw GPS/AIS would: the
  -- source never ships precomputed cells.
  EXECUTE format(
    'COPY (SELECT TripId AS tripId, VehicleId AS vehId,
                  asHexEWKB(transform(Trip, %s)) AS trip
           FROM Trips ORDER BY TripId)
     TO ''%strips.csv'' DELIMITER '','' CSV HEADER', srid, fullpath);

  RAISE INFO 'Exporting query_licences.csv';
  EXECUTE format(
    'COPY (SELECT LicenceId AS licenceId, Licence AS licence
           FROM Licences ORDER BY LicenceId)
     TO ''%squery_licences.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting query_instants.csv';
  EXECUTE format(
    'COPY (SELECT InstantId AS instantId, Instant AS instant
           FROM Instants ORDER BY InstantId)
     TO ''%squery_instants.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting query_points.csv (geometry as EWKT in SRID %)', srid;
  -- Raw geometry only; the geom_h3 cell-set index is built at LOAD from this
  -- lat/lon geometry (geoToH3IndexSet(geom, R)), like trip_h3.
  EXECUTE format(
    'COPY (SELECT PointId AS pointId, asEWKT(transform(Geom, %s)) AS geom
           FROM Points ORDER BY PointId)
     TO ''%squery_points.csv'' DELIMITER '','' CSV HEADER', srid, fullpath);

  RAISE INFO 'Exporting query_periods.csv';
  EXECUTE format(
    'COPY (SELECT PeriodId AS periodId, Period::text AS period
           FROM Periods ORDER BY PeriodId)
     TO ''%squery_periods.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting query_regions.csv (geometry as EWKT in SRID %)', srid;
  -- Raw geometry only; geom_h3 is built at LOAD like the point geom_h3.
  EXECUTE format(
    'COPY (SELECT RegionId AS regionId, asEWKT(transform(Geom, %s)) AS geom
           FROM Regions ORDER BY RegionId)
     TO ''%squery_regions.csv'' DELIMITER '','' CSV HEADER', srid, fullpath);

  endTime = clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Execution finished at %', endTime;
  RAISE INFO 'Execution time %', endTime - startTime;
  RAISE INFO '------------------------------------------------------------------';
  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
