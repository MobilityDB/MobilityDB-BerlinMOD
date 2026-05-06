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
 * Schema produced:
 *   vehicles.csv       : vehId, licence, type, model
 *   trips.csv          : tripId, vehId, trip   -- tgeompoint as WKT text
 *   query_licences.csv : licenceId, licence
 *   query_instants.csv : instantId, instant
 *   query_points.csv   : pointId, geom          -- geometry as WKT text
 *
 * Parameters:
 * - fullpath: directory path (with trailing slash) where CSV files are written.
 *
 * Example:
 *     \i berlinmod_export.sql
 *     SELECT berlinmod_portability_export('/home/mobilitydb/portability/');
 *****************************************************************************/

DROP FUNCTION IF EXISTS berlinmod_portability_export;
CREATE OR REPLACE FUNCTION berlinmod_portability_export(fullpath text)
RETURNS text AS $$
DECLARE
  startTime timestamptz;
  endTime   timestamptz;
BEGIN
  startTime = clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Exporting BerlinMOD data in cross-platform portability schema';
  RAISE INFO 'Target: %', fullpath;
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO '------------------------------------------------------------------';

  RAISE INFO 'Exporting vehicles.csv';
  EXECUTE format(
    'COPY (SELECT VehicleId AS vehId, Licence AS licence,
                  VehicleType AS type, Model AS model
           FROM Vehicles ORDER BY VehicleId)
     TO ''%svehicles.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting trips.csv (tgeompoint as WKT text)';
  EXECUTE format(
    'COPY (SELECT TripId AS tripId, VehicleId AS vehId,
                  asText(Trip) AS trip
           FROM Trips ORDER BY TripId)
     TO ''%strips.csv'' DELIMITER '','' CSV HEADER', fullpath);

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

  RAISE INFO 'Exporting query_points.csv (geometry as WKT text)';
  EXECUTE format(
    'COPY (SELECT PointId AS pointId, ST_AsText(Geom) AS geom
           FROM Points ORDER BY PointId)
     TO ''%squery_points.csv'' DELIMITER '','' CSV HEADER', fullpath);

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
