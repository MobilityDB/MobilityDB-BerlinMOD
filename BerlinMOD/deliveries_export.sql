/******************************************************************************
 * Exports the Brussels synthetic dataset obtained from the BerlinMOD generator
 * in CSV format 
 * https://github.com/MobilityDB/MobilityDB-BerlinMOD
 * into MobilityDB using projected (2D) coordinates with SRID 3857
 * https://epsg.io/3857
 * Parameters:
 *    fullpath: states the full path in which the CSV files are located.
 *    gist: states whether GiST or SP-GiST indexes are created on the tables.
 *      By default it is set to TRUE and thus creates GiST indexes.
 * Example of usage on psql:
 *     CREATE EXTENSION mobilitydb CASCADE;
 *     \i deliveries_export.sql
 *     SELECT deliveries_export('/home/mobilitydb/data/');
 *****************************************************************************/

DROP FUNCTION IF EXISTS deliveries_export;
CREATE OR REPLACE FUNCTION deliveries_export(fullpath text)
RETURNS text AS $$
DECLARE
  startTime timestamptz;
  endTime timestamptz;
BEGIN
--------------------------------------------------------------

  startTime = clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Exporting synthetic data from the Deliveries data generator';
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

  RAISE INFO 'Exporting table VehicleBrands';
  EXECUTE format('COPY (SELECT BrandId, BrandName FROM VehicleBrands
    ORDER BY BrandId) TO ''%svehiclebrands.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table VehicleClasses';
  EXECUTE format('COPY (SELECT ClassId, ClassName, DutyClass, WeightLimit FROM VehicleClasses
    ORDER BY ClassId) TO ''%svehicleclasses.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Vehicles';
  EXECUTE format('COPY (SELECT VehicleId, Licence, MakeYear, BrandId, ClassId, WarehouseId FROM Vehicles
    ORDER BY VehicleId) TO ''%svehicles.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Municipalities';
  EXECUTE format('COPY (SELECT MunicipalityId, MunicipalityName, Population, PercPop, PopDensityKm2, NoEnterp,
    PercEnterp, ST_AsEWKT(MunicipalityGeo) AS MunicipalityGeo FROM Municipalities ORDER BY MunicipalityId)
  TO ''%smunicipalities.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table RoadSegments';
  EXECUTE format('COPY (SELECT * FROM RoadSegments ORDER BY SegmentId)
  TO ''%sroadsegments.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Warehouses';
  EXECUTE format('COPY (SELECT WarehouseId, ST_AsEWKT(WarehouseGeo) AS WarehouseGeo
    FROM Warehouses ORDER BY WarehouseId)
    TO ''%swarehouses.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE INFO 'Exporting table Deliveries transformed into DeliveriesInput';
  EXECUTE format('COPY (
    WITH Instants(DeliveryId, VehicleId, StartDate, NoCustomers, Inst) AS (
      SELECT DeliveryId, VehicleId, StartDate, NoCustomers, unnest(instants(Trip))
      FROM Deliveries )
    SELECT DeliveryId, VehicleId, StartDate, NoCustomers, getValue(Inst) AS Point, 
      getTimestamp(Inst) AS T
    FROM Instants
    ORDER BY DeliveryId, VehicleId, StartDate, T )
    TO ''%sdeliveriesinput.csv'' DELIMITER '','' CSV HEADER', fullpath);
  
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
