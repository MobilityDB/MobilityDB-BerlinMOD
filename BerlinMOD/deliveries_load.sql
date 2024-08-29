/******************************************************************************
 * Loads the Deliveries data in CSV format into MobilityDB using projected (2D)
 * coordinates with SRID 3857
 * https://epsg.io/3857
 * Parameters:
 *    fullpath: states the full path in which the CSV files are located.
 *    gist: states whether GiST or SP-GiST indexes are created on the tables.
 *      By default it is set to TRUE and thus creates GiST indexes.
 * Example of usage on psql:
 *     CREATE EXTENSION mobilitydb CASCADE;
 *     \i deliveries_load.sql
 *     SELECT deliveries_load('/home/mobilitydb/data/', false);
 *****************************************************************************/

DROP FUNCTION IF EXISTS deliveries_load(fullpath text, gist bool);
CREATE OR REPLACE FUNCTION deliveries_load(fullpath text,
  gist bool DEFAULT TRUE)
RETURNS text AS $$
BEGIN
--------------------------------------------------------------

  CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Instants';
  DROP TABLE IF EXISTS Instants CASCADE;
  CREATE TABLE Instants (
    InstantId integer PRIMARY KEY,
    Instant timestamptz NOT NULL
  );
  EXECUTE format('COPY Instants(InstantId, Instant) FROM ''%sinstants.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);
  CREATE INDEX Instants_Instants_idx ON Instants USING btree(Instant);
  CREATE VIEW Instants1 AS SELECT * FROM Instants LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Periods';
  DROP TABLE IF EXISTS Periods CASCADE;
  CREATE TABLE Periods (
    PeriodId integer PRIMARY KEY,
    Period tstzspan
  );
  EXECUTE format('COPY Periods(PeriodId, Period) FROM ''%speriods.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX Periods_Period_gist_idx ON Periods USING gist(Period);
  ELSE
    CREATE INDEX Periods_Period_spgist_idx ON Periods USING spgist(Period);
  END IF;
  CREATE VIEW Periods1 AS SELECT * FROM Periods LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Points';
  DROP TABLE IF EXISTS Points CASCADE;
  CREATE TABLE Points (
    PointId integer PRIMARY KEY,
    Geom geometry(Point, 3857) NOT NULL
  );
  EXECUTE format('COPY Points(PointId, Geom) FROM ''%spoints.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX Points_Geom_gist_idx ON Points USING gist(Geom);
  ELSE
    CREATE INDEX Points_Geom_spgist_idx ON Points USING spgist(Geom);
  END IF;
  CREATE VIEW Points1 AS SELECT * FROM Points LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Regions';
  DROP TABLE IF EXISTS Regions CASCADE;
  CREATE TABLE Regions (
    RegionId integer PRIMARY KEY,
    Geom geometry(Polygon, 3857) NOT NULL
  );
  EXECUTE format('COPY Regions(RegionId, Geom) FROM ''%sregions.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX Regions_Geom_gist_idx ON Regions USING gist(Geom);
  ELSE
    CREATE INDEX Regions_Geom_spgist_idx ON Regions USING spgist(Geom);
  END IF;
  CREATE VIEW Regions1 AS SELECT * FROM Regions LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Municipalities';
  DROP TABLE IF EXISTS Municipalities;
  CREATE TABLE Municipalities (
    MunicipalityId integer PRIMARY KEY,
    MunicipalityName text NOT NULL,
    Population integer,
    PercPop float,
    PopDensityKm2 integer,
    NoEnterp integer,
    PercEnterp float,
    MunicipalityGeo geometry NOT NULL
  );
  EXECUTE format('COPY Municipalities(MunicipalityId, MunicipalityName, '
    'Population, PercPop, PopDensityKm2, NoEnterp, PercEnterp, MunicipalityGeo) '
    ' FROM ''%smunicipalities.csv'' DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX Municipalities_MunicipalityGeo_gist_idx ON Municipalities
      USING gist(MunicipalityGeo);
  ELSE
    CREATE INDEX Municipalities_MunicipalityGeo_spgist_idx ON Municipalities
      USING spgist(MunicipalityGeo);
  END IF;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Warehouses';
  DROP TABLE IF EXISTS Warehouses CASCADE;
  CREATE TABLE Warehouses (
    WarehouseId integer PRIMARY KEY,
    Geom geometry(Point, 3857) NOT NULL,
    MunicipalityId integer REFERENCES Municipalities(MunicipalityId)
  );
  EXECUTE format('COPY Warehouses(WarehouseId, Geom, MunicipalityId) '
    'FROM ''%swarehouses.csv'' DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX Warehouses_Geom_gist_idx ON Warehouses USING gist(Geom);
  ELSE
    CREATE INDEX Warehouses_Geom_spgist_idx ON Warehouses USING spgist(Geom);
  END IF;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table VehicleBrands';
  DROP TABLE IF EXISTS VehicleBrands CASCADE;
  CREATE TABLE VehicleBrands (
    BrandId integer PRIMARY KEY,
    BrandName text NOT NULL
  );
  EXECUTE format('COPY VehicleBrands(BrandId, BrandName) FROM '
    '''%svehiclebrands.csv'' DELIMITER '','' CSV HEADER', fullpath);

--------------------------------------------------------------

  RAISE NOTICE 'Creating table VehicleClasses';
  DROP TABLE IF EXISTS VehicleClasses CASCADE;
  CREATE TABLE VehicleClasses (
    ClassId integer PRIMARY KEY,
    ClassName text NOT NULL,
    DutyClass text NOT NULL,
    WeightLimit text NOT NULL
  );
  EXECUTE format('COPY VehicleClasses(ClassId, ClassName, DutyClass, '
    ' WeightLimit) FROM ''%svehicleclasses.csv'' DELIMITER '','' CSV HEADER',
    fullpath);

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Vehicles';
  DROP TABLE IF EXISTS Vehicles CASCADE;
  CREATE TABLE Vehicles (
    VehicleId integer PRIMARY KEY,
    Licence text NOT NULL,
    MakeYear int NOT NULL,
    BrandId int NOT NULL, 
    ClassId int NOT NULL,
    WarehouseId int NOT NULL
  );
  EXECUTE format('COPY Vehicles(VehicleId, Licence, MakeYear, BrandId, '
    'ClassId, WarehouseId) FROM ''%svehicles.csv'' DELIMITER '','' '
    'CSV HEADER', fullpath);
  CREATE VIEW Vehicles1 AS SELECT * FROM Vehicles LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Customers';
  DROP TABLE IF EXISTS Customers CASCADE;
  CREATE TABLE Customers (
    CustomerId integer PRIMARY KEY,
    MunicipalityId integer REFERENCES Municipalities(MunicipalityId),
    CustomerGeo geometry(Point, 3857) NOT NULL
  );
  EXECUTE format('COPY Customers(CustomerId, CustomerGeo, MunicipalityId) '
    ' FROM ''%scustomers.csv'' DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX Customers_Geom_gist_idx ON Customers
      USING gist(CustomerGeo);
  ELSE
    CREATE INDEX Customers_Geom_spgist_idx ON Customers
      USING spgist(CustomerGeo);
  END IF;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Date';
  DROP TABLE IF EXISTS Date CASCADE;
  CREATE TABLE Date (
    DateId integer PRIMARY KEY,
    Date  date NOT NULL UNIQUE,
    WeekNo integer,
    MonthNo integer,
    MonthName text,
    Quarter integer,
    Year integer
  );
  EXECUTE format('COPY Date(DateId, Date, WeekNo, MonthNo, MonthName, Quarter, '
    'Year)  FROM ''%sdate.csv'' DELIMITER '','' CSV HEADER', fullpath);
  CREATE INDEX Date_Date_idx ON Date USING btree(Date);

--------------------------------------------------------------

  RAISE NOTICE 'Creating table RoadSegments';
  DROP TABLE IF EXISTS RoadSegments;
  CREATE TABLE RoadSegments (
    SegmentId bigint PRIMARY KEY,
    Name text, 
    OsmId bigint,
    TagId integer,
    SegmentLength float,
    SourceNode bigint, 
    TargetNode bigint,
    SourceOsm bigint,
    TargetOsm bigint,
    TimeSecsFwd float,
    TimeSecsBwd float,
    OneWay integer,
    MaxSpeedFwd float,
    MaxSpeedBwd float, 
    Priority float, 
    SegmentGeo geometry
  );
  EXECUTE format('COPY RoadSegments(SegmentId, Name, OsmId, TagId, '
    'SegmentLength, SourceNode, TargetNode, SourceOsm, TargetOsm, '
    'TimeSecsFwd, TimeSecsBwd, OneWay, MaxSpeedFwd, MaxSpeedBwd, '
    'Priority, SegmentGeo) FROM ''%sroadsegments.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);
  IF gist THEN
    CREATE INDEX RoadSegments_SegmentGeo_gist_idx ON RoadSegments
      USING gist(SegmentGeo);
  ELSE
    CREATE INDEX RoadSegments_SegmentGeo_spgist_idx ON RoadSegments
      USING spgist(SegmentGeo);
  END IF;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table SegmentsInput';
  DROP TABLE IF EXISTS SegmentsInput;
  CREATE TABLE SegmentsInput (
    DeliveryId integer,
    SegNo integer NOT NULL,
    SourceWhId bigint,
    SourceCustId bigint,
    TargetWhId bigint,
    TargetCustId bigint,
    Point geometry(Point, 3857) NOT NULL,
    T timestamptz NOT NULL,
    PRIMARY KEY (DeliveryId, SegNo, T)
  );
  EXECUTE format('COPY SegmentsInput(DeliveryId, SegNo, SourceWhId, '
    'SourceCustId, TargetWhId, TargetCustId, Point, T) FROM '
    '''%ssegmentsinput.csv'' DELIMITER '','' CSV HEADER', fullpath);

  RAISE NOTICE 'Creating table Segments';
  DROP TABLE IF EXISTS Segments;
  CREATE TABLE Segments (
    DeliveryId integer,
    SegNo integer NOT NULL,
    SourceWhId bigint,
    SourceCustId bigint,
    TargetWhId bigint,
    TargetCustId bigint,
    Trip tgeompoint,
    Trajectory geometry,
    SourceGeom geometry,
    PRIMARY KEY (DeliveryId, SegNo)
  );
  INSERT INTO Segments(DeliveryId, SegNo, SourceWhId, SourceCustId, 
    TargetWhId, TargetCustId, Trip)
  SELECT DeliveryId, SegNo, SourceWhId, SourceCustId, TargetWhId, TargetCustId,
    tgeompointSeq(array_agg(tgeompoint(Point, T) ORDER BY T))
  FROM SegmentsInput
  GROUP BY DeliveryId, SegNo, SourceWhId, SourceCustId, TargetWhId, TargetCustId;
  UPDATE Segments SET Trajectory = trajectory(Trip), 
    SourceGeom = startValue(Trip);
  IF gist THEN
    CREATE INDEX Segments_Trip_gist_idx ON Segments
      USING gist(Trip);
    CREATE INDEX Segments_Trajectory_gist_idx ON Segments
      USING gist(Trajectory);
  ELSE
    CREATE INDEX Segments_Trip_spgist_idx ON Segments
      USING spgist(Trip);
    CREATE INDEX Segments_Trajectory_spgist_idx ON Segments
      USING spgist(Trajectory);
  END IF;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Deliveries';
  DROP TABLE IF EXISTS Deliveries CASCADE;
  CREATE TABLE Deliveries (
    DeliveryId integer PRIMARY KEY,
    VehicleId integer NOT NULL REFERENCES Vehicles(VehicleId),
    StartDate date,
    NoCustomers int,
    Trip tgeompoint,
    Trajectory geometry
  );
  EXECUTE format('COPY Deliveries(DeliveryId, VehicleId, StartDate, '
    'NoCustomers) FROM ''%sdeliveries.csv'' DELIMITER '','' CSV HEADER',
    fullpath);

  WITH Temp AS (
    SELECT DeliveryId, merge(array_agg(Trip ORDER BY Trip)) AS Trip
    FROM Segments 
    GROUP BY DeliveryId )
  UPDATE Deliveries d 
  SET Trip = t.trip
  FROM Temp t
  WHERE d.DeliveryId = t.DeliveryId;
  UPDATE Deliveries SET Trajectory = trajectory(Trip);

  IF gist THEN
    CREATE INDEX Deliveries_Trip_gist_idx ON Deliveries USING gist(trip);
  ELSE
    CREATE INDEX Deliveries_Trip_spgist_idx ON Deliveries USING spgist(trip);
  END IF;

  CREATE VIEW Delivery1 AS SELECT * FROM Deliveries LIMIT 100;

-------------------------------------------------------------------------------

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
