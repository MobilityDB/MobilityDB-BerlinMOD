/*-----------------------------------------------------------------------------
-- Deliveries Data Generator
-------------------------------------------------------------------------------
This file is part of MobilityDB.
Copyright (c) 2024, Esteban Zimanyi, Mahmoud Sakr,
  Universite Libre de Bruxelles.

The functions defined in this file use MobilityDB to generate data
corresponding to a delivery service as specified in
https://www.mdpi.com/2220-9964/8/4/170/htm
These functions call other functions defined in the file
berlinmod_datagenerator.sql located in the same directory as the
current file.

The generator needs the underlying road network topology. The file
brussels_preparedata.sql in the same directory can be used to create the
road network for Brussels constructed from OSM data by osm2pgrouting.
Alternatively, an optimized version of the graph can be constructed with the
file brussels_creategraph.sql that creates the graph from OSM data using SQL.

You can change parameters in the various functions of this file.
Usually, changing the master parameter 'P_SCALE_FACTOR' should do it.
But you also might be interested in changing parameters for the
random number generator, experiment with non-standard scaling
patterns or modify the sampling of positions.

The database must contain the following input relations:

* Nodes and RoadSegments are the tables defining the road network graph.
  These tables are typically obtained by osm2pgrouting from OSM data.
  The description of these tables is given in the file
  berlinmod_datagenerator.sql

The generated data is saved into the database in which the
functions are executed using the following tables

*  Warehouses(WarehouseId int primary key, NodeId bigint, Geom geometry(Point))
*  VehicleClasses(ClassId int primary key, ClassName text, DutyClass text, 
     WeightLimit text);
*  VehicleBrands(BrandId int primary key, BrandName text);
*  Vehicles(VehicleId int primary key, Licence text, MakeYear int, BrandId int,
     ClassId int, WarehouseId int)
*  Customers(CustomerId int primary key, CustomerGeo geometry(Point),
    MunicipalityId int);
*  Deliveries(DeliveryId int primary key, VehicleId int, StartDate date,
     NoCustomers int, Trip tgeompoint, Trajectory geometry)
*  Segments(DeliveryId int, SegNo int, SourceWHId bigint, SourceCustId bigint, 
     TargetWHId bigint, TargetCustId bigint, Trip tgeompoint,
     Trajectory geometry, SourceGeom geometry, primary key (DeliveryId, SegNo))
*  Points(PointId int primary key, Geom geometry)
*  Regions(RegionId int primary key, Geom geometry)
*  Instants(InstantId int primary key, Instant timestamptz)
*  Periods(PeriodId int primary key, Period tstzspan)

In addition the following work tables are created

*  Trips(VehicleId int, StartDate date, SegNo int, SourceNode bigint, 
     TargetNode bigint, SourceWHId int, TargetWHId int, SourceCustId int, 
     TargetCustId int, primary key (VehicleId, StartDate, SegNo))
*  Destinations(DestId serial, SourceNode bigint, target bigint)
*  Paths(StartNode bigint, EndNode bigint, SegNo int, 
     NodeId bigint, EdgeId bigint, Geom geometry, Speed float, Category int,
     primary key (StartNode, EndNode, path_seq));

-----------------------------------------------------------------------------*/

-- Type combining the elements needed to define a path in the graph

DROP TYPE IF EXISTS step CASCADE;
CREATE TYPE step as (linestring geometry, maxspeed float, category int);

-- Generate the data for a given number vehicles and days starting at a day.
-- The last two arguments correspond to the parameters P_PATH_MODE and
-- P_DISTURB_DATA

DROP FUNCTION IF EXISTS deliveries_createDeliveries;
CREATE FUNCTION deliveries_createDeliveries(noVehicles int, noDays int,
  startDay Date, disturbData boolean, messages text)
RETURNS void LANGUAGE plpgsql STRICT AS $$
DECLARE
  -- Loops over the days for which we generate the data
  aDay date;
  -- 0 (Sunday) to 6 (Saturday)
  weekday int;
  -- Current timestamp
  t timestamptz;
  -- Identifier of the deliveries
  delivId int;
  -- Number of segments in a delivery (number of destinations + 1)
  noSegments int;
  -- Source and target nodes of a delivery segment
  srcNode bigint; trgtNode bigint;
  -- Variables used for migrating the information from the Trips to the Segments
  srcWH int; trgtWH int; srcCust int; trgtCust int;
  -- Path betwen start and end nodes
  path step[];
  -- Segments trip obtained from a path
  trip tgeompoint;
  -- All segment trips of a delivery
  alltrips tgeompoint[] = '{}';
  -- Geometry of the source noDeliveries
  SourceGeom geometry;
  -- Start time of a segment
  startTime timestamptz;
  -- Time of the trip to a customer
  tripTime interval;
  -- Time servicing a customer
  deliveryTime interval;
  -- Loop variables
  vehId int; dayNo int; seg int;
  -- Number of vehicles for showing heartbeat messages when message is 'minimal'
  P_DELIVERIES_NO_VEHICLES int = 100;
BEGIN
  RAISE INFO 'Creating tables Deliveries and Segments';
  DROP TABLE IF EXISTS Deliveries;
  CREATE TABLE Deliveries(DeliveryId int PRIMARY KEY, 
    VehicleId int REFERENCES Vehicles(VehicleId),
    StartDate date, NoCustomers int, Trip tgeompoint, Trajectory geometry);
  DROP TABLE IF EXISTS Segments;
  CREATE TABLE Segments(DeliveryId int, SegNo int, 
    -- These columns are used for an OLAP schema
    SourceWHId bigint, SourceCustId bigint, TargetWHId bigint, 
    TargetCustId bigint, 
    -- These columns are used for visualization purposes
    Trip tgeompoint, Trajectory geometry, SourceGeom geometry,
    PRIMARY KEY (DeliveryId, SegNo));
  delivId = 1;
  aDay = startDay;
  FOR dayNo IN 1..noDays LOOP
    SELECT date_part('dow', aDay) INTO weekday;
    IF messages = 'minimal' OR messages = 'medium' OR messages = 'verbose' THEN
      RAISE INFO '-- Date %', aDay;
    END IF;
    -- 6: saturday, 0: sunday
    IF weekday <> 0 THEN
      <<vehicles_loop>> -- label needed for the CONTINUE below
      FOR vehId IN 1..noVehicles LOOP
        IF messages = 'minimal' AND vehId % P_DELIVERIES_NO_VEHICLES = 1 THEN
          RAISE INFO '  -- Vehicles % to %', vehId,
            LEAST(vehId + P_DELIVERIES_NO_VEHICLES - 1, noVehicles);
        END IF;
        IF messages = 'medium' OR messages = 'verbose' THEN
          RAISE INFO '  -- Vehicles %', vehId;
        END IF;
        -- Start delivery
        t = aDay + time '07:00:00' + createPauseN(120);
        IF messages = 'medium' OR messages = 'verbose' THEN
          RAISE INFO '    Deliveries starting at %', t;
        END IF;
        -- Get the number of segments (number of destinations + 1)
        SELECT COUNT(*) INTO noSegments
        FROM Trips
        WHERE VehicleId = vehId AND StartDate = aDay;
        <<segments_loop>>
        FOR seg IN 1..noSegments LOOP
          -- Get the source and destination nodes of the segment
          SELECT SourceNode, TargetNode, SourceWHId, TargetWHId, SourceCustId, 
            TargetCustId
          INTO srcNode, trgtNode, srcWH, trgtWH, srcCust, trgtCust
          FROM Trips
          WHERE VehicleId = vehId AND StartDate = aDay AND SegNo = seg;
          -- Get the path
          SELECT array_agg((Geom, speed, category) ORDER BY SeqNo) INTO path
          FROM Paths
          WHERE StartNode = srcNode AND EndNode = trgtNode AND EdgeId > 0;
          -- In exceptional circumstances, depending on the input graph, it may
          -- be the case that pgrouting does not find a connecting path between
          -- two nodes. Instead of stopping the generation process, the error
          -- is reported, the trip for the vehicle and the day is ignored, and
          -- the generation process is continued.
          IF path IS NULL THEN
            RAISE INFO 'ERROR: The path of a trip cannot be NULL. ';
            RAISE INFO '       Source node: %, target node: %, seg: %, noSegments: %',
              srcNode, trgtNode, seg, noSegments;
            RAISE INFO '       The trip of vehicle % for day % is ignored',
              vehId, aDay;
            DELETE FROM Segments where DeliveryId = delivId;
            alltrips = '{}';
            delivId = delivId + 1;
            CONTINUE vehicles_loop;
          END IF;
          startTime = t;
          trip = create_trip(path, t, disturbData, messages);
          IF trip IS NULL THEN
            RAISE INFO 'ERROR: A trip cannot be NULL';
            RAISE INFO '  The trip of vehicle % for day % is ignored', vehId,
              aDay;
            DELETE FROM Segments where DeliveryId = delivId;
            alltrips = '{}';
            delivId = delivId + 1;
            CONTINUE vehicles_loop;
          END IF;
          t = endTimestamp(trip);
          tripTime = t - startTime;
          IF messages = 'medium' OR messages = 'verbose' THEN
            RAISE INFO '      Trip to destination % started at % and lasted %',
              seg, startTime, tripTime;
          END IF;
          IF seg < noSegments THEN
            -- Add a delivery time in [10, 60] min using a bounded Gaussian distribution
            deliveryTime = random_boundedgauss(10, 60) * interval '1 min';
            IF messages = 'medium' OR messages = 'verbose' THEN
              RAISE INFO '      Deliveries lasted %', deliveryTime;
            END IF;
            t = t + deliveryTime;
            trip = appendInstant(trip, tgeompoint(endValue(trip), t));
          END IF;
          alltrips = alltrips || trip;
          SELECT Geom INTO SourceGeom FROM Nodes WHERE NodeId = srcNode;
          INSERT INTO Segments(DeliveryId, SegNo, SourceWHId, SourceCustId,
            TargetWHId, TargetCustId, Trip, Trajectory, SourceGeom)
          VALUES (delivId, seg, srcWH, srcCust, trgtWH, trgtCust, trip, 
            trajectory(Trip), SourceGeom);
        END LOOP;
        trip = merge(alltrips);
        INSERT INTO Deliveries(DeliveryId, VehicleId, StartDate, NoCustomers,
          Trip, Trajectory)
        VALUES (delivId, vehId, aDay, noSegments - 1, trip, trajectory(Trip));
        IF messages = 'medium' OR messages = 'verbose' THEN
          RAISE INFO '    Deliveries ended at %', t;
        END IF;
        delivId = delivId + 1;
        alltrips = '{}';
      END LOOP;
    ELSE
      IF messages = 'minimal' OR messages = 'medium' OR messages = 'verbose' THEN
        RAISE INFO '  No deliveries on Sunday';
      END IF;
    END IF;
    aDay = aDay + interval '1 day';
  END LOOP;
  RETURN;
END; $$;

/*
SELECT deliveries_createDeliveries(2, 2, '2020-06-01', false, 'minimal');
*/

-------------------------------------------------------------------------------
-- Selects the next destination node for a delivery
-------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS deliveries_selectCustNode;
CREATE FUNCTION deliveries_selectCustNode(vehicId int, NoCustomers int,
  prevNodes bigint[])
RETURNS bigint AS $$
DECLARE
  -- Random sequence number
  SegNo int;
  -- Customers id
  custId int;
  -- Result of the function
  result bigint;
BEGIN
  WHILE true LOOP
    custId = random_int(1, NoCustomers);
    -- Get the customer node
    SELECT c.NodeId INTO result
    FROM Customers c
    WHERE c.CustomerId = custId;
    IF result != ALL(prevNodes) THEN
      RETURN result;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

-------------------------------------------------------------------------------
-- Main Function
-------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS deliveries_datagenerator;
CREATE FUNCTION deliveries_datagenerator(scaleFactor float DEFAULT NULL,
  noWarehouses int DEFAULT NULL, noVehicles int DEFAULT NULL,
  NoCustomers int DEFAULT NULL, noDays int DEFAULT NULL, 
  startDay date DEFAULT NULL, pathMode text DEFAULT NULL, 
  disturbData boolean DEFAULT NULL, messages text DEFAULT NULL,
  indexType text DEFAULT NULL)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE

  ----------------------------------------------------------------------
  -- Primary parameters, which are optional arguments of the function
  ----------------------------------------------------------------------

  -- Scale factor
  -- Set value to 1.0 or bigger for a full-scaled benchmark
  P_SCALE_FACTOR float = 0.005;

  -- By default, the scale factor determines the number of warehouses, the
  -- number of vehicles and the number of days they are observed as follows
  --    noWarehouses int = round((100 * P_SCALE_FACTOR)::numeric, 0)::int;
  --    noVehicles int = round((2000 * sqrt(P_SCALE_FACTOR))::numeric, 0)::int;
  --    NoCustomers int = round((10000 * sqrt(P_SCALE_FACTOR))::numeric, 0)::int;
  --    noDays int = round((sqrt(P_SCALE_FACTOR) * 28)::numeric, 0)::int;
  -- For example, for P_SCALE_FACTOR = 0.1 these values will be
  --    noWarehouses = 32
  --    noVehicles = 632
  --    NoCustomers = 3163
  --    noDays int = 11
  -- Alternatively, you can manually set these parameters to arbitrary
  -- values using the optional arguments in the function call.

  -- The day the observation starts ===
  -- default: P_START_DAY = Monday 2020-06-01)
  P_START_DAY date = '2020-06-01';

  -- Method for selecting a path between a start and end nodes.
  -- Possible values are 'Fastest Path' (default) and 'Shortest Path'
  P_PATH_MODE text = 'Fastest Path';

  -- Choose imprecise data generation. Possible values are
  -- FALSE (no imprecision, default) and TRUE (disturbed data)
  P_DISTURB_DATA boolean = FALSE;

  -------------------------------------------------------------------------
  --  Secondary Parameters
  -------------------------------------------------------------------------

  -- Seed for the random generator used to ensure deterministic results
  P_RANDOM_SEED float = 0.5;

  -- Size for sample relations
  P_SAMPLE_SIZE int = 100;

  -- Number of paths sent in a batch to pgRouting
  P_PGROUTING_BATCH_SIZE int = 1e5;

  -- Minimum length in milliseconds of a pause, used to distinguish subsequent
  -- trips. Default 5 minutes
  P_MINPAUSE interval = 5 * interval '1 min';

  -- Velocity below which a vehicle is considered to be static
  -- Default: 0.04166666666666666667 (=1.0 m/24.0 h = 1 m/day)
  P_MINVELOCITY float = 0.04166666666666666667;

  -- Duration in milliseconds between two subsequent GPS-observations
  -- Default: 2 seconds
  P_GPSINTERVAL interval = 2 * interval '1 ms';

  -- Quantity of messages shown describing the generation process.
  -- Possible values are 'verbose', 'medium', 'minimal', and 'none'.
  -- Choose 'none' to only show the main steps of the process. However,
  -- for large scale factors, no message will be issued while executing steps
  -- taking long time and it may seems that the generated is blocked.
  -- Default to 'minimal' to show that the generator is running.
  P_MESSAGES text = 'minimal';

  -- Determine the type of indices.
  -- Possible values are 'gist' (default) and 'spgist'
  P_INDEX_TYPE text = 'gist';

  ----------------------------------------------------------------------
  --  Variables
  ----------------------------------------------------------------------
  -- Loop variables
  vehId int;
  -- Number of nodes in the graph
  noNodes int;
  -- Number of paths and number of calls to pgRouting
  noPaths int; noCalls int;
  -- Number of segments and deliveries generated
  noSegments int; noDeliveries int;
  -- Warehouses Id
  warehId int;
  -- Warehouses node
  warehNode bigint;
  -- Customers node
  custId int;
  -- Customers node
  custNode bigint;
  -- Node identifiers of a delivery segment
  srcNode bigint; trgtNode bigint;
  -- Warehouses/Customers identifiers of a delivery segment
  srcWH int; trgtWH int; srcCust int; trgtCust int;
  -- Day for which we generate data
  day date;
  -- Start and end time of the execution
  startTime timestamptz; endTime timestamptz;
  -- Start and end time of the batch call to pgRouting
  startPgr timestamptz; endPgr timestamptz;
  -- Queries sent to pgrouting for choosing the path according to P_PATH_MODE
  -- and the number of records defined by LIMIT/OFFSET
  query1_pgr text; query2_pgr text;
  -- Random number of destinations (between 1 and 3)
  noDest int;
  -- Previous nodes of the current delivery
  prevNodes bigint[];
  -- String to generate the trace message
  str text;
  -- Number of rows in the VehicleBrands and VehicleClasses tables
  noVehicleBrands int; noVehicleClasses int;
  -- Attributes of table Vehicles
  lic text; mkYear int; clId int; brId int;
BEGIN
  -------------------------------------------------------------------------
  --  Initialize parameters and variables
  -------------------------------------------------------------------------

  -- Set the P_RANDOM_SEED so that the random function will return a repeatable
  -- sequence of random numbers that is derived from the P_RANDOM_SEED.
  PERFORM setseed(P_RANDOM_SEED);

  -- Setting the parameters of the generation
  IF scaleFactor IS NULL THEN
    scaleFactor = P_SCALE_FACTOR;
  END IF;
  IF noWarehouses IS NULL THEN
    noWarehouses = round((100 * sqrt(scaleFactor))::numeric, 0)::int;
  END IF;
  IF noVehicles IS NULL THEN
    noVehicles = round((2000 * sqrt(scaleFactor))::numeric, 0)::int;
  END IF;
  IF NoCustomers IS NULL THEN
    NoCustomers = round((10000 * sqrt(scaleFactor))::numeric, 0)::int;
  END IF;
  IF noDays IS NULL THEN
    noDays = round((sqrt(scaleFactor) * 28)::numeric, 0)::int + 2;
  END IF;
  IF startDay IS NULL THEN
    startDay = P_START_DAY;
  END IF;
  IF pathMode IS NULL THEN
    pathMode = P_PATH_MODE;
  END IF;
  IF disturbData IS NULL THEN
    disturbData = P_DISTURB_DATA;
  END IF;
  IF messages IS NULL THEN
    messages = P_MESSAGES;
  END IF;
  IF indexType IS NULL THEN
    indexType = P_INDEX_TYPE;
  END IF;

  -- Set the seed so that the random function will return a repeatable
  -- sequence of random numbers that is derived from the P_RANDOM_SEED.
  PERFORM setseed(P_RANDOM_SEED);

  -- Get the number of nodes
  SELECT COUNT(*) INTO noNodes FROM Nodes;

  RAISE INFO '-----------------------------------------------------------------------';
  RAISE INFO 'Starting deliveries generation with scale factor %', scaleFactor;
  RAISE INFO '-----------------------------------------------------------------------';
  RAISE INFO 'Parameters:';
  RAISE INFO '------------';
  RAISE INFO 'No. of warehouses = %, No. of vehicles = %, No. of customers = %',
    noWarehouses, noVehicles, NoCustomers;
  RAISE INFO 'No. of days = %, Start day = %, Path mode = %, Disturb data = %',
    noDays, startDay, pathMode, disturbData;
  RAISE INFO 'Verbosity = %, Index type = %', 
    messages, indexType;
  SELECT clock_timestamp() INTO startTime;
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO '----------------------------------------------------------------------';

  -------------------------------------------------------------------------
  --  Creating the base data
  -------------------------------------------------------------------------

  -- Create a relation with all warehouses
  RAISE INFO 'Creating table Warehouses';
  DROP TABLE IF EXISTS Warehouses;
  CREATE TABLE Warehouses(WarehouseId int PRIMARY KEY, NodeId bigint,
    WarehouseGeo geometry(Point), 
    MunicipalityId int REFERENCES Municipalities(MunicipalityId));
  FOR warehId IN 1..noWarehouses LOOP
    -- Create a warehouse located at that a random node
    INSERT INTO Warehouses(WarehouseId, NodeId, WarehouseGeo)
    SELECT warehId, NodeId, Geom
    FROM Nodes n
    ORDER BY NodeId LIMIT 1 OFFSET random_int(1, noNodes) - 1;
  END LOOP;
  UPDATE Warehouses w SET MunicipalityId = (
    SELECT MunicipalityId FROM Municipalities m
    WHERE ST_Intersects(m.MunicipalityGeo, w.WarehouseGeo) LIMIT 1 );

  RAISE NOTICE 'Creating indexes on table Warehouses';
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Warehouses_WarehouseGeo_gist_idx 
    ON Warehouses USING gist(WarehouseGeo);
  ELSE
    CREATE INDEX IF NOT EXISTS Warehouses_WarehouseGeo_spgist_idx 
    ON Warehouses USING spgist(WarehouseGeo);
  END IF;

  -- Create a relation with all vehicle classes
  DROP TABLE IF EXISTS VehicleClasses;
  CREATE TABLE VehicleClasses(ClassId int PRIMARY KEY, ClassName text,
    DutyClass text, WeightLimit text);
  INSERT INTO VehicleClasses(ClassId, ClassName, DutyClass, WeightLimit) VALUES
  (1, 'Class 1', 'Light duty', '0-6,000 pounds (0-2,722 kg)'),
  (2, 'Class 2', 'Light duty', '6,001-10,000 pounds (2,722-4,536 kg)'),
  (3, 'Class 3', 'Medium duty', '10,001-14,000 pounds (4,536-6,350 kg)'),
  (4, 'Class 4', 'Medium duty', '14,001-16,000 pounds (6,351-7,257 kg)'),
  (5, 'Class 5', 'Medium duty', '16,001-19,500 pounds (7,258-8,845 kg)'),
  (6, 'Class 6', 'Medium duty', '19,501-26,000 pounds (8,846-11,793 kg)'),
  (7, 'Class 7', 'Heavy duty', '26,001-33,000 pounds (11,794-14,969 kg)'),
  (8, 'Class 8', 'Heavy duty', '33,001-80,000 pounds (14,969-36,287 kg) and above');

  -- Create a relation with all vehicle brands
  DROP TABLE IF EXISTS VehicleBrands;
  CREATE TABLE VehicleBrands(BrandId int PRIMARY KEY, BrandName text);
  INSERT INTO VehicleBrands(BrandId, BrandName) VALUES
  (1, 'RAM'), (2, 'GMC'), (3, 'Ford'), (4, 'Chevrolet'), (5, 'Volkswagen'),
  (6, 'Mercedes-Benz'), (7, 'Citroën'), (8, 'Renault'), (9, 'Peugeot'),
  (10, 'Fiat'), (12, 'Nissan'), (13, 'Toyota'), (14, 'Daihatsu'), 
  (15, 'Hyundai'), (16, 'Honda');

  -- Create a relation with all vehicles and the associated warehouse
  -- Warehouses are associated to vehicles in a round-robin way
  RAISE INFO 'Creating table Vehicles';
  DROP TABLE IF EXISTS Vehicles;
  CREATE TABLE Vehicles(VehicleId int PRIMARY KEY, Licence text, MakeYear int,
    brandId int, classId int, WarehouseId int);
  FOR vehId IN 1..noVehicles LOOP
    lic = berlinmod_createLicence(vehId);
    mkYear = EXTRACT(year FROM startDay) - random_int(1, 10);
    SELECT COUNT(*) INTO noVehicleBrands
    FROM VehicleBrands;
    SELECT COUNT(*) INTO noVehicleClasses
    FROM VehicleClasses;
    brId = random_int(1, noVehicleBrands);
    clId = random_int(1, noVehicleClasses);
    warehId = 1 + ((vehId - 1) % noWarehouses);
    INSERT INTO Vehicles (VehicleId, Licence, brandId, MakeYear, ClassId,
      WarehouseId)
    VALUES (vehId, lic, brId, mkYear, clId, warehId);
  END LOOP;

  -- Create a relation with all customers
  RAISE INFO 'Creating table Customers';
  DROP TABLE IF EXISTS Customers;
  CREATE TABLE Customers(CustomerId int PRIMARY KEY, NodeId bigint, 
    CustomerGeo geometry(Point),
    MunicipalityId int REFERENCES Municipalities(MunicipalityId));
  FOR custId IN 1..NoCustomers LOOP
    -- Create a customer located at that a random node
    INSERT INTO Customers(CustomerId, NodeId, CustomerGeo)
    SELECT custId, NodeId, Geom
    FROM Nodes n
    ORDER BY NodeId LIMIT 1 OFFSET random_int(1, noNodes) - 1;
  END LOOP;
  UPDATE Customers c SET MunicipalityId = (
    SELECT MunicipalityId FROM Municipalities m
    WHERE ST_Intersects(m.MunicipalityGeo, c.CustomerGeo) LIMIT 1 );

  RAISE NOTICE 'Creating indexes on table Customers';
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Customers_CustomerGeo_gist_idx ON Customers
      USING gist(CustomerGeo);
  ELSE
    CREATE INDEX IF NOT EXISTS Customers_CustomerGeo_spgist_idx ON Customers
      USING spgist(CustomerGeo);
  END IF;

  -------------------------------------------------------------------------
  -- Create auxiliary benchmarking data
  -- The number of rows these tables is determined by P_SAMPLE_SIZE
  -------------------------------------------------------------------------

  -- Random points
  RAISE INFO 'Creating tables Points and Regions';
  DROP TABLE IF EXISTS Points CASCADE;
  CREATE TABLE Points(PointId int PRIMARY KEY, Geom geometry(Point));
  INSERT INTO Points
  WITH Temp AS (
    SELECT PointId, random_int(1, noNodes) AS NodeId
    FROM generate_series(1, P_SAMPLE_SIZE) PointId )
  SELECT t.PointId, n.Geom
  FROM Temp t, Nodes n
  WHERE t.NodeId = n.NodeId;

  RAISE NOTICE 'Creating indexes on table Points';
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Points_geom_gist_idx ON Points
      USING gist(Geom);
  ELSE
    CREATE INDEX IF NOT EXISTS Points_geom_spgist_idx ON Points
      USING spgist(Geom);
  END IF;

  -- Random regions
  DROP TABLE IF EXISTS Regions CASCADE;
  CREATE TABLE Regions(RegionId int PRIMARY KEY, Geom geometry(Polygon));
  INSERT INTO Regions
  WITH Temp AS (
    SELECT RegionId, random_int(1, noNodes) AS NodeId
    FROM generate_series(1, P_SAMPLE_SIZE) RegionId )
  SELECT t.RegionId, ST_Buffer(n.Geom, random_int(1, 997) + 3.0, 
    random_int(0, 25)) AS Geom
  FROM Temp t, Nodes n
  WHERE t.NodeId = n.NodeId;

  RAISE NOTICE 'Creating indexes on table Regions';
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Regions_geom_gist_idx ON Regions
      USING gist(Geom);
  ELSE
    CREATE INDEX IF NOT EXISTS Regions_geom_spgist_idx ON Regions
      USING spgist(Geom);
  END IF;

  -- Random instants
  RAISE INFO 'Creating tables Instants and Periods';
  DROP TABLE IF EXISTS Instants CASCADE;
  CREATE TABLE Instants(InstantId int PRIMARY KEY, Instant timestamptz);
  INSERT INTO Instants
  SELECT InstantId, startDay + (random() * noDays) * interval '1 day' AS Instant
  FROM generate_series(1, P_SAMPLE_SIZE) InstantId;

  CREATE INDEX IF NOT EXISTS Instants_instant_idx ON Instants
    USING btree(Instant);

  -- Random periods
  DROP TABLE IF EXISTS Periods CASCADE;
  CREATE TABLE Periods(PeriodId int PRIMARY KEY, Period tstzspan);
  INSERT INTO Periods
  WITH Instants AS (
    SELECT PeriodId, startDay + (random() * noDays) * interval '1 day' AS Instant
    FROM generate_series(1, P_SAMPLE_SIZE) PeriodId )
  SELECT PeriodId, span(Instant, Instant + abs(random_gauss()) * interval '1 day',
    true, true) AS Period
  FROM Instants;

  RAISE NOTICE 'Creating indexes on table Periods';
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Periods_Period_gist_idx ON Periods
      USING gist(Period);
  ELSE
    CREATE INDEX IF NOT EXISTS Periods_Period_spgist_idx ON Periods
      USING spgist(Period);
  END IF;

  -- Create a Date dimension table for OLAP querying
  RAISE INFO 'Creating table Date';
  DROP TABLE IF EXISTS Date;
  CREATE TABLE Date(DateId serial PRIMARY KEY, Date date NOT NULL UNIQUE,
    WeekNo int, MonthNo int, MonthName text, Quarter int, Year int);
  INSERT INTO Date (Date)
  SELECT generate_series(startDay, startDay + interval '1 year',
    interval '1 day');
  UPDATE Date SET
    WeekNo = EXTRACT(week FROM Date),
    MonthNo = EXTRACT(month FROM Date),
    MonthName = TO_CHAR(Date, 'Month'),
    Quarter = EXTRACT(quarter FROM Date),
    Year = EXTRACT(year FROM Date);

  -------------------------------------------------------------------------
  -- Generate the deliveries
  -------------------------------------------------------------------------

  -- Create Destinations table accumulating all pairs (SourceNode, TargetNode)
  -- that will be sent to pgRouting in a single call. We DO NOT test whether we
  -- are inserting duplicates in the table, the query sent to the pgr_dijkstra
  -- function MUST use 'SELECT DISTINCT ...'

  RAISE INFO 'Creating tables Trips and Destinations';
  DROP TABLE IF EXISTS Trips;
  CREATE TABLE Trips(VehicleId int, StartDate date REFERENCES Date(Date), 
    SegNo int, SourceNode bigint, TargetNode bigint,
    SourceWHId int, TargetWHId int, SourceCustId int, TargetCustId int,
    PRIMARY KEY (VehicleId, StartDate, SegNo));
  DROP TABLE IF EXISTS Destinations;
  CREATE TABLE Destinations(DestId serial PRIMARY KEY, SourceNode bigint,
    TargetNode bigint);
  -- Loop for every vehicle
  FOR vehId IN 1..noVehicles LOOP
    IF messages = 'verbose' THEN
      RAISE INFO '-- Vehicles %', vehId;
    END IF;
    -- Get the warehouse node
    SELECT w.WarehouseId, w.NodeId INTO warehId, warehNode
    FROM Vehicles v, Warehouses w
    WHERE v.VehicleId = vehId AND v.WarehouseId = w.WarehouseId;
    day = startDay;
    -- Loop for every generation day
    FOR dayNo IN 1..noDays LOOP
      IF messages = 'verbose' THEN
        RAISE INFO '  -- Day %', day;
      END IF;
      -- Generate deliveries excepted on Sunday
      IF date_part('dow', day) <> 0 THEN
        -- Select a number of destinations between 3 and 7
        SELECT random_int(3, 7) INTO noDest;
        IF messages = 'verbose' THEN
          RAISE INFO '    Number of destinations: %', noDest;
        END IF;
        srcNode = warehNode;
        srcWH = warehId; srcCust = NULL; trgtWH = NULL; 
        prevNodes = '{}';
        prevNodes = prevNodes || warehNode;
        FOR dest IN 1..noDest + 1 LOOP
          IF dest <= noDest THEN
            trgtNode = deliveries_selectCustNode(vehId, NoCustomers, prevNodes);
            SELECT c.CustomerId INTO custId
            FROM Customers c
            WHERE c.NodeId = trgtNode;
            prevNodes = prevNodes || trgtNode;
            trgtCust = custId; trgtWH = NULL; 
          ELSE
            trgtNode = warehNode;
            trgtWH = warehId; trgtCust = NULL; 
          END IF;
          IF srcNode IS NULL THEN
            RAISE EXCEPTION '    Destination node cannot be NULL';
          END IF;
          IF trgtNode IS NULL THEN
            RAISE EXCEPTION '    Destination node cannot be NULL';
          END IF;
          IF srcNode = trgtNode THEN
            RAISE EXCEPTION '    Source and destination nodes must be different, node: %', srcNode;
          END IF;
          IF messages = 'verbose' THEN
            RAISE INFO '    Deliveries segment from % to %', srcNode, trgtNode;
          END IF;
          -- Keep the source and target nodes of each segment
          INSERT INTO Trips(VehicleId, StartDate, SegNo, SourceNode,
            TargetNode, SourceWHId, TargetWHId, SourceCustId, TargetCustId) 
          VALUES (vehId, day, dest, srcNode, trgtNode, srcWH, trgtWH, srcCust, 
            trgtCust);
          INSERT INTO Destinations(SourceNode, TargetNode)
          VALUES (srcNode, trgtNode);
          srcNode = trgtNode;
          srcCust = trgtCust;
          IF dest <= noDest THEN
            srcWH = NULL; trgtWH = NULL; 
          ELSE
            srcWH = NULL; trgtWH = warehId; 
          END IF;
        END LOOP;
      ELSE
        IF messages = 'verbose' THEN
          RAISE INFO 'No delivery on Sunday';
        END IF;
      END IF;
      day = day + interval '1 day';
    END LOOP;
  END LOOP;

  -------------------------------------------------------------------------
  -- Call pgRouting to generate the paths
  -------------------------------------------------------------------------

  RAISE INFO 'Creating table Paths';
  DROP TABLE IF EXISTS Paths;
  CREATE TABLE Paths(
    -- The following attributes are generated by pgRouting
    StartNode bigint, EndNode bigint, SeqNo int, NodeId bigint, EdgeId bigint,
    -- The following attributes are filled in the subsequent update
    Geom geometry NOT NULL, Speed float NOT NULL, Category int NOT NULL,
    PRIMARY KEY (StartNode, EndNode, SeqNo));

  -- Select query sent to pgRouting
  IF pathMode = 'Fastest Path' THEN
    query1_pgr = 
      'SELECT SegmentId AS id, SourceNode AS source, TargetNode AS target, '
      '  TimeSecsFwd AS cost, TimeSecsBwd AS reverse_cost FROM RoadSegments';
  ELSE
    query1_pgr = 
      'SELECT SegmentId AS id, SourceNode AS source, TargetNode AS target, '
      '  SegmentLength AS cost, SegmentLength * sign(TimeSecsBwd) AS '
      '  reverse_cost FROM RoadSegments';
  END IF;
  -- Get the total number of paths and number of calls to pgRouting
  SELECT COUNT(*) INTO noPaths FROM (
    SELECT DISTINCT SourceNode, TargetNode FROM Destinations ) AS t;
  noCalls = ceiling(noPaths / P_PGROUTING_BATCH_SIZE::float);
  IF messages = 'minimal' OR messages = 'medium' OR messages = 'verbose' THEN
    IF noCalls = 1 THEN
      RAISE INFO '  Call to pgRouting to compute % paths', noPaths;
    ELSE
      RAISE INFO '  Call to pgRouting to compute % paths in % calls of % (source, target) couples each',
        noPaths, noCalls, P_PGROUTING_BATCH_SIZE;
    END IF;
  END IF;

  startPgr = clock_timestamp();
  FOR i IN 1..noCalls LOOP
    query2_pgr = format('SELECT DISTINCT SourceNode AS source, TargetNode AS target '
      'FROM Destinations ORDER BY SourceNode, TargetNode LIMIT %s OFFSET %s',
      P_PGROUTING_BATCH_SIZE, (i - 1) * P_PGROUTING_BATCH_SIZE);
    IF messages = 'medium' OR messages = 'verbose' THEN
      IF noCalls = 1 THEN
        RAISE INFO '  Call started at %', clock_timestamp();
      ELSE
        RAISE INFO '  Call number % started at %', i, clock_timestamp();
      END IF;
    END IF;
    INSERT INTO Paths(StartNode, EndNode, SeqNo, NodeId, EdgeId, Geom, Speed,
      Category)
    WITH Temp(StartNode, EndNode, SeqNo, NodeId, EdgeId) AS (
      SELECT start_vid, end_vid, path_seq, node, edge
      FROM pgr_dijkstra(query1_pgr, query2_pgr, true)
      WHERE edge > 0 )
    SELECT StartNode, EndNode, SeqNo, NodeId, EdgeId,
      -- adjusting directionality
      CASE
        WHEN t.NodeId = r.SourceNode THEN r.SegmentGeo
        ELSE ST_Reverse(r.SegmentGeo)
      END AS Geom, r.MaxSpeedFwd AS Speed,
      berlinmod_roadCategory(r.TagId) AS Category
    FROM Temp t, RoadSegments r
    WHERE r.SegmentId = t.EdgeId;
    IF messages = 'medium' OR messages = 'verbose' THEN
      IF noCalls = 1 THEN
        RAISE INFO '  Call ended at %', clock_timestamp();
      ELSE
        RAISE INFO '  Call number % ended at %', i, clock_timestamp();
      END IF;
    END IF;
  END LOOP;
  endPgr = clock_timestamp();

  -- Build index to speed up processing
  CREATE INDEX Paths_EndNode_EndNode_idx ON Paths
    USING btree(StartNode, EndNode);

  -------------------------------------------------------------------------
  -- Generate the deliveries
  -------------------------------------------------------------------------

  PERFORM deliveries_createDeliveries(noVehicles, noDays, startDay,
    disturbData, messages);

  RAISE NOTICE 'Creating indexes on table Deliveries';
  CREATE INDEX IF NOT EXISTS Deliveries_VehicleId_idx ON Deliveries
    USING btree(VehicleId);
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Deliveries_trip_gist_idx ON Deliveries
      USING gist(trip);
    CREATE INDEX IF NOT EXISTS Deliveries_trajectory_gist_idx ON Deliveries
      USING gist(trajectory);
  ELSE
    CREATE INDEX IF NOT EXISTS Deliveries_trip_spgist_idx ON Deliveries
      USING spgist(trip);
    CREATE INDEX IF NOT EXISTS Deliveries_trajectory_spgist_idx ON Deliveries
      USING spgist(trajectory);
  END IF;

  RAISE NOTICE 'Creating indexes on table Segments';
  IF lower(indexType) = 'gist' THEN
    CREATE INDEX IF NOT EXISTS Segments_trip_gist_idx ON Segments
      USING gist(trip);
    CREATE INDEX IF NOT EXISTS Segments_trajectory_gist_idx ON Segments
      USING gist(trajectory);
  ELSE
    CREATE INDEX IF NOT EXISTS Segments_trip_spgist_idx ON Segments USING
      spgist(trip);
    CREATE INDEX IF NOT EXISTS Segments_trajectory_spgist_idx ON Segments
      USING spgist(trajectory);
  END IF;

  -------------------------------------------------------------------------
  -- Print generation summary
  -------------------------------------------------------------------------

  -- Get the number of deliveries generated
  SELECT COUNT(*) INTO noSegments FROM Segments;
  SELECT COUNT(*) INTO noDeliveries FROM Deliveries;

  SELECT clock_timestamp() INTO endTime;
  IF messages = 'medium' OR messages = 'verbose' THEN
    RAISE INFO '-----------------------------------------------------------------------';
    RAISE INFO 'Deliveries generation with scale factor %', scaleFactor;
    RAISE INFO '-----------------------------------------------------------------------';
    RAISE INFO 'Parameters:';
    RAISE INFO '------------';
    RAISE INFO 'No. of warehouses = %, No. of vehicles = %, No. of customers = %',
      noWarehouses, noVehicles, NoCustomers;
    RAISE INFO 'No. of days = %, Start day = %, Path mode = %, Disturb data = %',
      noDays, startDay, pathMode, disturbData;
    RAISE INFO 'Verbosity = %, Index type = %', 
      messages, indexType;
  END IF;
  RAISE INFO '----------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Execution finished at %', endTime;
  RAISE INFO 'Execution time %', endTime - startTime;
  RAISE INFO 'Call to pgRouting with % paths lasted %',
    noPaths, endPgr - startPgr;
  RAISE INFO 'Number of deliveries generated %', noDeliveries;
  RAISE INFO 'Number of segments generated %', noSegments;
  RAISE INFO '----------------------------------------------------------------------';

  -------------------------------------------------------------------------------------------------

  return 'THE END';
END; $$;

/*
select deliveries_generate();
*/

----------------------------------------------------------------------
-- THE END
----------------------------------------------------------------------
