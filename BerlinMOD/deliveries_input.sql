
CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

DROP TABLE IF EXISTS Instants CASCADE;
CREATE TABLE Instants (
  InstantId integer PRIMARY KEY,
  Instant timestamptz NOT NULL
);
DROP TABLE IF EXISTS Periods CASCADE;
CREATE TABLE Periods (
  PeriodId integer PRIMARY KEY,
  Period tstzspan
);
DROP TABLE IF EXISTS Points CASCADE;
CREATE TABLE Points (
  PointId integer PRIMARY KEY,
  Geom geometry(Point, 3857) NOT NULL
);
DROP TABLE IF EXISTS Regions CASCADE;
CREATE TABLE Regions (
  RegionId integer PRIMARY KEY,
  Geom geometry(Polygon, 3857) NOT NULL
);

DROP TABLE IF EXISTS VehicleBrands CASCADE;
CREATE TABLE VehicleBrands (
  BrandId integer PRIMARY KEY,
  BrandName text NOT NULL
);

DROP TABLE IF EXISTS VehicleClasses CASCADE;
CREATE TABLE VehicleClasses (
  ClassId integer PRIMARY KEY,
  ClassName text NOT NULL,
  DutyClass text NOT NULL,
  WeightLimit text NOT NULL
);

DROP TABLE IF EXISTS Vehicles CASCADE;
CREATE TABLE Vehicles (
  VehicleId integer PRIMARY KEY,
  Licence text NOT NULL,
  MakeYear int NOT NULL,
  BrandId int NOT NULL, 
  ClassId int NOT NULL,
  WarehouseId int NOT NULL
);
DROP TABLE IF EXISTS Warehouses CASCADE;
CREATE TABLE Warehouses (
  WarehouseId integer PRIMARY KEY,
  Geom geometry(Point, 3857) NOT NULL
);
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

DROP TABLE IF EXISTS RoadSegments;
CREATE TABLE RoadSegments (
  segmentid bigint PRIMARY KEY,
  name text,
  osm_id bigint,
  tag_id integer,
  segmentlength float,
  sourcenode bigint,
  targetnode bigint,
  source_osm bigint,
  target_osm bigint,
  cost_s float,
  reverse_cost_s float,
  one_way integer,
  maxspeedfwd float,
  maxspeedbwd float,
  priority float,
  SegmentGeo geometry
);

DROP TABLE IF EXISTS DeliveriesInput CASCADE;
CREATE TABLE DeliveriesInput (
  DeliveryId integer NOT NULL,
  VehicleId integer NOT NULL REFERENCES Vehicles(VehicleId),
  StartDate date,
  NoCustomers int,
  Point geometry(Point, 3857) NOT NULL,
  T timestamptz NOT NULL,
  PRIMARY KEY (DeliveryId, T)
);
DROP TABLE IF EXISTS Deliveries CASCADE;
CREATE TABLE Deliveries (
  DeliveryId integer PRIMARY KEY,
  VehicleId integer NOT NULL REFERENCES Vehicles(VehicleId),
  StartDate date,
  NoCustomers int,
  Trip tgeompoint NOT NULL,
  Trajectory geometry
);

COPY Instants(InstantId, Instant)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/instants.csv'
DELIMITER ',' CSV HEADER;
COPY Periods(PeriodId, Period)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/periods.csv'
DELIMITER ',' CSV HEADER;
COPY Points(PointId, Geom)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/points.csv'
DELIMITER ',' CSV HEADER;
COPY Regions(RegionId, Geom)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/regions.csv'
DELIMITER ',' CSV HEADER;
COPY VehicleBrands(BrandId, BrandName)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/vehiclebrands.csv'
DELIMITER ',' CSV HEADER;
COPY VehicleClasses(ClassId, ClassName, DutyClass, WeightLimit)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/vehicleclasses.csv'
DELIMITER ',' CSV HEADER;
COPY Vehicles(VehicleId, Licence, MakeYear, BrandId, ClassId, WarehouseId)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/vehicles.csv'
DELIMITER ',' CSV HEADER;
COPY Warehouses(WarehouseId, Geom)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/warehouses.csv'
DELIMITER ',' CSV HEADER;
COPY Municipalities(MunicipalityId, MunicipalityName, Population, PercPop, PopDensityKm2,
  NoEnterp, PercEnterp, MunicipalityGeo)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/municipalities.csv'
DELIMITER ',' CSV HEADER;
COPY RoadSegments
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/roadsegments.csv'
DELIMITER ',' CSV HEADER;
COPY DeliveriesInput(DeliveryId, VehicleId, StartDate, NoCustomers, Point, T)
FROM '/home/esteban/src/MobilityDB-BerlinMOD/BerlinMOD/deliveries_sf0.1/deliveriesinput.csv'
DELIMITER ',' CSV HEADER;

INSERT INTO Deliveries(DeliveryId, VehicleId, StartDate, NoCustomers, Trip)
SELECT DeliveryId, VehicleId, StartDate, NoCustomers,
  tgeompointSeq(array_agg(tgeompoint(Point, T) ORDER BY T))
FROM DeliveriesInput
GROUP BY VehicleId, DeliveryId, StartDate, NoCustomers;

UPDATE Deliveries SET Trajectory = trajectory(Trip);

CREATE INDEX Instants_Instant_Idx ON Instants USING btree(Instant);
CREATE INDEX Periods_Period_Idx ON Periods USING gist(Period);
CREATE INDEX Points_Geom_Idx ON Points USING gist(Geom);
CREATE INDEX Regions_Geom_Idx ON Regions USING gist(Geom);
CREATE INDEX Deliveries_VehicleId_Idx ON Deliveries USING btree(VehicleId);
CREATE INDEX Deliveries_Trip_gist_Idx ON Deliveries USING gist(trip);

CREATE VIEW Instants1 AS SELECT * FROM Instants LIMIT 10;
CREATE VIEW Periods1 AS SELECT * FROM Periods LIMIT 10;
CREATE VIEW Points1 AS SELECT * FROM Points LIMIT 10;
CREATE VIEW Regions1 AS SELECT * FROM Regions LIMIT 10;
CREATE VIEW Vehicles1 AS SELECT * FROM Vehicles LIMIT 10;
CREATE VIEW Delivery1 AS SELECT * FROM Deliveries LIMIT 100;
