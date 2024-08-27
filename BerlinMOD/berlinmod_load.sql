/******************************************************************************
 * Loads the BerlinMOD data with WGS84 coordinates in CSV format 
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD.html  
 * into MobilityDB using projected (2D) coordinates with SRID 5676
 * https://epsg.io/5676
 * Parameters:
 *    fullpath: states the full path in which the CSV files are located.
 *    gist: states whether GiST or SP-GiST indexes are created on the tables.
 *      By default it is set to TRUE and thus creates GiST indexes.
 * Example of usage on psql:
 *     CREATE EXTENSION mobilitydb CASCADE;
 *     \i berlinmod_load.sql
 *     SELECT berlinmod_load('/home/mobilitydb/data/', false);
 *****************************************************************************/

DROP FUNCTION IF EXISTS berlinmod_load(fullpath text, gist bool);
CREATE OR REPLACE FUNCTION berlinmod_load(fullpath text, gist bool DEFAULT TRUE) 
RETURNS text AS $$
BEGIN
--------------------------------------------------------------

  RAISE NOTICE 'Creating table Instants';
  DROP TABLE IF EXISTS Instants CASCADE;
  CREATE TABLE Instants
  (
    InstantId integer PRIMARY KEY,
    Instant timestamptz
  );
  EXECUTE format('COPY Instants(InstantId, Instant) FROM ''%sinstants.csv'' DELIMITER '','' CSV HEADER', fullpath);

  /* There are NO duplicate instants in Instants
  SELECT COUNT(*)
  FROM Instants I1, Instants I2
  WHERE I1.InstantId < I2.InstantId AND I1.Instant = I2.Instant;
  */

  CREATE VIEW Instants1 (InstantId, Instant) AS
  SELECT InstantId, Instant 
  FROM Instants
  LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Periods';
  DROP TABLE IF EXISTS Periods CASCADE;
  CREATE TABLE Periods
  (
    PeriodId integer PRIMARY KEY,
    Period tstzspan
  );
  EXECUTE format('COPY Periods(PeriodId, Period) FROM ''%speriods.csv'' DELIMITER '','' CSV HEADER', fullpath);
  UPDATE Periods
  SET Period = period(StartP,EndP);

  IF gist THEN
    CREATE INDEX Periods_Period_gist_idx ON Periods USING gist (Period);
  ELSE
    CREATE INDEX Periods_Period_spgist_idx ON Periods USING spgist (Period);
  END IF;
  
  /* There are NO duplicate periods in Periods
  SELECT COUNT(*)
  FROM Periods P1, Periods P2
  WHERE P1.PeriodId < P2.PeriodId AND
  P1.StartP = P2.StartP AND P1.EndP = P2.EndP;
  */
  
  CREATE VIEW Periods1 (PeriodId, StartP, EndP, Period) AS
  SELECT PeriodId, StartP, EndP, Period
  FROM Periods
  LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Points';
  DROP TABLE IF EXISTS Points CASCADE;
  CREATE TABLE Points
  (
    PointId integer PRIMARY KEY,
    PosX double precision,
    PosY double precision,
    Geom geometry(Point,5676)
  );
  EXECUTE format('COPY Points(PointId, PosX, PosY) FROM ''%spoints.csv'' DELIMITER '','' CSV HEADER', fullpath);
  UPDATE Points
  SET Geom = ST_Transform(ST_SetSRID(ST_MakePoint(PosX, PosY),4326),5676);

  IF gist THEN
    CREATE INDEX Points_geom_gist_idx ON Points USING gist(Geom);
  ELSE
    CREATE INDEX Points_geom_spgist_idx ON Points USING spgist(Geom);
  END IF;
  
  /* There are NO duplicate points in Points
  SELECT COUNT(*)
  FROM Points P1, Points P2
  WHERE P1.PointId < P2.PointId AND
  P1.PosX = P2.PosX AND P1.PosY = P2.PosY;
  */

  CREATE VIEW Points1 (PointId, PosX, PosY, Geom) AS
  SELECT PointId, PosX, PosY, Geom
  FROM Points
  LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Regions';
  DROP TABLE IF EXISTS RegionsInput CASCADE;
  CREATE TABLE RegionsInput
  (
    RegionId integer,
    PointNo integer,
    PosX double precision,
    PosY double precision,
    PRIMARY KEY (RegionId, PointNo)
  );
  EXECUTE format('COPY RegionsInput(RegionId, PointNo, PosX, PosY) FROM ''%sregions.csv'' DELIMITER '','' CSV HEADER', fullpath);
  
  DROP TABLE IF EXISTS Regions CASCADE;
  CREATE TABLE Regions
  (
    RegionId integer PRIMARY KEY,
    Geom Geometry(Polygon,5676)
  );
  INSERT INTO Regions(RegionId, Geom)
  SELECT RegionId, ST_MakePolygon(ST_MakeLine(array_agg(
    ST_Transform(ST_SetSRID(ST_MakePoint(PosX, PosY), 4326), 5676) ORDER BY PointNo)))
  FROM RegionsInput
  GROUP BY RegionId;

  IF gist THEN
    CREATE INDEX Regions_geom_gist_idx ON Regions USING gist (Geom);
  ELSE
    CREATE INDEX Regions_geom_spgist_idx ON Regions USING spgist (Geom);
  END IF;

  CREATE VIEW Regions1 (RegionId, Geom) AS
  SELECT RegionId, Geom
  FROM Regions
  LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Vehicles';
  DROP TABLE IF EXISTS Vehicles CASCADE;
  CREATE TABLE Vehicles
  (
    VehicleId integer PRIMARY KEY,
    Licence varchar(32),
    Type varchar(32),
    Model varchar(32)
  );
  EXECUTE format('COPY Vehicles(VehicleId, Licence, Type, Model) FROM ''%svehicles.csv'' DELIMITER '','' CSV HEADER', fullpath);
  
--------------------------------------------------------------

  RAISE NOTICE 'Creating table Licences';
  DROP TABLE IF EXISTS Licences CASCADE;
  CREATE TABLE Licences
  (
    LicenceId integer PRIMARY KEY,
    Licence text,
    VehicleId integer,
    FOREIGN KEY (VehicleId) REFERENCES Vehicles(VehicleId)
  );
  EXECUTE format('COPY Licences(LicenceId, Licence, VehicleId) FROM ''%slicences.csv'' DELIMITER '','' CSV HEADER', fullpath);

  CREATE INDEX Licences_VehId_idx ON Licences USING btree (VehicleId);

  /* There are duplicate licences in Licences, e.g., in SF 0.005
  SELECT COUNT(*)
  FROM Licences L1, Licences L2
  WHERE L1.LicenceId < L2.LicenceId AND L1.Licence = L2.Licence;
  */

  CREATE VIEW Licences1 (LicenceId, Licence, VehicleId) AS
  SELECT LicenceId, Licence, VehicleId
  FROM Licences
  LIMIT 10;

  CREATE VIEW Licences2 (LicenceId, Licence, VehicleId) AS
  SELECT LicenceId, Licence, VehicleId
  FROM Licences
  LIMIT 10 OFFSET 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Trips';
  DROP TABLE IF EXISTS TripsInput CASCADE;
  CREATE TABLE TripsInput
  (
    TripId integer,
    VehicleId integer,
    t timestamptz,
    PosX double precision,
    PosY double precision,
    Trip tgeompoint,
    UNIQUE (VehicleId, T),
    FOREIGN KEY (VehicleId) REFERENCES Vehicles(VehicleId)
  );
  EXECUTE format('COPY TripsInput(TripId, VehicleId, PosX, PosY, T) FROM ''%strips.csv'' DELIMITER '','' CSV HEADER', fullpath);

  DROP TABLE IF EXISTS Trips CASCADE;
  CREATE TABLE Trips
  (
    TripId integer PRIMARY KEY,
    VehicleId integer NOT NULL,
    Trip tgeompoint NOT NULL,
    Trajectory geometry,
    FOREIGN KEY (VehicleId) REFERENCES Vehicles(VehicleId) 
  );
  
  INSERT INTO Trips(TripId, VehicleId, Trip)
  SELECT TripId, VehicleId, tgeompoint_seq(array_agg(tgeompoint_inst(
    ST_Transform(ST_SetSRID(ST_MakePoint(PosX, PosY), 4326), 5676), T) ORDER BY T))
  FROM TripsInput
  GROUP BY VehicleId, TripId;
  UPDATE Trips
  SET Trajectory = trajectory(Trip);

  CREATE INDEX Trips_VehId_idx ON Trips USING btree(VehicleId);

  IF gist THEN
    CREATE INDEX Trips_gist_idx ON Trips USING gist(trip);
  ELSE
    CREATE INDEX Trips_spgist_idx ON Trips USING spgist(trip);
  END IF;
  
  DROP VIEW IF EXISTS Trips1;
  CREATE VIEW Trips1 AS
  SELECT * FROM Trips LIMIT 100;
  
-------------------------------------------------------------------------------
/*
-- Loads the BerlinMOD dataset using PostGIS trajectories  
-- https://postgis.net/docs/reference.html#Temporal
   
  DROP TABLE IF EXISTS TripsGeo3DM;
  CREATE TABLE TripsGeo3DM AS
  SELECT VehicleId, TripId, Trip::geometry AS Trip
  FROM Trips;

  CREATE INDEX TripsGeo3DM_VehId_idx ON TripsGeo3DM USING btree (VehicleId);
  CREATE UNIQUE INDEX TripsGeo3DM_pkey_idx ON TripsGeo3DM USING btree (VehicleId, TripId);
  CREATE INDEX TripsGeo3DM_spatial_idx ON TripsGeo3DM USING gist (Trip);
*/
-------------------------------------------------------------------------------

  DROP TABLE RegionsInput;
  -- DROP TABLE TripsInput;

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
