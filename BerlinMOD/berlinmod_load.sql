/******************************************************************************
 * Loads the BerlinMOD data with WGS84 coordinates in CSV format 
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD.html  
 * into MobilityDB using projected (2D) coordinates with SRID 3857
 * https://epsg.io/3857
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

  CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

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
  EXECUTE format('COPY Periods(PeriodId, Period) FROM ''%speriods.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);

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
  
  CREATE VIEW Periods1 (PeriodId, Period) AS
  SELECT PeriodId, Period
  FROM Periods
  LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Points';
  DROP TABLE IF EXISTS Points CASCADE;
  CREATE TABLE Points
  (
    PointId integer PRIMARY KEY,
    Geom geometry(Point,3857)
  );
  EXECUTE format('COPY Points(PointId, Geom) FROM ''%spoints.csv'' '
    'DELIMITER '','' CSV HEADER', fullpath);

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

  CREATE VIEW Points1 (PointId, Geom) AS
  SELECT PointId, Geom
  FROM Points
  LIMIT 10;

--------------------------------------------------------------

  RAISE NOTICE 'Creating table Regions';
  DROP TABLE IF EXISTS Regions CASCADE;
  CREATE TABLE Regions
  (
    RegionId integer PRIMARY KEY,
    Geom Geometry(Polygon,3857)
  );
  EXECUTE format('COPY Regions(RegionId, Geom) '
    'FROM ''%sregions.csv'' DELIMITER '','' CSV HEADER', fullpath);
  
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

  RAISE NOTICE 'Creating table Vehicles';
  DROP TABLE IF EXISTS Vehicles CASCADE;
  CREATE TABLE Vehicles
  (
    VehicleId integer PRIMARY KEY,
    Licence varchar(32),
    VehicleType varchar(32),
    Model varchar(32)
  );
  EXECUTE format('COPY Vehicles(VehicleId, Licence, VehicleType, Model) '
    'FROM ''%svehicles.csv'' DELIMITER '','' CSV HEADER', fullpath);
  
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
  EXECUTE format('COPY Licences(LicenceId, Licence, VehicleId) '
    'FROM ''%slicences.csv'' DELIMITER '','' CSV HEADER', fullpath);

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
    StartDate date,
    SeqNo int,
    Point geometry,
    t timestamptz,
    UNIQUE (TripId, StartDate, SeqNo, T),
    FOREIGN KEY (VehicleId) REFERENCES Vehicles(VehicleId)
  );
  EXECUTE format('COPY TripsInput(TripId, VehicleId, StartDate, SeqNo, Point, T) '
    'FROM ''%stripsinput.csv'' DELIMITER '','' CSV HEADER', fullpath);

  DROP TABLE IF EXISTS Trips CASCADE;
  CREATE TABLE Trips
  (
    TripId integer PRIMARY KEY,
    VehicleId integer NOT NULL,
    StartDate date,
    SeqNo int,
    Trip tgeompoint NOT NULL,
    Trajectory geometry,
    FOREIGN KEY (VehicleId) REFERENCES Vehicles(VehicleId) 
  );
  
  INSERT INTO Trips(TripId, VehicleId, StartDate, SeqNo, Trip)
  SELECT TripId, VehicleId, StartDate, SeqNo,
    tgeompointSeq(array_agg(tgeompoint(Point, T) ORDER BY T))
  FROM TripsInput
  GROUP BY TripId, VehicleId, StartDate, SeqNo;
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

  -- DROP TABLE TripsInput;

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
