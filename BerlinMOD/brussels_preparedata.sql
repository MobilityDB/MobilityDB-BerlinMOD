-------------------------------------------------------------------------------
-- Prepare the BerlinMOD generator using the OSM data from Brussels
-------------------------------------------------------------------------------
 
-- We need to convert the resulting data in Spherical Mercator (SRID = 3857)
-- We create two tables for that

DROP TABLE IF EXISTS RoadSegments;
CREATE TABLE RoadSegments(SegmentId bigint PRIMARY KEY, Name text, 
  OsmId bigint, TagId integer, SegmentLength float, SourceNode bigint, 
  TargetNode bigint, SourceOsm bigint, TargetOsm bigint, TimeSecsFwd float,
  TimeSecsBwd float, OneWay integer, MaxSpeedFwd float, MaxSpeedBwd float, 
  Priority float, SegmentGeo geometry);
INSERT INTO RoadSegments(SegmentId, Name, OsmId, TagId, SegmentLength, 
  SourceNode, TargetNode, SourceOsm, TargetOsm, TimeSecsFwd, TimeSecsBwd, 
  OneWay, MaxSpeedFwd, MaxSpeedBwd, Priority, SegmentGeo)
SELECT gid, name, osm_id, tag_id, length_m, source, target, source_osm,
  target_osm, cost_s, reverse_cost_s, one_way, maxspeed_forward,
  maxspeed_backward, priority, ST_Transform(the_geom, 3857)
FROM ways;

-- The nodes table should contain ONLY the vertices that belong to the largest
-- connected component in the underlying map. Like this, we guarantee that
-- there will be a non-NULL shortest path between any two nodes.
DROP TABLE IF EXISTS Nodes;
CREATE TABLE Nodes(NodeId bigint PRIMARY KEY, OsmId bigint, Geom geometry);
INSERT INTO Nodes(NodeId, OsmId, Geom)
WITH Components AS (
  SELECT * FROM pgr_strongComponents(
    'SELECT SegmentId AS id, SourceNode AS source, TargetNode AS target, '
    'SegmentLength AS cost, SegmentLength * sign(TimeSecsBwd) AS reverse_cost '
    'FROM RoadSegments') ),
LargestComponent AS (
  SELECT component, COUNT(*) FROM Components
  GROUP BY component ORDER BY COUNT(*) DESC LIMIT 1),
Connected AS (
  SELECT id, osm_id, the_geom AS Geom
  FROM ways_vertices_pgr w, LargestComponent l, Components c
  WHERE w.id = c.node AND c.component = l.component
)
SELECT ROW_NUMBER() OVER (), osm_id, ST_Transform(Geom, 3857) AS Geom
FROM Connected;

CREATE UNIQUE INDEX Nodes_NodeId_idx ON Nodes USING btree(NodeId);
CREATE INDEX Nodes_osm_id_idx ON Nodes USING btree(OsmId);
CREATE INDEX Nodes_geom_gist_idx ON NODES USING gist(Geom);

UPDATE RoadSegments r SET
SourceNode = (SELECT NodeId FROM Nodes n WHERE n.OsmId = r.SourceOsm),
TargetNode = (SELECT NodeId FROM Nodes n WHERE n.OsmId = r.TargetOsm);

-- Delete the edges whose source or target node has been removed
DELETE FROM RoadSegments WHERE SourceNode IS NULL OR TargetNode IS NULL;

CREATE INDEX RoadSegments_SegmentGeo_gist_idx ON RoadSegments USING gist(SegmentGeo);

/*
-- The following were obtained FROM the OSM file extracted on March 26, 2023
SELECT COUNT(*) FROM RoadSegments;
-- 95025
SELECT COUNT(*) FROM Nodes;
-- 80304
*/

-------------------------------------------------------------------------------
-- Get municipalities data to define home and work regions
-------------------------------------------------------------------------------

-- Brussels' municipalities data from the following sources
-- https://en.wikipedia.org/wiki/List_of_municipalities_of_the_Brussels-Capital_Region
-- http://ibsa.brussels/themes/economie

DROP TABLE IF EXISTS Municipalities;
CREATE TABLE Municipalities(MunicipalityId int PRIMARY KEY, 
  MunicipalityName text UNIQUE, Population int, PercPop float,
  PopDensityKm2 int, NoEnterp int, PercEnterp float);
INSERT INTO Municipalities VALUES
(1,'Anderlecht',118241,0.10,6680,6460,0.08),
(2,'Auderghem - Oudergem',33313,0.03,3701,2266,0.03),
(3,'Berchem-Sainte-Agathe - Sint-Agatha-Berchem',24701,0.02,8518,1266,0.02),
(4,'Etterbeek',176545,0.15,5415,14204,0.18),
(5,'Evere',47414,0.04,15295,3769,0.05),
(6,'Forest - Vorst',40394,0.03,8079,1880,0.02),
(7,'Ganshoren',55746,0.05,8991,3436,0.04),
(8,'Ixelles - Elsene',24596,0.02,9838,1170,0.01),
(9,'Jette',86244,0.07,13690,9304,0.12),
(10,'Koekelberg',51933,0.04,10387,2403,0.03),
(11,'Molenbeek-Saint-Jean - Sint-Jans-Molenbeek',21609,0.02,18008,1064,0.01),
(12,'Saint-Gilles - Sint-Gillis',96629,0.08,16378,4362,0.05),
(13,'Saint-Josse-ten-Noode - Sint-Joost-ten-Node',50471,0.04,20188,3769,0.05),
(14,'Schaerbeek - Schaarbeek',27115,0.02,24650,1411,0.02),
(15,'Uccle - Ukkel',133042,0.11,16425,7511,0.09),
(16,'Ville de Bruxelles - Stad Brussel',82307,0.07,3594,7435,0.09),
(17,'Watermael-Boitsfort - Watermaal-Bosvoorde',24871,0.02,1928,1899,0.02),
(18,'Woluwe-Saint-Lambert - Sint-Lambrechts-Woluwe',55216,0.05,7669,3590,0.04),
(19,'Woluwe-Saint-Pierre - Sint-Pieters-Woluwe',41217,0.03,4631,2859,0.04);

-- Compute the geometry of the Municipalities from the boundaries in planet_osm_line

DROP TABLE IF EXISTS MunicipalitiesGeo;
CREATE TABLE MunicipalitiesGeo(MunicipalityName, Geom) AS
SELECT name, way
FROM planet_osm_line
WHERE name IN ( SELECT MunicipalityName FROM Municipalities );

-- The geometries of the Municipalities are of type Linestring. They need to be
-- converted into polygons.

ALTER TABLE MunicipalitiesGeo ADD COLUMN GeomPoly geometry;
UPDATE MunicipalitiesGeo
SET GeomPoly = ST_MakePolygon(Geom);

-- Disjoint components of Ixelles and Saint-Gilles are encoded as two different
-- features. For this reason ST_Union is needed to make a multipolygon
ALTER TABLE Municipalities ADD COLUMN MunicipalityGeo geometry;
UPDATE Municipalities m
SET MunicipalityGeo = (
  SELECT ST_Union(GeomPoly) FROM MunicipalitiesGeo g
  WHERE m.MunicipalityName = g.MunicipalityName);

CREATE INDEX Municipalities_MunicipalityGeo_gist_idx ON Municipalities 
USING gist(MunicipalityGeo);

-- Clean up tables
DROP TABLE IF EXISTS MunicipalitiesGeo;

-- Create home/work regions and nodes

DROP TABLE IF EXISTS HomeRegions;
CREATE TABLE HomeRegions(RegionId, Priority, Weight, Prob, CumulProb, Geom) AS
SELECT MunicipalityId, MunicipalityId, Population, PercPop,
  SUM(PercPop) OVER (ORDER BY MunicipalityId ASC ROWS 
    BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulProb, MunicipalityGeo
FROM Municipalities;

CREATE INDEX HomeRegions_geom_gist_idx ON HomeRegions USING gist(Geom);

DROP TABLE IF EXISTS WorkRegions;
CREATE TABLE WorkRegions(RegionId, Priority, Weight, Prob, CumulProb, Geom) AS
SELECT MunicipalityId, MunicipalityId, NoEnterp, PercEnterp,
  SUM(PercEnterp) OVER (ORDER BY MunicipalityId ASC ROWS
    BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulProb, MunicipalityGeo
FROM Municipalities;

CREATE INDEX WorkRegions_geom_gist_idx ON WorkRegions USING gist(Geom);

DROP TABLE IF EXISTS HomeNodes;
CREATE TABLE HomeNodes AS
SELECT n.*, r.RegionId, r.CumulProb
FROM Nodes n, HomeRegions r
WHERE ST_Intersects(n.Geom, r.Geom);

CREATE INDEX HomeNodes_NodeId_idx ON HomeNodes USING btree(NodeId);

DROP TABLE IF EXISTS WorkNodes;
CREATE TABLE WorkNodes AS
SELECT n.*, r.RegionId
FROM Nodes n, WorkRegions r
WHERE ST_Intersects(n.Geom, r.Geom);

CREATE INDEX WorkNodes_NodeId_idx ON WorkNodes USING btree(NodeId);

-------------------------------------------------------------------------------
-- THE END
-------------------------------------------------------------------------------
