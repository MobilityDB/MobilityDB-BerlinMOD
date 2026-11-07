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

-------------------------------------------------------------------------------
-- Get municipalities data to define home and work regions
-------------------------------------------------------------------------------

-- Hanoi's municipalities data from the following sources
-- https://en.wikipedia.org/wiki/Hanoi#Administrative_divisions
-- https://www.nso.gov.vn/en/px-web/?pxid=E0529&theme=Enterprise 
-- Total number of enterprises in 2022: 150522

DROP TABLE IF EXISTS Municipalities;
CREATE TABLE Municipalities(MunicipalityId int PRIMARY KEY, 
  MunicipalityName text UNIQUE, Population int, PercPop float,
  PopDensityKm2 int, NoEnterp int, PercEnterp float);
INSERT INTO Municipalities VALUES
(1,'Quận Ba Đình',223100,0.03,24224,3981,0.03),
(2,'Quận Bắc Từ Liêm',359200,0.04,7938,6409,0.05),
(3,'Quận Cầu Giấy',294500,0.03,23788,5255,0.03),
(4,'Quận Đống Đa',377900,0.04,37980,6743,0.04),
(5,'Quận Hà Đông',435500,0.05,8773,7771,0.05),
(6,'Quận Hai Bà Trưng',293900,0.03,28645,5244,0.03),
(7,'Quận Hoàn Kiếm',140200,0.02,26206,2502,0.02),
(8,'Quận Hoàng Mai',539800,0.06,13431,9632,0.06),
(9,'Quận Long Biên',342700,0.04,5703,6115,0.04),
(10,'Quận Nam Từ Liêm',290500,0.03,9030,5184,0.03),
(11,'Quận Tây Hồ',166600,0.02,6834,2973,0.02),
(12,'Quận Thanh Xuân',293400,0.03,31996,5235,0.03),
(13,'Thị xã Sơn Tây',156500,0.02,1335,2793,0.02),
(14,'Huyện Ba Vì',307600,0.04,729,5489,0.04),
(15,'Huyện Chương Mỹ',351200,0.04,1479,6267,0.04),
(16,'Huyện Đan Phượng',186100,0.02,2391,3321,0.02),
(17,'Huyện Đông Anh',411700,0.05,2217,7346,0.05),
(18,'Huyện Gia Lâm',299800,0.04,2570,5250,0.04),
(19,'Huyện Hoài Đức',282300,0.03,3324,5037,0.03),
(20,'Huyện Mê Linh',254400,0.03,1801,4539,0.03),
(21,'Huyện Mỹ Đức',210200,0.02,929,3751,0.02),
(22,'Huyện Phú Xuyên',231900,0.03,1336,4138,0.03),
(23,'Huyện Phúc Thọ',195300,0.02,1648,3485,0.02),
(24,'Huyện Quốc Oai',204400,0.02,1352,3647,0.02),
(25,'Huyện Sóc Sơn', 361200,0.04,1182,6445,0.04),
(26,'Huyện Thạch Thất',226000,0.03,1205,4033,0.03),
(27,'Huyện Thanh Oai',225900,0.03,1815,4031,0.03),
(28,'Huyện Thanh Trì',294100,0.03,4632,5248,0.03),
(29,'Huyện Thường Tín',263800,0.03,2027,4707,0.03),
(30,'Huyện Ứng Hòa',215900,0.03,1147,3852,0.03);


ALTER TABLE Municipalities ADD COLUMN MunicipalityGeo geometry;
UPDATE Municipalities m
SET MunicipalityGeo = p.way
FROM planet_osm_polygon p
WHERE p.name = m.MunicipalityName;

CREATE INDEX Municipalities_MunicipalityGeo_gist_idx ON Municipalities 
USING gist(MunicipalityGeo);

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
