/*-----------------------------------------------------------------------------
These scripts are meant for debugging the generator
-----------------------------------------------------------------------------*/

CREATE OR REPLACE FUNCTION countRows()
RETURNS bool AS $$
Declare
          record record;
          numRows integer;
BEGIN
          DROP TABLE IF EXISTS testcounts;
          CREATE TABLE testcounts(table_name text, numRows integer);
           FOR record in SELECT table_name
                                  FROM information_schema.tables
                                 WHERE table_schema not in ('pg_catalog', 'information_schema')
                                 AND table_type='BASE TABLE'
                                 ORDER BY table_name ASC
          LOOP
                     EXECUTE concat('SELECT COUNT(*) FROM ',record.table_name) INTO numRows;
                     INSERT INTO testcounts values(record.table_name, numRows);
          END LOOP;
RETURN true;
END;
$$ LANGUAGE plpgsql STRICT;

SELECT countRows();
SELECT * FROM testcounts;

-------------------------------------------------------------------------------
--      table_name      | numrows 
---------------------+---------
--  communes            |      19
--  configuration       |      16
--  destinations        |     840
--  edges               |   80826
--  homenodes           |   43744
--  homeregions         |      19
--  leisuretrip         |     558
--  licences            |     141
--  neighbourhood       |  395529
--  nodes               |   66781
--  paths               |   87740
--  pg_temporal_opcache |     702
--  planet_osm_line     |  100511
--  planet_osm_point    |  157997
--  planet_osm_polygon  |  334395
--  planet_osm_roads    |   11584
--  pointsofinterest    |       0
--  queryinstants       |     100
--  queryperiods        |     100
--  querypoints         |     100
--  queryregions        |     100
--  spatial_ref_sys     |    8500
--  testcounts          |      22
--  trips               |    1686
--  vehicle             |     141
--  ways                |   82810
--  ways_vertices_pgr   |   68425
--  worknodes           |   43744
--  workregions         |      19
--  (29 rows)


-- THE END
-------------------------------------------------------------------------------
