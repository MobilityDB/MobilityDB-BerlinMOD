/******************************************************************************
 * Executes the 17 BerlinMOD/r benchmark queries
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD-FinalReview-2008-06-18.pdf
 * in MobilityDB.
 * Parameters:
 *    notimes: number of times that each query is run. It is set by default
 *       to 5
 *    detailed: states whether detailed statistics are collected during
 *      the execution. By default it is set to TRUE. 
 * Example of usage:
 *     <Create the function>
 *     SELECT berlinmod_R_queries(1, true);
 * It is supposed that the BerlinMOD data with WGS84 coordinates in CSV format 
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD.html  
 * has been previously loaded using projected (2D) coordinates with SRID 5676
 * https://epsg.io/5676
 * For loading the data see the companion file 'berlinmod_load.sql'
 *****************************************************************************/

DROP FUNCTION IF EXISTS berlinmod_R_queries;
CREATE OR REPLACE FUNCTION berlinmod_R_queries(times integer DEFAULT 1,
  detailed boolean DEFAULT false) 
RETURNS text AS $$
DECLARE
  Query char(5);
  J json;
  StartTime timestamp;
  PlanningTime float;
  ExecutionTime float;
  Duration interval;
  TotalDuration interval = 0;
  NumberRows bigint;
  Experiment_Id int;
BEGIN
FOR Experiment_Id IN 1..times
LOOP
  SET log_error_verbosity to terse;

  CREATE TABLE IF NOT EXISTS execution_tests_explain (
    Experiment_Id int,
    Query char(5),
    StartTime timestamp,
    PlanningTime float,
    ExecutionTime float,
    Duration interval,
    NumberRows bigint,
    J json
  );

  -------------------------------------------------------------------------------
  -- Query 1: What are the models of the vehicles with licence plate numbers 
  -- from Licences?

  Query = 'Q1';
  StartTime := clock_timestamp();

  -- Query 1
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT l.Licence, v.Model AS Model
  FROM Vehicles v, Licences l
  WHERE v.Licence = l.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 2: How many vehicles exist that are passenger cars?

  Query = 'Q2';
  StartTime := clock_timestamp();

  -- Query 2
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT COUNT (Licence)
  FROM Vehicles v
  WHERE VehicleType = 'passenger'
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 3: Where have the vehicles with licences from Licences1 been 
  -- at each of the instants from Instants1?

  Query = 'Q3';
  StartTime := clock_timestamp();

  -- Query 3
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT l.Licence, i.InstantId, i.Instant AS Instant,
    valueAtTimestamp(t.Trip, i.Instant) AS Location
  FROM Trips t, Licences1 l, Instants1 i
  WHERE t.VehicleId = l.VehicleId AND t.Trip::tstzspan @> i.Instant
  ORDER BY l.Licence, i.InstantId
  INTO J;

  /* Check the spgist index. It took more than 10 min in sf11_0
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp AS (
    SELECT DISTINCT t.VehicleId, i.InstantId, i.Instant, valueAtTimestamp(t.Trip, i.Instant) AS Location
    FROM Trips t, Instants1 i
    WHERE t.Trip @> i.Instant )
  SELECT l.Licence, t.InstantId, t.Instant, t.Location
  FROM Temp t, Licences1 l
  WHERE t.VehicleId = l.VehicleId 
  ORDER BY l.Licence, t.InstantId
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 4: Which vehicles have passed the points from Points?

  Query = 'Q4';
  StartTime := clock_timestamp();

  -- Query 4
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT p.PointId, p.Geom, v.Licence
  FROM Trips t, Vehicles v, Points p
  WHERE t.VehicleId = v.VehicleId
  AND ST_Intersects(trajectory(t.Trip), p.Geom) 
  ORDER BY p.PointId, v.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 5: What is the minimum distance between places, where a vehicle with a 
  -- licence from Licences1 and a vehicle with a licence from Licences2 
  -- have been?

  Query = 'Q5';
  StartTime := clock_timestamp();

  -- Query 5
  /* Slower version of the query
  SELECT l1.Licence AS Licence1, l2.Licence AS Licence2,
    MIN(ST_Distance(trajectory(t1.Trip), trajectory(t2.Trip))) AS MinDist
  FROM Trips t1, Licences1 l1, Trips t2, Licences2 l2
  WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = l2.VehicleId
  GROUP BY l1.Licence, l2.Licence 
  ORDER BY l1.Licence, l2.Licence
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp1(Licence1, Trajs) AS (
    SELECT l1.Licence, ST_Collect(trajectory(t1.Trip))
    FROM Trips t1, Licences1 l1
    WHERE t1.VehicleId = l1.VehicleId
    GROUP BY l1.Licence ),
  Temp2(Licence2, Trajs) AS (
    SELECT l2.Licence, ST_Collect(trajectory(t2.Trip))
    FROM Trips t2, Licences2 l2
    WHERE t2.VehicleId = l2.VehicleId
    GROUP BY l2.Licence )
  SELECT Licence1, Licence2, ST_Distance(t1.Trajs, t2.Trajs) AS MinDist
  FROM Temp1 t1, Temp2 t2  
  ORDER BY Licence1, Licence2
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 6: What are the pairs of licence plate numbers of “trucks”
  -- that have ever been as close as 10m or less to each other?

  Query = 'Q6';
  StartTime := clock_timestamp();  
  -- Query 6
  /* Slower version of the query
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT v1.Licence AS Licence1, v2.Licence AS Licence2
  FROM Trips t1, Vehicles v1, Trips t2, Vehicles v2
  WHERE t1.VehicleId = v1.VehicleId AND t2.VehicleId = v2.VehicleId AND
    t1.VehicleId < t2.VehicleId AND v1.VehicleType = 'truck' AND 
    v2.VehicleType = 'truck' AND t1.Trip && expandSpatial(t2.Trip, 10) 
  AND eDwithin(t1.Trip, t2.Trip, 10.0)
  ORDER BY v1.Licence, v2.Licence
  INTO J;
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp(Licence, VehicleId, Trip) AS (
    SELECT v.Licence, t.VehicleId, t.Trip
    FROM Trips t, Vehicles v
    WHERE t.VehicleId = v.VehicleId 
    AND v.VehicleType = 'truck' )
  SELECT t1.Licence, t2.Licence
  FROM Temp t1, Temp t2
  WHERE t1.VehicleId < t2.VehicleId 
  AND t1.Trip && expandSpace(t2.Trip, 10) 
  AND eDwithin(t1.Trip, t2.Trip, 10.0)
  ORDER BY t1.Licence, t2.Licence
  INTO J;
                        
  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;
  --set enable_indexscan = on;
  --set enable_seqscan =on;
  -------------------------------------------------------------------------------
  -- Query 7: What are the licence plate numbers of the passenger cars that have 
  -- reached the points from Points first of all passenger cars during the
  -- complete observation period?

  Query = 'Q7';
  StartTime := clock_timestamp();

  -- Query 7
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp AS (
    SELECT DISTINCT v.Licence, p.PointId, p.Geom, 
      MIN(startTimestamp(atValues(t.Trip,p.Geom))) AS Instant
    FROM Trips t, Vehicles v, Points p
    WHERE t.VehicleId = v.VehicleId AND v.VehicleType = 'passenger'
    AND ST_Intersects(trajectory(t.Trip), p.Geom)
    GROUP BY v.Licence, p.PointId, p.Geom )
  SELECT t1.Licence, t1.PointId, t1.Geom, t1.Instant
  FROM Temp t1
  WHERE t1.Instant <= ALL (
    SELECT t2.Instant
    FROM Temp t2
    WHERE t1.PointId = t2.PointId )
  ORDER BY t1.PointId, t1.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 8: What are the overall travelled distances of the vehicles with licence
  -- plate numbers from Licences1 during the periods from Periods1?

  Query = 'Q8';
  StartTime := clock_timestamp();

  -- Query 8
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT l.Licence, p.PeriodId, p.Period,
  SUM(length(atTime(t.Trip, p.Period))) AS Dist
  FROM Trips t, Licences1 l, Periods1 p
  WHERE t.VehicleId = l.VehicleId AND t.Trip && p.Period
  GROUP BY l.Licence, p.PeriodId, p.Period 
  ORDER BY l.Licence, p.PeriodId
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 9: What is the longest distance that was travelled by a vehicle during 
  -- each of the periods from Periods?

  Query = 'Q9';
  StartTime := clock_timestamp();  
  -- Query 9
  EXPLAIN (ANALYZE, FORMAT JSON)        
  WITH Distances AS (
    SELECT p.PeriodId, p.Period, t.VehicleId,
      SUM(length(atTime(t.Trip, p.Period))) AS Dist
    FROM Trips t, Periods p
    WHERE t.Trip && p.Period
    GROUP BY p.PeriodId, p.Period, t.VehicleId )
  SELECT PeriodId, Period, MAX(Dist) AS MaxDist
  FROM Distances
  GROUP BY PeriodId, Period
  ORDER BY PeriodId
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 10: When and where did the vehicles with licence plate numbers from 
  -- Licences1 meet other vehicles (distance < 3m) and what are the latter
  -- licences?

  Query = 'Q10';
  StartTime := clock_timestamp();  
  -- Query 10
  /* Slower version of the query where the atValue expression in the WHERE
     clause and the SELECT clauses are executed twice
  SELECT l1.Licence AS Licence1, t2.VehicleId AS Car2Id,
    whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) AS Periods
  FROM Trips t1, Licences1 l1, Trips t2, Vehicles v
  WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = v.VehicleId AND t1.VehicleId <> t2.VehicleId
  AND t2.Trip && expandSpace(t1.trip, 3)
  AND whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) IS NOT NULL
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp AS (
    SELECT l1.Licence AS Licence1, t2.VehicleId AS Car2Id,
    whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) AS Periods
    FROM Trips t1, Licences1 l1, Trips t2, Vehicles v
    WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = v.VehicleId AND
      t1.VehicleId <> t2.VehicleId AND t2.Trip && expandSpace(t1.trip, 3) )
  SELECT Licence1, Car2Id, Periods
  FROM Temp
  WHERE Periods IS NOT NULL
  INTO J;
  
  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 11: Which vehicles passed a point from Points1 at one of the 
  -- instants from Instants1?

  Query = 'Q11';
  StartTime := clock_timestamp();                    
  -- Query 11
  EXPLAIN (ANALYZE, FORMAT JSON)  
  WITH Temp AS (
    SELECT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
    FROM Trips t, Points1 p, Instants1 i
    WHERE t.Trip @> stbox(p.Geom, i.Instant) AND
      valueAtTimestamp(t.Trip, i.Instant) = p.Geom )
  SELECT t.PointId, t.Geom, t.InstantId, t.Instant, v.Licence
  FROM Temp t JOIN Vehicles v ON t.VehicleId = v.VehicleId
  ORDER BY t.PointId, t.InstantId, v.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;
  --set enable_seqscan =on;

  -------------------------------------------------------------------------------
  -- Query 12: Which vehicles met at a point from Points1 at an instant 
  -- from Instants1?

  Query = 'Q12';
  StartTime := clock_timestamp();                  
  -- Query 12
  EXPLAIN (ANALYZE, FORMAT JSON)  
  WITH Temp AS (
    SELECT DISTINCT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
    FROM Trips t, Points1 p, Instants1 i
    WHERE t.Trip @> stbox(p.Geom, i.Instant)
    AND valueAtTimestamp(t.Trip, i.Instant) = p.Geom )
  SELECT DISTINCT t1.PointId, t1.Geom, t1.InstantId, t1.Instant, 
    v1.Licence AS Licence1, v2.Licence AS Licence2
  FROM Temp t1 JOIN Vehicles v1 ON t1.VehicleId = v1.VehicleId JOIN
    Temp t2 ON t1.VehicleId < t2.VehicleId AND t1.PointID = t2.PointID AND
    t1.InstantId = t2.InstantId JOIN Vehicles v2 ON t2.VehicleId = v2.VehicleId
  ORDER BY t1.PointId, t1.InstantId, v1.Licence, v2.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 13: Which vehicles travelled within one of the regions from 
  -- Regions1 during the periods from Periods1?    
  Query = 'Q13';
  StartTime := clock_timestamp();

  -- Query 13
  /* Flat version
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT r.RegionId, p.PeriodId, p.Period, v.Licence
  FROM Trips t, Vehicles v, Regions1 r, Periods1 p
  WHERE t.VehicleId = v.VehicleId 
  AND t.trip && stbox(r.Geom, p.Period)
  AND _ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom)
  ORDER BY r.RegionId, p.PeriodId, v.Licence
  INTO J;
  */
  -- Modified version
  EXPLAIN (ANALYZE, FORMAT JSON)           
  WITH Temp AS (
    SELECT DISTINCT r.RegionId, p.PeriodId, p.Period, t.VehicleId
    FROM Trips t, Regions1 r, Periods1 p
    WHERE t.trip && stbox(r.Geom, p.Period)
    AND ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom)
    ORDER BY r.RegionId, p.PeriodId )
  SELECT DISTINCT t.RegionId, t.PeriodId, t.Period, v.Licence
  FROM Temp t, Vehicles v
  WHERE t.VehicleId = v.VehicleId 
  ORDER BY t.RegionId, t.PeriodId, v.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;
  --set enable_seqscan =on;  

  -------------------------------------------------------------------------------
  -- Query 14: Which vehicles travelled within one of the regions from 
  -- Regions1 at one of the instants from Instants1?

  Query = 'Q14';
  StartTime := clock_timestamp();  
  -- Query 14
  /* Flat version
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT r.RegionId, i.InstantId, i.Instant, v.Licence
  FROM Trips t, Vehicles v, Regions1 r, Instants1 i
  WHERE t.VehicleId = v.VehicleId 
  AND t.trip && stbox(r.Geom, i.Instant)
  AND ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant))
  ORDER BY r.RegionId, i.InstantId, v.Licence
  INTO J;
  */
  EXPLAIN (ANALYZE, FORMAT JSON)  
  WITH Temp AS (
    SELECT DISTINCT r.RegionId, i.InstantId, i.Instant, t.VehicleId
    FROM Trips t, Regions1 r, Instants1 i
    WHERE t.Trip && stbox(r.Geom, i.Instant)
    AND ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant)) )
  SELECT DISTINCT t.RegionId, t.InstantId, t.Instant, v.Licence
  FROM Temp t JOIN Vehicles v ON t.VehicleId = v.VehicleId 
  ORDER BY t.RegionId, t.InstantId, v.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;
  --set enable_seqscan =on;

  -------------------------------------------------------------------------------
  -- Query 15: Which vehicles passed a point from Points1 during a period 
  -- from Periods1?

  Query = 'Q15';
  StartTime := clock_timestamp();  
  -- Query 15
  /* Flat version
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT pt.PointId, pt.Geom, pr.PeriodId, pr.Period, v.Licence
  FROM Trips t, Vehicles v, Points1 pt, Periods1 pr
  WHERE t.VehicleId = v.VehicleId 
  AND t.Trip && stbox(pt.Geom, pr.Period)
  AND _ST_Intersects(trajectory(atTime(t.Trip, pr.Period)), pt.Geom)
  ORDER BY pt.PointId, pr.PeriodId, v.Licence
  INTO J;
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp AS (
    SELECT DISTINCT pt.PointId, pt.Geom, pr.PeriodId, pr.Period, t.VehicleId
    FROM Trips t, Points1 pt, Periods1 pr
    WHERE t.Trip && stbox(pt.Geom, pr.Period)
    AND _ST_Intersects(trajectory(atTime(t.Trip, pr.Period)), pt.Geom) )
  SELECT DISTINCT t.PointId, t.Geom, t.PeriodId, t.Period, v.Licence  
  FROM Temp t, Vehicles v
  WHERE t.VehicleId = v.VehicleId 
  ORDER BY t.PointId, t.PeriodId, v.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;
  --set enable_seqscan =on;  

  -------------------------------------------------------------------------------
  -- Query 16: List the pairs of licences for vehicles, the first from 
  -- Licences1, the second from Licences2, where the corresponding 
  -- vehicles are both present within a region from Regions1 during a 
  -- period from QueryPeriod1, but do not meet each other there and then.

  Query = 'Q16';
  StartTime := clock_timestamp();

  -- Query 16
  EXPLAIN (ANALYZE, FORMAT JSON)      
  SELECT p.PeriodId, p.Period, r.RegionId, 
    l1.Licence AS Licence1, l2.Licence AS Licence2
  FROM Trips t1, Licences1 l1, Trips t2, Licences2 l2, Periods1 p, Regions1 r
  WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = l2.VehicleId AND 
    l1.Licence < l2.Licence
  -- AND t1.Trip && stbox(r.Geom, p.Period) AND t2.Trip && stbox(r.Geom, p.Period) 
  AND ST_Intersects(trajectory(atTime(t1.Trip, p.Period)), r.Geom)
  AND ST_Intersects(trajectory(atTime(t2.Trip, p.Period)), r.Geom)
  AND aDisjoint(atTime(t1.Trip, p.Period), atTime(t2.Trip, p.Period))
  ORDER BY PeriodId, RegionId, Licence1, Licence2
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 17: Which point(s) from Points have been visited by a 
  -- maximum number of different vehicles?

  Query = 'Q17';
  StartTime := clock_timestamp();  
  -- Query 17
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH PointCount AS (
    SELECT p.PointId, COUNT(DISTINCT t.VehicleId) AS Hits
    FROM Trips t, Points p
    WHERE ST_Intersects(trajectory(t.Trip), p.Geom)
    GROUP BY p.PointId )
  SELECT PointId, Hits
  FROM PointCount AS p
  WHERE p.Hits = ( SELECT MAX(Hits) FROM PointCount )
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  TotalDuration = TotalDuration + Duration;

END LOOP;
-------------------------------------------------------------------------------
  RAISE INFO 'Total Duration of all queries: %', TotalDuration;
  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
