/******************************************************************************
 * Executes the 17 BerlinMOD/R benchmark queries
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD-FinalReview-2008-06-18.pdf
 * in MobilityDB.
 * Parameters:
 *    notimes: number of times that each query is run. It is set by default
 *       to 5
 *    detailed: states whether detailed statistics are collected during
 *      the execution. By default it is set to TRUE. 
 * Example of usage:
 *     <Create the function>
 *     SELECT berlinmod_R_queries(1, true)
 * It is supposed that the BerlinMOD data with WGS84 coordinates in CSV format 
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD.html  
 * has been previously loaded using projected (2D) coordinates with SRID 5676
 * https://epsg.io/5676
 * For loading the data see the companion file 'berlinmod_load.sql'
 *****************************************************************************/

/*
 * This version of the file is for running Citus where query 10 has been
 * commented out
 */
 
/*
DROP TABLE IF EXISTS execution_tests_explain;
CREATE TABLE execution_tests_explain (
  Experiment_Id int,
  Query char(5),
  StartTime timestamp,
  PlanningTime float,
  ExecutionTime float,
  Duration interval,
  NumberRows bigint,
  J json
);
*/

DROP FUNCTION IF EXISTS berlinmod_R_queries_citus;
CREATE OR REPLACE FUNCTION berlinmod_R_queries_citus(times integer DEFAULT 1,
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
  SET citus.enable_repartition_joins=ON;

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
  -- from Vehicles?

  Query = 'Q1';
  StartTime := clock_timestamp();

  -- Query 1
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT DISTINCT L.Licence, V.Model AS Model
  FROM Vehicles V, Vehicles L
  WHERE V.Licence = L.Licence
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
  FROM Vehicles V
  WHERE Type = 'passenger'
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
  SELECT DISTINCT L.Licence, I.InstantId, I.Instant AS Instant,
    valueAtTimestamp(T.Trip, I.Instant) AS Pos
  FROM Trips T, Licences1 L, Instants1 I
  WHERE T.VehId = L.VehId AND T.Trip::tstzspan @> I.Instant
  ORDER BY L.Licence, I.InstantId
  INTO J;

  /* Check the spgist index. It took more than 10 min in sf11_0
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp AS (
    SELECT DISTINCT T.VehId, I.InstantId, I.Instant, valueAtTimestamp(T.Trip, I.Instant) AS Pos
    FROM Trips T, Instants1 I
    WHERE T.Trip @> I.Instant )
  SELECT L.Licence, T.InstantId, T.Instant, T.Pos
  FROM Temp T, Licences1 L
  WHERE T.VehId = L.VehId 
  ORDER BY L.Licence, T.InstantId
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
  SELECT DISTINCT P.PointId, P.geom, V.Licence
  FROM Trips T, Vehicles V, Points P
  WHERE T.VehId = V.VehId
  AND ST_Intersects(trajectory(T.Trip), P.geom) 
  ORDER BY P.PointId, V.Licence
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
  SELECT L1.Licence AS Licence1, L2.Licence AS Licence2,
    MIN(ST_Distance(trajectory(T1.Trip), trajectory(T2.Trip))) AS MinDist
  FROM Trips T1, Licences1 L1, Trips T2, Licences2 L2
  WHERE T1.VehId = L1.VehId AND T2.VehId = L2.VehId
  GROUP BY L1.Licence, L2.Licence 
  ORDER BY L1.Licence, L2.Licence
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp1(Licence1, Trajs) AS (
    SELECT L1.Licence, ST_Collect(trajectory(T1.Trip))
    FROM Trips T1, Licences1 L1
    WHERE T1.VehId = L1.VehId
    GROUP BY L1.Licence
  ),
  Temp2(Licence2, Trajs) AS (
    SELECT L2.Licence, ST_Collect(trajectory(T2.Trip))
    FROM Trips T2, Licences2 L2
    WHERE T2.VehId = L2.VehId
    GROUP BY L2.Licence
  )
  SELECT Licence1, Licence2, ST_Distance(T1.Trajs, T2.Trajs) AS MinDist
  FROM Temp1 T1, Temp2 T2  
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
  SELECT DISTINCT V1.Licence AS Licence1, V2.Licence AS Licence2
  FROM Trips T1, Vehicles V1, Trips T2, Vehicles V2
  WHERE T1.VehId = V1.VehId AND T2.VehId = V2.VehId
  AND T1.VehId < T2.VehId AND V1.Type = 'truck' AND V2.Type = 'truck' 
  AND T1.Trip && expandSpatial(T2.Trip, 10) 
  AND tdwithin(T1.Trip, T2.Trip, 10.0) ?= true
  ORDER BY V1.Licence, V2.Licence
  INTO J;
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp(Licence, VehId, Trip) AS (
    SELECT V.Licence, T.VehId, T.Trip
    FROM Trips T, Vehicles V
    WHERE T.VehId = V.VehId 
    AND V.Type = 'truck'
  )
  SELECT T1.Licence, T2.Licence
  FROM Temp T1, Temp T2
  WHERE T1.VehId < T2.VehId 
  AND T1.Trip && expandSpace(T2.Trip, 10) 
  AND tdwithin(T1.Trip, T2.Trip, 10.0) ?= true
  ORDER BY T1.Licence, T2.Licence
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
    SELECT DISTINCT V.Licence, P.PointId, P.geom, 
      MIN(startTimestamp(atValues(T.Trip,P.geom))) AS Instant
    FROM Trips T, Vehicles V, Points P
    WHERE T.VehId = V.VehId AND V.Type = 'passenger'
    AND ST_Intersects(trajectory(T.Trip), P.geom)
    GROUP BY V.Licence, P.PointId, P.geom
  )
  SELECT T1.Licence, T1.PointId, T1.geom, T1.Instant
  FROM Temp T1
  WHERE T1.Instant <= ALL (
    SELECT T2.Instant
    FROM Temp T2
    WHERE T1.PointId = T2.PointId
  )
  ORDER BY T1.PointId, T1.Licence
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
  SELECT L.Licence, P.PeriodId, P.Period,
  SUM(length(atTime(T.Trip, P.Period))) AS Dist
  FROM Trips T, Licences1 L, Periods1 P
  WHERE T.VehId = L.VehId AND T.Trip && P.Period
  GROUP BY L.Licence, P.PeriodId, P.Period 
  ORDER BY L.Licence, P.PeriodId
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
    SELECT P.PeriodId, P.Period, T.VehId,
      SUM(length(atTime(T.Trip, P.Period))) AS Dist
    FROM Trips T, Periods P
    WHERE T.Trip && P.Period
    GROUP BY P.PeriodId, P.Period, T.VehId
  )
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

  -- Query = 'Q10';
  -- StartTime := clock_timestamp();  
  -- Query 10
  /* Slower version of the query where the atValue expression in the WHERE
     clause and the SELECT clauses are executed twice
  SELECT L1.Licence AS Licence1, T2.VehId AS Car2Id,
    whenTrue(tdwithin(T1.Trip, T2.Trip, 3.0)) AS Periods
  FROM Trips T1, Licences1 L1, Trips T2, Vehicles V
  WHERE T1.VehId = L1.VehId AND T2.VehId = V.VehId AND T1.VehId <> T2.VehId
  AND T2.Trip && expandSpace(T1.trip, 3)
  AND whenTrue(tdwithin(T1.Trip, T2.Trip, 3.0)) IS NOT NULL
  */

  -- EXPLAIN (ANALYZE, FORMAT JSON)
  -- WITH Temp AS (
    -- SELECT L1.Licence AS Licence1, T2.VehId AS Car2Id,
    -- whenTrue(tdwithin(T1.Trip, T2.Trip, 3.0)) AS Periods
    -- FROM Trips T1, Licences1 L1, Trips T2, Vehicles V
    -- WHERE T1.VehId = L1.VehId AND T2.VehId = V.VehId AND T1.VehId <> T2.VehId
    -- AND T2.Trip && expandSpace(T1.trip, 3)
  -- )
  -- SELECT Licence1, Car2Id, Periods
  -- FROM Temp
  -- WHERE Periods IS NOT NULL
  -- INTO J;
  
  -- PlanningTime := (J->0->>'Planning Time')::float;
  -- ExecutionTime := (J->0->>'Execution Time')::float/1000;
  -- Duration := make_interval(secs := PlanningTime + ExecutionTime);
  -- NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  -- IF detailed THEN
    -- RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
    -- trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  -- ELSE
    -- RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
  -- END IF;
  -- INSERT INTO execution_tests_explain
  -- VALUES (Experiment_Id, trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows, J);
  -- TotalDuration = TotalDuration + Duration;

  -------------------------------------------------------------------------------
  -- Query 11: Which vehicles passed a point from Points1 at one of the 
  -- instants from Instants1?

  Query = 'Q11';
  StartTime := clock_timestamp();                    
  -- Query 11
  EXPLAIN (ANALYZE, FORMAT JSON)  
  WITH Temp AS (
    SELECT P.PointId, P.geom, I.InstantId, I.Instant, T.VehId
    FROM Trips T, Points1 P, Instants1 I
    WHERE T.Trip @> stbox(P.geom, I.Instant)
    AND valueAtTimestamp(T.Trip, I.Instant) = P.geom
  )
  SELECT T.PointId, T.geom, T.InstantId, T.Instant, V.Licence
  FROM Temp T JOIN Vehicles V ON T.VehId = V.VehId
  ORDER BY T.PointId, T.InstantId, V.Licence
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
    SELECT DISTINCT P.PointId, P.geom, I.InstantId, I.Instant, T.VehId
    FROM Trips T, Points1 P, Instants1 I
    WHERE T.Trip @> stbox(P.geom, I.Instant)
    AND valueAtTimestamp(T.Trip, I.Instant) = P.geom
  )
  SELECT DISTINCT T1.PointId, T1.geom, T1.InstantId, T1.Instant, 
    V1.Licence AS Licence1, V2.Licence AS Licence2
  FROM Temp T1 JOIN Vehicles V1 ON T1.VehId = V1.VehId JOIN
    Temp T2 ON T1.VehId < T2.VehId AND T1.PointID = T2.PointID AND
    T1.InstantId = T2.InstantId JOIN Vehicles V2 ON T2.VehId = V2.VehId
  ORDER BY T1.PointId, T1.InstantId, V1.Licence, V2.Licence
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
  SELECT DISTINCT R.RegionId, P.PeriodId, P.Period, V.Licence
  FROM Trips T, Vehicles V, Regions1 R, Periods1 P
  WHERE T.VehId = V.VehId 
  AND T.trip && stbox(R.geom, P.Period)
  AND _ST_Intersects(trajectory(atTime(T.Trip, P.Period)), R.geom)
  ORDER BY R.RegionId, P.PeriodId, V.Licence
  INTO J;
  */
  -- Modified version
  EXPLAIN (ANALYZE, FORMAT JSON)           
  WITH Temp AS (
    SELECT DISTINCT R.RegionId, P.PeriodId, P.Period, T.VehId
    FROM Trips T, Regions1 R, Periods1 P
    WHERE T.trip && stbox(R.geom, P.Period)
    AND _ST_Intersects(trajectory(atTime(T.Trip, P.Period)), R.geom)
    ORDER BY R.RegionId, P.PeriodId
  )
  SELECT DISTINCT T.RegionId, T.PeriodId, T.Period, V.Licence
  FROM Temp T, Vehicles V
  WHERE T.VehId = V.VehId 
  ORDER BY T.RegionId, T.PeriodId, V.Licence
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
  SELECT DISTINCT R.RegionId, I.InstantId, I.Instant, V.Licence
  FROM Trips T, Vehicles V, Regions1 R, Instants1 I
  WHERE T.VehId = V.VehId 
  AND T.trip && stbox(R.geom, I.Instant)
  AND _ST_Contains(R.geom, valueAtTimestamp(T.Trip, I.Instant))
  ORDER BY R.RegionId, I.InstantId, V.Licence
  INTO J;
  */
  EXPLAIN (ANALYZE, FORMAT JSON)  
  WITH Temp AS (
    SELECT DISTINCT R.RegionId, I.InstantId, I.Instant, T.VehId
    FROM Trips T, Regions1 R, Instants1 I
    WHERE T.Trip && stbox(R.geom, I.Instant)
    AND _ST_Contains(R.geom, valueAtTimestamp(T.Trip, I.Instant))
  )
  SELECT DISTINCT T.RegionId, T.InstantId, T.Instant, V.Licence
  FROM Temp T JOIN Vehicles V ON T.VehId = V.VehId 
  ORDER BY T.RegionId, T.InstantId, V.Licence
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
  SELECT DISTINCT PO.PointId, PO.geom, PR.PeriodId, PR.Period, V.Licence
  FROM Trips T, Vehicles V, Points1 PO, Periods1 PR
  WHERE T.VehId = V.VehId 
  AND T.Trip && stbox(PO.geom, PR.Period)
  AND _ST_Intersects(trajectory(atTime(T.Trip, PR.Period)), PO.geom)
  ORDER BY PO.PointId, PR.PeriodId, V.Licence
  INTO J;
  */

  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Temp AS (
    SELECT DISTINCT PO.PointId, PO.geom, PR.PeriodId, PR.Period, T.VehId
    FROM Trips T, Points1 PO, Periods1 PR
    WHERE T.Trip && stbox(PO.geom, PR.Period)
    AND _ST_Intersects(trajectory(atTime(T.Trip, PR.Period)), PO.geom)      
  )
  SELECT DISTINCT T.PointId, T.geom, T.PeriodId, T.Period, V.Licence  
  FROM Temp T, Vehicles V
  WHERE T.VehId = V.VehId 
  ORDER BY T.PointId, T.PeriodId, V.Licence
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
  SELECT P.PeriodId, P.Period, R.RegionId, 
    L1.Licence AS Licence1, L2.Licence AS Licence2
  FROM Trips T1, Licences1 L1, Trips T2, Licences2 L2, Periods1 P, Regions1 R
  WHERE T1.VehId = L1.VehId AND T2.VehId = L2.VehId AND L1.Licence < L2.Licence
  -- AND T1.Trip && stbox(R.geom, P.Period) AND T2.Trip && stbox(R.geom, P.Period) 
  AND _ST_Intersects(trajectory(atTime(T1.Trip, P.Period)), R.geom)
  AND _ST_Intersects(trajectory(atTime(T2.Trip, P.Period)), R.geom)
  AND tintersects(atTime(T1.Trip, P.Period), atTime(T2.Trip, P.Period)) %= FALSE 
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
    SELECT P.PointId, COUNT(DISTINCT T.VehId) AS Hits
    FROM Trips T, Points P
    WHERE ST_Intersects(trajectory(T.Trip), P.geom)
    GROUP BY P.PointId
  )
  SELECT PointId, Hits
  FROM PointCount AS P
  WHERE P.Hits = ( SELECT MAX(Hits) FROM PointCount )
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
