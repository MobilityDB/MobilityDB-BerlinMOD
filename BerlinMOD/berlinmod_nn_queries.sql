/******************************************************************************
 * Executes the 9 BerlinMOD/NN benchmark queries
 * http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD-FinalReview-2008-06-18.pdf
 * in MobilityDB.
 * Parameters:
 *    notimes: number of times that each query is run. It is set by default
 *       to 5
 *    detailed: states whether detailed statistics are collected during
 *      the execution. By default it is set to TRUE. 
 * Example of usage:
 *     <Create the function>
 *     SELECT berlinmod_NN_queries(1, true)
 * It is supposed that the BerlinMOD data in CSV format has been previously
 * loaded. For loading the data see the companion file 'berlinmod_load.sql'
 *****************************************************************************/
/*
DROP TABLE IF EXISTS execution_tests_explain;
CREATE TABLE execution_tests_explain (
  ExperimentId int,
  Query char(5),
  StartTime timestamp,
  PlanningTime float,
  ExecutionTime float,
  Duration interval,
  NumberRows bigint,
  J json
);
*/

DROP FUNCTION IF EXISTS berlinmod_NN_queries;
CREATE OR REPLACE FUNCTION berlinmod_NN_queries(times integer,
  detailed boolean DEFAULT false) 
RETURNS text AS $$
DECLARE
  Query char(5);
  J json;
  StartTime timestamp;
  PlanningTime float;
  ExecutionTime float;
  Duration interval;
  NumberRows bigint;
  ExperimentId int;
BEGIN
FOR ExperimentId IN 1..times
LOOP
  SET log_error_verbosity to terse;

  -------------------------------------------------------------------------------
  -- Query 18: For each vehicle with a licence plate number from Licences1
  -- and each instant from Instants1: Which are the 10 vehicles that have
  -- been closest to that vehicle at the given instant?

  Query = 'Q18';
  StartTime := clock_timestamp();

  -- Query 18
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT l1.Licence AS Licence1, i.InstantId, v3.RowNo, v3.Licence AS Licence2,
    v3.Dist
  FROM Licences1 l1
  CROSS JOIN Instants1 i
  JOIN Trips t1 ON l1.VehicleId = t1.VehicleId AND getTime(t1.Trip) @> i.Instant
  CROSS JOIN LATERAL (
    SELECT v2.*, ROW_NUMBER() OVER() AS RowNo
    FROM (
    SELECT v.Licence, valueAtTimestamp(t1.Trip, i.Instant) <-> 
      valueAtTimestamp(t2.Trip, i.Instant) AS Dist
    FROM Trips t2, Vehicles v
    WHERE t2.VehicleId = v.VehicleId AND t1.VehicleId < t2.VehicleId
    AND getTime(t2.Trip) @> i.Instant
    ORDER BY valueAtTimestamp(t1.Trip, i.Instant) <-> 
      valueAtTimestamp(t2.Trip, i.Instant)
    LIMIT 3 ) AS v2 ) AS v3
  ORDER BY Licence1, InstantId, RowNo
  INTO J;

  /*
  -- Query 18
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Distances AS (
    -- DISTINCT is necessary since there are duplicates in Licences1
    SELECT DISTINCT l1.Licence AS Licence1, i.InstantId, i.Instant, C2.Licence AS Licence2,
    ST_distance(valueAtTimestamp(t1.Trip, i.Instant),valueAtTimestamp(t2.Trip, i.Instant)) AS Dist
    FROM Trips t1, Licences1 l1, Trips t2, Vehicles C2, Instants1 i
    WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = C2.VehicleId AND t1.VehicleId <> t2.VehicleId
    AND t1.Trip @> i.Instant AND t2.Trip @> i.Instant
  )
  SELECT *
  FROM (SELECT *, RANK() OVER (PARTITION BY Licence1, InstantId ORDER BY Dist) AS Rank
  FROM Distances) AS Tmp
  WHERE Rank <= 10
  ORDER BY Licence1, InstantId, Rank
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 19: For each vehicle with a licence from Licences1 and each
  -- period from Periods1: Which points from Points have been the
  -- 3 closest to that vehicle during that period?

  Query = 'Q19';
  StartTime := clock_timestamp();

  -- Query 19
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT l1.Licence AS Licence1, pr.PeriodId, pt2.RowNo, pt2.PointId, pt2.Dist
  FROM Trips t JOIN Licences1 l1 ON t.VehicleId = l1.VehicleId
  JOIN Periods1 pr ON t.Trip && pr.Period
  CROSS JOIN LATERAL (
    SELECT pt1.*, ROW_NUMBER() OVER () AS RowNo
    FROM ( SELECT pt.PointId, pt.Geom, 
      trajectory(atTime(t.Trip, pr.Period)) <-> pt.Geom AS Dist
    FROM Points pt
    ORDER BY trajectory(atTime(t.Trip, pr.Period)) <-> pt.Geom
    LIMIT 3
  ) AS pt1 ) AS pt2
  ORDER BY l1.Licence, pr.PeriodId, pt2.Dist
  INTO J;

  /*
  -- Query 19
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH DistanceVehPoint AS (
    -- Distance between all trajectories of a car (restricted to a period) and a point
    -- DISTINCT is necessary since there are duplicates in Licences1
    SELECT DISTINCT l.Licence, pr.PeriodId, pr.Period, pt.PointId, pt.geom,
    MIN(ST_Distance(trajectory(atTime(t.Trip, pr.Period)), pt.geom)) AS Dist
    FROM Trips t, Licences1 l, Periods1 pr, Points pt
    WHERE t.VehicleId = l.VehicleId AND t.Trip && pr.Period
    GROUP BY l.licence, pr.PeriodId, pr.Period, pt.PointId, pt.geom
    -- 10,000 rows after 36 seconds =
    -- card(Licences1) = 10 * card(Periods1) = 10 * card(Points) = 100
  )
  SELECT *
  FROM ( SELECT *, RANK() OVER (PARTITION BY Licence, PeriodId ORDER BY Dist) AS Rank
  FROM DistanceVehPoint ) AS Tmp
  WHERE Rank <= 3
  ORDER BY Licence, PeriodId, Rank
  -- 307 rows (instead of 300) after 1:18 minutes: Some cars crossed more than 3 points
  -- during a given period and thus they have more than 3 closest points with
  -- rank 1 and distance 0
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 20: For each region from Regions1 and period from Periods1:
  -- What are the licences of the 10 vehicles that are closest to that region
  -- during the given observation period?

  Query = 'Q20';
  StartTime := clock_timestamp();

  -- Query 20
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT r.RegionId, p.PeriodId, v2.RowNo, v2.Licence, v2.Dist
  FROM Regions1 r CROSS JOIN Periods1 p
  CROSS JOIN LATERAL (
    SELECT v1.*, ROW_NUMBER() OVER () AS RowNo FROM (
    SELECT v.Licence, trajectory(atTime(t.trip, p.Period)) <-> r.geom AS Dist
    FROM Trips t, Vehicles v
    WHERE t.VehicleId = v.VehicleId AND t.Trip && p.Period
    ORDER BY trajectory(atTime(t.trip, p.Period)) <-> r.geom
    LIMIT 3 ) AS v1 ) AS v2
  ORDER BY r.RegionId, p.PeriodId, v2.RowNo
  INTO J;

  /*
  -- Query 20
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH DistanceRegionVeh AS (
    SELECT r.RegionId, p.PeriodId, p.Period, v.Licence,
    MIN(ST_Distance(trajectory(atTime(t.Trip, p.Period)), r.geom)) AS Dist
    FROM Regions1 r, Periods1 p, Trips t, Vehicles v
    WHERE t.VehicleId = v.VehicleId AND t.Trip && p.Period
    GROUP BY r.RegionId, p.PeriodId, p.Period, v.Licence
    -- 14,100 rows in 77 seconds
  )
  SELECT *
  FROM (
    SELECT *, RANK() OVER (PARTITION BY RegionId, PeriodId ORDER BY Dist) AS Rank
    FROM DistanceRegionVeh ) AS Tmp
  WHERE Rank <= 10
  ORDER BY RegionId, PeriodId, Rank
  -- 1139 rows on 77 seconds
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 21: For each vehicle with a licence plate number from Licences1
  -- and each period from Periods1: Which are the 10 vehicles that are
  -- closest to that vehicle at all times during that period?

  Query = 'Q21';
  StartTime := clock_timestamp();

  -- Query 21
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT l1.Licence AS Licence1, p.PeriodId, L3.RowNo, L3.Licence AS Licence2, 
    L3.Dist
  FROM Licences1 l1
  CROSS JOIN Periods1 p
  CROSS JOIN LATERAL (
    SELECT L2.*, ROW_NUMBER() OVER () AS RowNo FROM (
    SELECT v.Licence, trajectory(atTime(t1.Trip, getTime(t2.trip) + p.Period)) <->
      trajectory(atTime(t2.Trip, getTime(t1.trip) + p.Period)) AS Dist
    FROM Trips t1, Trips t2, Vehicles v
    WHERE t1.VehicleId = l1.VehicleId AND t1.trip && p.Period
    AND t2.VehicleId = v.VehicleId AND t2.trip && p.Period
    AND getTime(t1.trip) && getTime(t2.trip)
    ORDER BY trajectory(atTime(t1.Trip, getTime(t2.trip) + p.Period)) <->
      trajectory(atTime(t2.Trip, getTime(t1.trip) + p.Period))
    LIMIT 3 ) AS L2 ) AS L3
  ORDER BY l1.Licence, p.PeriodId, L3.RowNo, L3.Licence
  INTO J;

  /*
  -- Query 21: 25 minutes
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH DistanceVehVeh AS (
    SELECT l1.Licence AS Licence1, p.PeriodId, p.Period, C2.Licence AS Licence2,
    ST_Distance(trajectory(atTime(t1.Trip, p.Period)), trajectory(atTime(t2.Trip, p.Period))) AS Dist
    -- minValue(distance(atTime(t1.Trip, p.Period),atTime(t2.Trip, p.Period))) AS Dist
    FROM Trips t1, Licences1 l1, Trips t2, Vehicles C2, Periods1 p
    WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = C2.VehicleId AND t1.VehicleId <> t2.VehicleId
    AND t1.Trip && p.Period AND t2.Trip && p.Period
    -- LIMIT 100
  )
  SELECT *
  FROM (
    SELECT *, RANK() OVER (PARTITION BY Licence1, PeriodId ORDER BY Dist) AS Rank
    FROM DistanceVehVeh ) AS Tmp
  WHERE Rank <= 10
  ORDER BY Licence1, PeriodId, Rank
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 22: For each vehicle with a licence from Licences1 give the point
  -- from Points1 that was the nearest neighbour of the vehicle and the
  -- interval during which this was the case.

  Query = 'Q22';
  StartTime := clock_timestamp();

  /*
  -- Query 22
  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT l.Licence, l.VehicleId, p.PointId, t.VehicleId, t.Dist
  -- Given a car l and a point p
  FROM Licences1 l CROSS JOIN Points p
  JOIN LATERAL (
    -- Find the car t which is the closest to p
    SELECT VehicleId, trajectory(Trip) <-> p.Geom AS Dist
    FROM Trips
    ORDER BY trajectory(Trip) <-> p.Geom
    LIMIT 1
    ) AS t
    -- Verify that the closest car t is equal to l
    ON l.VehicleId = t.VehicleId
  ORDER BY l.Licence, p.PointId
  INTO J;
  */
  
  -- Query 22
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH DistancePointVeh AS (
  -- Minimum distance between a point and all trajectories of a car
    SELECT p.PointId, p.geom, l.Licence, tmin(p.geom <-> t.Trip) AS TempMin
    FROM Points1 p, Trips t, Licences1 l
    WHERE t.VehicleId = l.VehicleId
    GROUP BY p.PointId, p.geom, Licence
  ),
  MinDistancePoint AS (
    -- Minimum distance between all trajectories of a car and all points
    SELECT Licence, MIN(minValue(TempMin)) AS MinDist
    FROM DistancePointVeh
    GROUP BY Licence
  ),
  VehRNNPoint AS (
    -- Intervals during which a point was the closest one to a car
    SELECT p.Licence, pv.PointId,
    getTime(atValues(p.MinDist #= pv.TempMin, True)) AS timeinterval
    FROM MinDistancePoint p, DistancePointVeh pv
    WHERE p.Licence = pv.Licence -- AND getTime(p.MinDist) && getTime(pv.TempMin)
    AND atValues(p.MinDist #= pv.TempMin, True) IS NOT NULL
  )
  SELECT *
  FROM VehRNNPoint
  ORDER BY Licence, timeinterval
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 23 For each point from Points1 and period from Periods1, give the
  -- licences of the vehicles, having that point as the nearest point,
  -- and the temporal intervals, for that these relations hold during the given
  -- period.

  Query = 'Q23';
  StartTime := clock_timestamp();

  -- Query 23
  EXPLAIN (ANALYZE, FORMAT JSON)
  -- DISTINCT is needed to remove duplicate licences
  -- associated with different TripId
  SELECT DISTINCT pt.PointId, pr.PeriodId, v.Licence
  -- Given a point pt and a period pr
  FROM Points1 pt CROSS JOIN Periods1 pr
  CROSS JOIN LATERAL (
    -- Project the trips t to the period pr. Notice that the
    -- same VehicleId can appear with various different TripIds
    SELECT VehicleId, TripId, atTime(Trip, pr.Period) AS Trip
    FROM Trips
    WHERE Trip && pr.Period ) AS t
  JOIN LATERAL (
    -- Find the point p3 that is closest to t
    SELECT p2.PointID
    FROM Points1 p2
    ORDER BY trajectory(t.Trip) <-> p2.Geom
    LIMIT 1
    ) AS p3
    -- Verify that the closest point p3 is equal to pt
    ON pt.PointId = p3.PointId
  -- Find the Licence of t
  JOIN Vehicles v ON t.VehicleId = v.VehicleId
  ORDER BY pt.PointId, pr.PeriodId, v.Licence
  INTO J;

  /* 
  -- Query 23 Alternative
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH TempMinDistPointPerVeh AS (
  -- Minimum temporal distance between a point and all trajectories of a car
  -- projected to a period
    SELECT pt.PointId, pr.PeriodId, t.VehicleId,
      tmin(pt.geom <-> atTime(t.Trip, pr.period)) AS TempMin
    FROM Points1 pt, Periods1 pr, Trips t
    GROUP BY pt.PointId, pr.PeriodId, t.VehicleId
  ),
  MinDistPointPerVeh AS (
    -- Minimum distance between a point and all trajectories of a car projected
    -- to a period
    SELECT PointId, PeriodId, VehicleId, MIN(minValue(TempMin)) AS Dist
    FROM TempMinDistPointPerVeh
    GROUP BY PointId, PeriodId, VehicleId
  ),
  MinDistPerVeh AS (
    -- Minimum distance between all trajectories of a car projected to a period
    -- and all points
    SELECT PeriodId, VehicleId, MIN(Dist) AS MinDist
    FROM MinDistPointPerVeh
    GROUP BY PeriodId, VehicleId
  ),
  PointRNNVeh AS (
    -- Intervals during which a car has a point as the closest one during a period
    SELECT t.VehicleId, t.PeriodId, t.PointId,
    getTime(atValues(m.MinDist #= t.TempMin, True)) AS TimeInterval
    FROM TempMinDistPointPerVeh t, MinDistPerVeh m
    WHERE t.VehicleId = m.VehicleId AND t.PeriodId = m.PeriodId
    -- AND getTime(t.MinDist) && getTime(t.TempMin)
    AND atValues(m.MinDist #= t.TempMin, True) IS NOT NULL
  )
  SELECT Licence, PeriodId, PointId, TimeInterval
  FROM PointRNNVeh p, Vehicles v
  WHERE p.VehicleId = v.VehicleId
  ORDER BY Licence, TimeInterval
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 24: For each vehicle with a licence from Licences1 and each
  -- period from Periods1, report the licences of vehicles, having the
  -- given vehicle as the nearest vehicle and the time intervals during which
  -- this was the case.

  Query = 'Q24';
  StartTime := clock_timestamp();

  -- Query 24
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH ProjTrips AS (
    SELECT PeriodId, VehicleId, TripId, atTime(Trip, Period) AS Trip
    FROM Periods1, Trips
    -- Use the temporal index if any
    WHERE Trip && Period
  )
  SELECT DISTINCT l.Licence AS Licence1, p.PeriodId, v.Licence AS Licence2
  -- Given a car l and a period p
  FROM Licences1 l CROSS JOIN Periods1 p
  CROSS JOIN LATERAL (
    -- Find trips of cars t1 distinct from l during the period p.
    SELECT VehicleId, TripId, Trip FROM ProjTrips
    WHERE VehicleId != l.VehicleId AND PeriodId = p.PeriodId ) AS t1
  JOIN LATERAL (
    -- Find car t2 which is the closest car to t1 during the period.
    SELECT VehicleId AS ClosestOjbId FROM ProjTrips
    WHERE VehicleId != t1.VehicleId AND PeriodId = p.PeriodId
    ORDER BY trajectory(t1.Trip) <-> trajectory(Trip)
    LIMIT 1 ) AS t2
    -- Verify that the closest car t2 is equal to l
    ON l.VehicleId = t2.ClosestOjbId
  -- Find the Licence of t1
  JOIN Vehicles v ON t1.VehicleId = v.VehicleId
  ORDER BY l.Licence, p.PeriodId, v.Licence
  INTO J;

  /*
  -- Query 24
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH DistTraj1Traj2 AS (
    -- Distance between the trajectories of two cars (projected to a period)
    SELECT t1.TripId AS TripId1, l1.Licence AS Licence1,
    t2.TripId AS TripId2, L2.Licence AS Licence2, pr.PeriodId,
    distance(atTime(t1.Trip, pr.Period), atTime(t2.Trip, pr.Period)) AS Dist
    FROM Trips t1, Licences1 l1, Trips t2, Licences1 L2, Periods1 pr
    WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = L2.VehicleId AND t1.VehicleId <> t2.VehicleId
    AND t1.Trip && pr.Period AND t2.Trip && pr.Period
  ),
  DistanceVeh1Veh2 AS (
    -- Minimum distance between all trajectories of two cars (projected to a period)
    SELECT Licence1, Licence2, PeriodId, UNNEST(MIN(Dist)) AS Dist
    FROM DistTraj1Traj2
    GROUP BY Licence1, Licence2, PeriodId
  ),
  MinDistanceVeh AS (
    -- Minimum distance between all trajectories of a car (projected to a period)
    -- and all other cars
    SELECT Licence1, PeriodId, UNNEST(MIN(Dist)) AS MinDist
    FROM DistanceVeh1Veh2
    GROUP BY Licence1, PeriodId
  ),
  Veh1RNNVeh2 AS (
    -- Intervals during which a car has another car as the closest one during a period
    SELECT v.Licence1, v.PeriodId, cc.Licence2,
    getTime(unnest(atValues(v.MinDist #= cc.Dist, True))) AS TimeInterval
    FROM MinDistanceVeh v, DistanceVeh1Veh2 cc
    WHERE v.Licence1 = cc.Licence1 AND v.PeriodId = cc.PeriodId
    -- AND getTime(v.MinDist) && getTime(cc.Dist)
    AND atValues(v.MinDist #= cc.Dist, True) IS NOT NULL
  )
  SELECT *
  FROM Veh1RNNVeh2
  ORDER BY Licence1, TimeInterval
  INTO J;
  */

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 25 For each group of ten vehicles having ten disjoint consecutive
  -- licences from Licences and each period from Periods1: Report the
  -- point(s) from Points, having the minimum aggregated distance from the
  -- given group of ten vehicles during the given period.

  Query = 'Q25';
  StartTime := clock_timestamp();

  -- Query 25
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Licences AS (
    SELECT DISTINCT(Licence)
    FROM Licences
  ),
  Groups AS (
    SELECT l.Licence, v.VehicleId, ((row_number() OVER (ORDER BY l.Licence))-1)/10 + 1 AS GroupId
    FROM Licences l, Vehicles v
    WHERE l.Licence = v.Licence
  ),
  SumDistances AS (
    SELECT g.GroupId, pr.PeriodId, pt.PointId,
    SUM(ST_Distance(trajectory(atTime(t.Trip, pr.Period)), pt.geom)) AS SumDist
    -- SUM(distance(atTime(t.Trip, pr.Period), pt.geom)) AS SumDist
    FROM Groups g, Periods1 pr, Points1 pt, Trips t
    WHERE t.VehicleId = g.VehicleId AND t.Trip && pr.Period
    GROUP BY g.GroupId, pr.PeriodId, pt.PointId
  )
  SELECT s1.GroupId, s1.PeriodId, s1.PointId, s1.SumDist
  FROM SumDistances s1
  WHERE s1.SumDist <= ALL (
    SELECT SumDist
    FROM SumDistances s2
    WHERE s1.GroupId = s2.GroupId AND s1.PeriodId = s2.PeriodId )
  ORDER BY s1.GroupId, s1.PeriodId, s1.PointId
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query),
      Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  -------------------------------------------------------------------------------
  -- Query 26 Create the ten groups of vehicles having ten disjoint consecutive
  -- licences from Licences.
  -- For each pair of such groups and each period from Periods1: Report the licence(s) of the
  -- vehicle(s) within the given first group of ten vehicles, having the minimum aggregated distance
  -- from the given other group of ten vehicles during the given period.

  Query = 'Q26';
  StartTime := clock_timestamp();

  -- Query 26
  EXPLAIN (ANALYZE, FORMAT JSON)
  WITH Licences AS (
    SELECT DISTINCT(Licence)
    FROM Licences
  ),
  Groups AS (
    SELECT l.Licence, v.VehicleId, 
      ((ROW_NUMBER() OVER (ORDER BY l.Licence))-1)/10 + 1 AS GroupId
    FROM Licences l, Vehicles v
    WHERE l.Licence = v.Licence
  ),
  Pairs AS (
    SELECT DISTINCT g1.GroupId AS GroupId1, g2.GroupId AS GroupId2
    FROM Groups g1, Groups g2
    WHERE g1.GroupId <> g2.GroupId
  ),
  Distances AS (
    SELECT pa.GroupId1, pa.GroupId2, pr.PeriodId, pr.Period,
      g1.Licence AS Licence, g2.Licence AS OtherLicence,
    -- Minimum distance among all trajectories of two cars
    MIN(ST_Distance(trajectory(atTime(t1.Trip, pr.Period)), 
      trajectory(atTime(t2.Trip, pr.Period)))) AS MinDist
    -- MIN(minValue(distance(atTime(t1.Trip, pr.Period), atTime(t2.Trip, pr.Period)))) AS MinDist
    FROM Pairs pa, Groups g1, Groups g2, Periods1 pr, Trips t1, Trips t2
    WHERE pa.GroupId1 = g1.GroupId AND pa.GroupId2 = g2.GroupId
    AND t1.VehicleId = g1.VehicleId AND t2.VehicleId = g2.VehicleId AND 
      t1.VehicleId <> t2.VehicleId AND t1.Trip && pr.Period AND t2.Trip && pr.Period
    GROUP BY pa.GroupId1, pa.GroupId2, pr.PeriodId, pr.Period, g1.Licence, 
      g2.Licence
  ),
  AggDistances AS (
    SELECT GroupId1, GroupId2, PeriodId, Period, Licence, SUM(MinDist) AS AggDist
    FROM Distances
    GROUP BY GroupId1, GroupId2, PeriodId, Period, Licence
  )
  SELECT d1.GroupId1, d1.GroupId2, d1.PeriodId, d1.Period, d1.Licence, d1.AggDist
  FROM AggDistances d1
  WHERE d1.AggDist <= ALL (
    SELECT d2.AggDist
    FROM AggDistances d2
    WHERE d1.GroupId1 = d2.GroupId1 AND d1.GroupId2 = d2.GroupId2
    AND d1.PeriodId = d2.PeriodId )
  ORDER BY d1.GroupId1, d1.GroupId2, d1.PeriodId, d1.Licence
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float/1000;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, '
      'Execution Time: % secs, Total Duration: %, Number of Rows: %',
      trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', 
      trim(Query), Duration, NumberRows;
  END IF;
  INSERT INTO execution_tests_explain
  VALUES (ExperimentId, trim(Query), StartTime, PlanningTime, ExecutionTime,
    Duration, NumberRows, J);

  /*
  -- There are several trajectories of the same object in a period
  SELECT pr.PeriodId, VehicleId, array_agg(TripId order by TripId)
  FROM Periods1 pr, Trips t1
  WHERE atTime(t1.Trip, pr.Period) IS NOT NULL
  GROUP BY pr.PeriodId, VehicleId
  ORDER BY pr.PeriodId, VehicleId;
  */
-------------------------------------------------------------------------------

END LOOP;

RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
