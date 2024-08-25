/*-----------------------------------------------------------------------------
-- BerlinMOD Data Generator
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright (C) 2024, Esteban Zimanyi, Mahmoud Sakr,
  Universite Libre de Bruxelles.

The functions defined in this file use MobilityDB to generate data
similar to the data used in the BerlinMOD benchmark as defined in
http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD-FinalReview-2008-06-18.pdf

You can change parameters in the various functions of this file.
Usually, changing the master parameter 'P_SCALE_FACTOR' should do it.
But you also might be interested in changing parameters for the
random number generator, experiment with non-standard scaling
patterns or modify the sampling of positions.

The database must contain the following input relations.

*  Nodes(id bigint primary key, geom geometry(Point))
*  RoadSegments(SegmentId bigint primary key, name text, tag_id int, 
    sourceNode bigint, targetNode bigint, SegmentLength float, 
    cost_s float, reverse_cost_s float, MaxSpeedFwd float,
    MaxSpeedBwd float, priority float, SegmentGeo geometry(Linestring))
    sourceNode and targetNode references Nodes(id)
  The Nodes and RoadSegments tables define the road network graph.
  These tables are typically obtained by osm2pgrouting from OSM data.
  The minimum number of attributes these tables should contain are
  those defined above. The OSM tag 'highway' defines several of
  the attributes and this is stated in the configuration file
  for osm2pgrouting which looks as follows:
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <tag_name name="highway" id="1">
      <tag_value name="motorway" id="101" priority="1.0" maxspeed="120" />
        [...]
      <tag_value name="services" id="116" priority="4" maxspeed="20" />
      </tag_name>
    </configuration>
  It is supposed that the RoadSegments and the Nodes table define a connected
  graph, that is, there is a path between every pair of nodes in the graph.
  IF THIS CONDITION IS NOT SATISFIED THE GENERATION WILL FAIL.
  Indeed, in that case pgRouting will return a NULL value when looking
  for a path between two nodes.

The generated data is saved into the database in which the
functions are executed using the following tables

*  Vehicles(vehicleId int primary key, licence text, vehType text, model text)
*  Licences(licenceId int primary key, licence text, vehicleId int)
*  Trips(tripId serial primary key, vehicleId int, startDate date, seqNo int,
     trip tgeompoint, trajectory geometry);
*  LeisureTrips(vehicleId int, startDate date, tripNo int, seqNo int, sourceNode bigint,
    targetNode bigint)
    primary key (vehicleId, startDate, tripNo, seqNo)
    tripNo is 1 for morning/evening trip and is 2 for afternoon trip
    seqNo is the sequence of trips composing a leisure trip
*  Points(pointId int, geom geometry)
*  Regions(regionId int, geom geometry)
*  Instants(instantId int, instant timestamptz)
*  Periods(periodId int, period)

In addition the following working tables are created

*  HomeRegions(id int primary key, priority int, weight int, prob float,
    cumProb float, geom geometry)
*  WorkRegions(id int primary key, priority int, weight int, prob float,
    cumProb float, geom geometry)
    priority indicates the region selection priority
    weight is the relative weight to choose from the given region
    geom is a (Multi)Polygon describing the region's area
*  HomeNodes(id bigint primary key, osm_id bigint, geom geometry, region int)
*  WorkNodes(id bigint primary key, osm_id bigint, geom geometry, region int)
*  VehicleNodes(vehicleId int primary key, home bigint, work bigint, noNeighbours int);
*  Neighbourhoods(vehicleId int, seqNo int, node bigint)
    primary key (vehicleId, seqNo)
*  Destinations(vehicleId int, sourceNode bigint, targetNode bigint)
    primary key (vehicleId, sourceNode, targetNode)
*  Paths(vehicleId int, start_vid bigint, end_vid bigint, seqNo int,
    node bigint, edge bigint, geom geometry, speed float, category int);

-----------------------------------------------------------------------------*/

-------------------------------------------------------------------------------
-- Functions generating random numbers according to various
-- probability distributions. Inspired from
-- https://stackoverflow.com/questions/9431914/gaussian-random-distribution-in-postgresql
-- https://bugfactory.io/blog/generating-random-numbers-according-to-a-continuous-probability-distribution-with-postgresql/
-------------------------------------------------------------------------------

-- Random integer in a range with uniform distribution

CREATE OR REPLACE FUNCTION random_int(low int, high int)
  RETURNS int AS $$
BEGIN
  RETURN floor(random() * (high - low + 1) + low);
END;
$$ LANGUAGE plpgsql STRICT;

/*
select random_int(1,7), count(*)
from generate_series(1, 1e3)
group by 1
order by 1
*/

-- Random integer with binomial distribution

CREATE OR REPLACE FUNCTION random_binomial(n int, p float)
RETURNS int AS $$
DECLARE
  -- Loop variable
  i int;
  -- Result of the function
  result float = 0;
BEGIN
  IF n <= 0 OR p <= 0.0 OR p >= 1.0 THEN
    RETURN NULL;
  END IF;
  FOR i IN 1..n LOOP
    IF random() < p THEN
      result = result + 1;
    END IF;
  END LOOP;
  RETURN result;
END;
$$ LANGUAGE plpgsql STRICT;

/*
with data as (
  select random_binomial(100,0.5) AS r from generate_series(1,1e5) t
)
select min(r), max(r), avg(r)
from data;
-- Successfully run. Total query runtime: 40 secs 876 msec.
*/

-- Random float with exponential distribution

CREATE OR REPLACE FUNCTION random_exp(lambda float DEFAULT 1.0)
RETURNS float AS $$
DECLARE
  -- Random value
  r float;
BEGIN
  IF lambda = 0.0 THEN
    RETURN NULL;
  END IF;
  LOOP
    r = random();
    EXIT WHEN r <> 0.0;
  END LOOP;
  RETURN -1 * ln(r) * lambda;
END;
$$ LANGUAGE plpgsql STRICT;

/*
with data as (
  select random_exp(1) AS r from generate_series(1,1e5) t
)
select min(r), max(r), avg(r)
from data;
-- Successfully run. Total query runtime: 6 min 18 secs.
*/

-- Random float with Gaussian distribution

CREATE OR REPLACE FUNCTION random_gauss(avg float = 0, stddev float = 1)
RETURNS float AS $$
DECLARE
  x1 real; x2 real; w real;
BEGIN
  LOOP
    x1 = 2.0 * random() - 1.0;
    x2 = 2.0 * random() - 1.0;
    w = x1 * x1 + x2 * x2;
    EXIT WHEN w < 1.0;
  END LOOP;
  RETURN avg + x1 * sqrt(-2.0*ln(w)/w) * stddev;
END;
$$ LANGUAGE plpgsql STRICT;

/*
with data as (
  select t, random_gauss(100,15)::int score from generate_series(1,1000000) t
)
select score, sum(1), repeat('=',sum(1)::int/500) bar
from data
where score between 60 and 140
group by score
order by 1;
*/

-- Random float with a Gaussian distributed value within [Low, High]

CREATE OR REPLACE FUNCTION random_boundedgauss(low float, high float,
  avg float = 0, stddev float = 1)
RETURNS float AS $$
DECLARE
  -- Result of the function
  result real;
BEGIN
  result = random_gauss(avg, stddev);
  IF result < low THEN
    RETURN low;
  ELSEIF result > high THEN
    RETURN high;
  ELSE
    RETURN result;
  END IF;
END;
$$ LANGUAGE plpgsql STRICT;

/*
select random_boundedgauss(-0.5, 0.5)
from generate_series(1, 1e2)
order by 1
*/

-------------------------------------------------------------------------------

-- Creates a random duration of length [0ms, 2h] using Gaussian
-- distribution

CREATE OR REPLACE FUNCTION createPause()
RETURNS interval AS $$
BEGIN
  RETURN (((random_boundedgauss(-6.0, 6.0, 0.0, 1.4) * 100.0) + 600.0) * 6000.0)::int * interval '1 ms';
END;
$$ LANGUAGE plpgsql STRICT;

/*
with test(t) as (
select CreatePause()
from generate_series(1, 1e5)
order by 1
)
select min(t), max(t) from test
*/

-- Creates a random non-zero duration of length [2ms, N min - 4ms]
-- using a uniform distribution

CREATE OR REPLACE FUNCTION createPauseN(Minutes int)
  RETURNS interval AS $$
BEGIN
  RETURN ( 2 + random_int(1, Minutes * 60000 - 6) ) * interval '1 ms';
END;
$$ LANGUAGE plpgsql STRICT;

/*
with test(t) as (
select CreatePauseN(1)
from generate_series(1, 1e5)
order by 1
)
select min(t), max(t) from test
*/

-- Creates a normally distributed duration within [-Rhours h, +Rhours h]

CREATE OR REPLACE FUNCTION createDurationRhoursNormal(Rhours float)
  RETURNS interval AS $$
DECLARE
  -- Result of the function
  result interval;
BEGIN
  result = ((random_gauss() * Rhours * 1800000) / 86400000) * interval '1 d';
  IF result > (Rhours / 24.0 ) * interval '1 d' THEN
    result = (Rhours / 24.0) * interval '1 d';
  ELSEIF result < (Rhours / -24.0 ) * interval '1 d' THEN
    result = (Rhours / -24.0) * interval '1 d';
  END IF;
  RETURN result;
END
$$ LANGUAGE plpgsql STRICT;

/*
with test(t) as (
select CreateDurationRhoursNormal(12)
from generate_series(1, 1e5)
order by 1
)
select min(t), max(t) from test
*/

-------------------------------------------------------------------------------

-- Maps an OSM road type as defined in the tag 'highway' to one of
-- the three categories from BerlinMOD: freeway (1), main street (2),
-- side street (3)

/*
  'motorway' id='101' priority='1.0' maxspeed='120' category='1'
  'motorway_link' id='102' priority='1.0' maxspeed='120' category='1'
  'motorway_junction' id='103' priority='1.0' maxspeed='120' category='1'
  'trunk' id='104' priority='1.05' maxspeed='120' category='1'
  'trunk_link' id='105' priority='1.05' maxspeed='120' category='1'
  'primary' id='106' priority='1.15' maxspeed='90' category='2'
  'primary_link' id='107' priority='1.15' maxspeed='90' category='1'
  'secondary' id='108' priority='1.5' maxspeed='70' category='2'
  'secondary_link' id='109' priority='1.5' maxspeed='70' category='2'
  'tertiary' id='110' priority='1.75' maxspeed='50' category='2'
  'tertiary_link' id='111' priority='1.75' maxspeed='50' category='2'
  'residential' id='112' priority='2.5' maxspeed='30' category='3'
  'living_street' id='113' priority='3' maxspeed='20' category='3'
  'unclassified' id='114' priority='3' maxspeed='20' category='3'
  'service' id='115' priority='4' maxspeed='20' category='3'
  'services' id='116' priority='4' maxspeed='20' category='3'
*/

CREATE OR REPLACE FUNCTION berlinmod_roadCategory(tagId int)
RETURNS int AS $$
BEGIN
  RETURN CASE
  -- motorway, motorway_link, motorway_junction, trunk, trunk_link
  WHEN tagId BETWEEN 101 AND 105 THEN 1 -- i.e., "freeway"
  -- primary, primary_link, secondary, secondary_link, tertiary, tertiary_link
  WHEN tagId BETWEEN 106 AND 111 THEN 2 -- i.e., "main street"
  -- residential, living_street, unclassified, service, services
  ELSE 3 -- i.e., "side street"
  END;
END;
$$ LANGUAGE plpgsql STRICT;

-- Type combining the elements needed to define a path between source and
-- target nodes in the graph

DROP TYPE IF EXISTS step CASCADE;
CREATE TYPE step as (linestring geometry, maxspeed float, category int);

-- Call pgrouting to find a path between source and target nodes.
-- A path is composed of an array of steps (see the above type definition).
-- The last argument corresponds to the parameter P_PATH_MODE.
-- This function is currently not used in the generation but is useful
-- for debugging purposes.

CREATE OR REPLACE FUNCTION createPath(sourceN bigint, targetN bigint, pathMode text)
RETURNS step[] AS $$
DECLARE
  -- Query sent to pgrouting depending on the argument pathMode
  query_pgr text;
  -- Result of the function
  result step[];
BEGIN
  IF pathMode = 'Fastest Path' THEN
    query_pgr = 'SELECT id, sourceNode, targetNode, cost_s AS cost, reverse_cost_s as reverse_cost FROM RoadSegments';
  ELSE
    query_pgr = 'SELECT id, sourceNode, targetNode, length_m AS cost, '
      'length_m * sign(reverse_cost_s) as reverse_cost FROM RoadSegments';
  END IF;
  WITH Temp1 AS (
    SELECT P.seqNo, P.node, P.edge
    FROM pgr_dijkstra(query_pgr, sourceN, targetN, true) P
  ),
  Temp2 AS (
    SELECT T.seqNo,
      -- adjusting directionality
      CASE
        WHEN T.node = E.sourceNode THEN E.geom
        ELSE ST_Reverse(geom)
      END AS geom,
      maxspeed_forward AS maxSpeed, berlinmod_roadCategory(tag_id) AS category
    FROM Temp1 T, RoadSegments E
    WHERE edge IS NOT NULL AND E.id = T.edge
  )
  SELECT array_agg((geom, maxSpeed, category)::step ORDER BY seqNo) INTO result
  FROM Temp2;
  RETURN result;
END;
$$ LANGUAGE plpgsql STRICT;

/*
select createPath(9598, 4010, 'Fastest Path')
*/

-- Creates a trip following a path between a source and a target node starting
-- at a timestamp t. Implements Algorithm 1 in BerlinMOD Technical Report.
-- The last argument corresponds to the parameter P_DISTURB_DATA.

CREATE OR REPLACE FUNCTION createTrip(edges step[], startTime timestamptz,
  disturbData boolean, messages text)
RETURNS tgeompoint AS $$
DECLARE
  -------------------------
  -- CONSTANT PARAMETERS --
  -------------------------
  -- Speed in km/h which is considered as a stop and thus only an
  -- accelaration event can be applied
  P_EPSILON_SPEED float = 1;
  -- Used for determining whether the distance is almost equal to 0.0
  P_EPSILON float = 0.0001;

  -- The probability of an event is proportional to (P_EVENT_C)/Vmax.
  -- The probability for an event being a forced stop is given by
  -- 0.0 <= 'P_EVENT_P' <= 1.0 (the balance, 1-P, is meant to trigger
  -- deceleration events).
  P_EVENT_C float = 1.0;
  P_EVENT_P float = 0.1;

  -- Sampling distance in meters at which an acceleration/deceleration/stop
  -- event may be generated.
  P_EVENT_LENGTH float = 5.0;
  -- Speed in Km/h that is added to the current speed in an acceleration event
  P_EVENT_ACC float = 12.0;

  -- Probabilities for forced stops at crossings by road type transition
  -- defined by a matrix where lines and columns are ordered by
  -- side road (S), main road (M), freeway (F). The OSM highway types must be
  -- mapped to one of these categories using the function berlinmod_roadCategory
  P_DEST_STOPPROB float[] =
    '{{0.33, 0.66, 1.00}, {0.33, 0.50, 0.66}, {0.10, 0.33, 0.05}}';
  -- Mean waiting time in seconds using an exponential distribution.
  -- Increasing/decreasing this parameter allows us to slow down or speed up
  -- the trips. Could be think of as a measure of network congestion.
  -- Given a specific path, fine-tuning this parameter enable us to obtain
  -- an average travel time for this path that is the same as the expected
  -- travel time computed, e.g., by Google Maps.
  P_DEST_EXPMU float = 1.0;
  -- Parameters for measuring errors (only required for P_DISTURB_DATA = TRUE)
  -- Maximum total deviation from the real position (default = 100.0)
  -- and maximum deviation per step (default = 1.0) both in meters.
  P_GPS_TOTALMAXERR float = 100.0;
  P_GPS_STEPMAXERR float = 1.0;

  ---------------
  -- Variables --
  ---------------
  -- SRID of the geometries being manipulated
  srid int;
  -- Number of edges in a path, number of segments in an edge,
  -- number of fractions of size P_EVENT_LENGTH in a segment
  noEdges int; noSegs int; noFracs int;
  -- Loop variables
  edge int; seg int; frac int;
  -- Number of instants generated so far
  l int;
  -- Categories of the current and next road
  category int; nextCategory int;
  -- Current speed and distance of the moving car
  curSpeed float; curDist float;
  -- Time to wait and total wait time
  waitTime float; totalWaitTime float = 0.0;
  -- Time to travel the fraction given the current speed and total travel time
  travelTime float; totalTravelTime float = 0.0;
  -- Angle between the current segment and the next one
  alpha float;
  -- Maximum speed of an edge
  maxSpeedEdge float;
  -- Maximum speed of a turn between two segments as determined
  -- by their angle
  maxSpeedTurn float;
  -- Maximum speed and new speed of the car
  maxSpeed float; newSpeed float;
  -- Coordinates of the next point
  x float; y float;
  -- Coordinates of p1 and p2
  x1 float; y1 float; x2 float; y2 float;
  -- Number in [0,1] used for determining the next point
  fraction float;
  -- Disturbance of the coordinates of a point and total accumulated
  -- error in the coordinates of an edge. Used when disturbing the position
  -- of an object to simulate GPS errors
  dx float; dy float;
  errx float = 0.0; erry float = 0.0;
  -- Length of a segment and maximum speed of an edge
  segLength float;
  -- Geometries of the current edge
  linestring geometry;
  -- Points of the current linestring
  points geometry [];
  -- Start and end points of segment of a linestring
  p1 geometry; p2 geometry;
  -- Next point (if any) after p2 in the same edge
  p3 geometry;
  -- Current position of the moving object
  curPos geometry;
  -- Current timestamp of the moving object
  t timestamptz;
  -- Instants of the result being constructed
  instants tgeompoint[];
  -- Statistics about the trip
  noAccel int = 0;
  noDecel int = 0;
  noStop int = 0;
  twSumSpeed float = 0.0;
BEGIN
  srid = ST_SRID((edges[1]).linestring);
  p1 = ST_PointN((edges[1]).linestring, 1);
  x1 = ST_X(p1);
  y1 = ST_Y(p1);
  curPos = p1;
  t = startTime;
  curSpeed = 0;
  instants[1] = tgeompoint_inst(p1, t);
  l = 2;
  noEdges = array_length(edges, 1);
  -- Loop for every edge
  FOR edge IN 1..noEdges LOOP
    IF messages = 'debug' THEN
      RAISE INFO '      Edge %', edge;
    END IF;
    -- Get the information about the current edge
    linestring = (edges[edge]).linestring;
    maxSpeedEdge = (edges[edge]).maxSpeed;
    category = (edges[edge]).category;
    SELECT array_agg(geom ORDER BY path) INTO points
    FROM ST_DumpPoints(linestring);
    noSegs = array_length(points, 1) - 1;
    -- Loop for every segment of the current edge
    FOR seg IN 1..noSegs LOOP
      IF messages = 'debug' AND noSegs > 1 THEN
        RAISE INFO '        Segment %', seg;
      END IF;
      p2 = points[seg + 1];
      x2 = ST_X(p2);
      y2 = ST_Y(p2);
      -- If there is a segment ahead in the current edge
      -- compute the angle of the turn
      IF seg < noSegs THEN
        p3 = points[seg + 2];
        -- Compute the angle α between the current segment and the next one;
        alpha = degrees(ST_Angle(p1, p2, p3));
        -- Compute the maximum speed at the turn by multiplying the
        -- maximum speed by a factor proportional to the angle so that
        -- the factor is 1.00 at 0/360° and is 0.0 at 180°, e.g.
        -- 0° -> 1.00, 5° 0.97, 45° 0.75, 90° 0.50, 135° 0.25, 175° 0.03
        -- 180° 0.00, 185° 0.03, 225° 0.25, 270° 0.50, 315° 0.75, 355° 0.97, 360° 0.00
        IF abs(mod(alpha::numeric, 360.0)) < P_EPSILON THEN
          maxSpeedTurn = maxSpeedEdge;
        ELSE
          maxSpeedTurn = mod(abs(alpha - 180.0)::numeric, 180.0) / 180.0 * maxSpeedEdge;
        END IF;
      END IF;
      segLength = ST_Distance(p1, p2);
      IF segLength < P_EPSILON THEN
        RAISE EXCEPTION 'Segment % of edge % has zero length', seg, edge;
      END IF;
      fraction = P_EVENT_LENGTH / segLength;
      noFracs = ceiling(segLength / P_EVENT_LENGTH);
      -- Loop for every fraction of the current segment
      frac = 1;
      WHILE frac < noFracs LOOP
        -- If the current speed is considered as a stop, apply an
        -- acceleration event where the new speed is bounded by the
        -- maximum speed of either the segment or the turn
        IF curSpeed <= P_EPSILON_SPEED THEN
          noAccel = noAccel + 1;
          -- If we are not approaching a turn
          IF frac < noFracs THEN
            curSpeed = least(P_EVENT_ACC, maxSpeedEdge);
          ELSE
            curSpeed = least(P_EVENT_ACC, maxSpeedTurn);
          END IF;
          IF messages = 'debug' THEN
            RAISE INFO '          Acceleration after stop -> Speed = %', round(curSpeed::numeric, 3);
          END IF;
        ELSE
          -- If the current speed is not considered as a stop,
          -- with a probability proportional to P_EVENT_C/vmax apply
          -- a deceleration event (p=90%) or a stop event (p=10%)
          IF random() <= P_EVENT_C / maxSpeedEdge THEN
            IF random() <= P_EVENT_P THEN
              -- Apply stop event to the trip
              curSpeed = 0.0;
              noStop = noStop + 1;
              IF messages = 'debug' THEN
                RAISE INFO '          Stop -> Speed = %', round(curSpeed::numeric, 3);
              END IF;
            ELSE
              -- Apply deceleration event to the trip
              curSpeed = curSpeed * random_binomial(20, 0.5) / 20.0;
              noDecel = noDecel + 1;
              IF messages = 'debug' THEN
                RAISE INFO '          Deceleration -> Speed = %', round(curSpeed::numeric, 3);
              END IF;
            END IF;
          ELSE
            -- Apply an acceleration event. The speed is bound by
            -- (1) the maximum speed of the turn if we are within
            -- an edge, or (2) the maximum speed of the edge
            IF frac = noFracs AND seg < noSegs THEN
              maxSpeed = maxSpeedTurn;
              IF messages = 'debug' THEN
                RAISE INFO '           Turn -> Angle = %, Maximum speed at turn = %', round(alpha::numeric, 3), round(maxSpeedTurn::numeric, 3);
              END IF;
            ELSE
              maxSpeed = maxSpeedEdge;
            END IF;
            newSpeed = least(curSpeed + P_EVENT_ACC, maxSpeed);
            IF curSpeed < newSpeed THEN
              noAccel = noAccel + 1;
              IF messages = 'debug' THEN
                RAISE INFO '          Acceleration -> Speed = %', round(newSpeed::numeric, 3);
              END IF;
            ELSIF curSpeed > newSpeed THEN
              noDecel = noDecel + 1;
              IF messages = 'debug' THEN
                RAISE INFO '          Deceleration -> Speed = %', round(newSpeed::numeric, 3);
              END IF;
            END IF;
            curSpeed = newSpeed;
          END IF;
        END IF;
        -- If speed is zero add a wait time
        IF curSpeed < P_EPSILON_SPEED THEN
          waitTime = random_exp(P_DEST_EXPMU);
          IF waitTime < P_EPSILON THEN
            waitTime = P_DEST_EXPMU;
          END IF;
          t = t + waitTime * interval '1 sec';
          totalWaitTime = totalWaitTime + waitTime;
          IF messages = 'debug' THEN
            RAISE INFO '          Waiting for % seconds', round(waitTime::numeric, 3);
          END IF;
        ELSE
          -- Otherwise, move current position P_EVENT_LENGTH meters towards p2
          -- or to p2 if it is the last fraction
          IF frac < noFracs THEN
            x = x1 + ((x2 - x1) * fraction * frac);
            y = y1 + ((y2 - y1) * fraction * frac);
            IF disturbData THEN
              dx = (2 * P_GPS_STEPMAXERR * rand()) - P_GPS_STEPMAXERR;
              dy = (2 * P_GPS_STEPMAXERR * rand()) - P_GPS_STEPMAXERR;
              errx = errx + dx;
              erry = erry + dy;
              IF errx > P_GPS_TOTALMAXERR THEN
                errx = P_GPS_TOTALMAXERR;
              END IF;
              IF errx < - 1 * P_GPS_TOTALMAXERR THEN
                errx = -1 * P_GPS_TOTALMAXERR;
              END IF;
              IF erry > P_GPS_TOTALMAXERR THEN
                erry = P_GPS_TOTALMAXERR;
              END IF;
              IF erry < -1 * P_GPS_TOTALMAXERR THEN
                erry = -1 * P_GPS_TOTALMAXERR;
              END IF;
              x = x + dx;
              y = y + dy;
            END IF;
            curPos = ST_SetSRID(ST_Point(x, y), srid);
            curDist = P_EVENT_LENGTH;
          ELSE
            curPos = p2;
            curDist = segLength - (segLength * fraction * (frac - 1));
          END IF;
          travelTime = (curDist / (curSpeed / 3.6));
          IF travelTime < P_EPSILON THEN
            travelTime = P_DEST_EXPMU;
          END IF;
          t = t + travelTime * interval '1 sec';
          totalTravelTime = totalTravelTime + travelTime;
          twSumSpeed = twSumSpeed + (travelTime * curSpeed);
          frac = frac + 1;
        END IF;
        instants[l] = tgeompoint_inst(curPos, t);
        l = l + 1;
      END LOOP;
      p1 = p2;
      x1 = x2;
      y1 = y2;
    END LOOP;
    -- If we are not already in a stop, apply a stop event with a
    -- probability depending on the category of the current edge
    -- and the next one (if any)
    IF curSpeed > P_EPSILON_SPEED AND edge < noEdges THEN
      nextCategory = (edges[edge + 1]).category;
      IF random() <= P_DEST_STOPPROB[category][nextCategory] THEN
        curSpeed = 0;
        waitTime = random_exp(P_DEST_EXPMU);
        IF waitTime < P_EPSILON THEN
          waitTime = P_DEST_EXPMU;
        END IF;
        t = t + waitTime * interval '1 sec';
        totalWaitTime = totalWaitTime + waitTime;
        IF messages = 'debug' THEN
          RAISE INFO '      Stop at crossing -> Waiting for % seconds', round(waitTime::numeric, 3);
        END IF;
        instants[l] = tgeompoint_inst(curPos, t);
        l = l + 1;
      END IF;
    END IF;
  END LOOP;
  IF messages = 'verbose' OR messages = 'debug' THEN
    RAISE INFO '      Number of edges %', noEdges;
    RAISE INFO '      Number of acceleration events: %', noAccel;
    RAISE INFO '      Number of deceleration events: %', noDecel;
    RAISE INFO '      Number of stop events: %', noStop;
    RAISE INFO '      Total travel time: % secs.', round(totalTravelTime::numeric, 3);
    RAISE INFO '      Total waiting time: % secs.', round(totalWaitTime::numeric, 3);
    RAISE INFO '      Time-weighted average speed: % Km/h',
      round((twSumSpeed / (totalTravelTime + totalWaitTime))::numeric, 3);
  END IF;
  RETURN tgeompoint_seq(instants, true, true, true);
  -- RETURN instants;
END;
$$ LANGUAGE plpgsql STRICT;

/*
WITH Temp(trip) AS (
  SELECT createTrip(createPath(34125, 44979, 'Fastest Path'), '2020-05-10 08:00:00', false, 'minimal')
)
SELECT startTimestamp(trip), endTimestamp(trip), timespan(trip)
FROM Temp;
*/

-------------------------------------------------------------------------------

-- Choose a random home, work, or destination node for the region-based
-- approach

CREATE OR REPLACE FUNCTION berlinmod_selectHomeNode()
RETURNS bigint AS $$
DECLARE
  -- Result of the function
  result bigint;
BEGIN
  WITH RandomRegion AS (
    SELECT id
    FROM HomeRegions
    WHERE random() <= cumProb
    ORDER BY cumProb
    LIMIT 1
  )
  SELECT N.id INTO result
  FROM HomeNodes N, RandomRegion R
  WHERE N.region = R.id
  ORDER BY random()
  LIMIT 1;
  RETURN result;
END;
$$ LANGUAGE plpgsql STRICT;

/*
-- WE DON'T COVER ALL REGIONS EVEN AFTER 1e5 attempts
with temp(node) as (
select berlinmod_selectHomeNode()
from generate_series(1, 1e5)
)
select region, count(*)
from temp T, homenodes N
where t.node = id
group by region order by region;
-- Total query runtime: 3 min 6 secs.
*/

CREATE OR REPLACE FUNCTION berlinmod_selectWorkNode()
RETURNS int AS $$
DECLARE
  -- Result of the function
  result bigint;
BEGIN
  WITH RandomRegion AS (
    SELECT id
    FROM WorkRegions
    WHERE random() <= cumProb
    ORDER BY cumProb
    LIMIT 1
  )
  SELECT N.id INTO result
  FROM WorkNodes N, RandomRegion R
  WHERE N.region = R.id
  ORDER BY random()
  LIMIT 1;
  RETURN result;
END;
$$ LANGUAGE plpgsql STRICT;

/*
-- WE DON'T COVER ALL REGIONS EVEN AFTER 1e5 attempts
with temp(node) as (
select berlinmod_selectWorkNode()
from generate_series(1, 1e5)
)
select region, count(*)
from temp T, homenodes N
where t.node = id
group by region order by region;
-- Total query runtime: 3 min.
*/

-- Selects a destination node for an additional trip. 80% of the
-- destinations are from the neighbourhood, 20% are from the complete graph

CREATE OR REPLACE FUNCTION berlinmod_selectDestNode(vehId int, noNeigh int,
  noNodes int)
RETURNS bigint AS $$
DECLARE
  -- Random sequence number
  seq int;
  -- Result of the function
  result bigint;
BEGIN
  IF noNeigh > 0 AND random() < 0.8 THEN
    seq = random_int(1, noNeigh);
    SELECT node INTO result
    FROM Neighbourhoods
    WHERE vehicleId = vehId AND seqNo = seq;
  ELSE
    result = random_int(1, noNodes);
  END IF;
  RETURN result;
END;
$$ LANGUAGE plpgsql STRICT;

/*
SELECT berlinmod_selectDestNode(150)
FROM generate_series(1, 50)
ORDER BY 1;
*/

-- Return the unique licence string for a given vehicle identifier
-- where the identifier is in [0,26999]

CREATE OR REPLACE FUNCTION berlinmod_createLicence(vehicId int)
  RETURNS text AS $$
BEGIN
  IF vehicId > 0 and vehicId < 1000 THEN
    RETURN text 'B-' || chr(random_int(1, 26) + 65) || chr(random_int(1, 25) + 65)
      || ' ' || vehicId::text;
  ELSEIF vehicId % 1000 = 0 THEN
    RETURN text 'B-' || chr((vehicId % 1000) + 65) || ' '
      || (random_int(1, 998) + 1)::text;
  ELSE
    RETURN text 'B-' || chr((vehicId % 1000) + 64) || 'Z '
      || (vehicId % 1000)::text;
  END IF;
END;
$$ LANGUAGE plpgsql STRICT;

/*
SELECT berlinmod_createLicence(random_int(1,100))
FROM generate_series(1, 10);
*/


-- Return a random vehicle type with the following values
-- passenger (p=90%), bus (p=5%), truck (p=5%)

CREATE OR REPLACE FUNCTION berlinmod_vehicleType()
  RETURNS text AS $$
DECLARE
  -------------------------
  -- CONSTANT PARAMETERS --
  -------------------------
  P_VEHICLE_TYPES text[] = '{"passenger", "bus", "truck"}';
BEGIN
  IF random() < 0.9 THEN
    RETURN P_VEHICLE_TYPES[1];
  ELSEIF random() < 0.5 THEN
    RETURN P_VEHICLE_TYPES[2];
  ELSE
    RETURN P_VEHICLE_TYPES[3];
  END IF;
END;
$$ LANGUAGE plpgsql STRICT;

/*
 SELECT berlinmod_vehicleType(), COUNT(*)
 FROM generate_series(1, 1e5)
 GROUP BY 1
 ORDER BY 1;
 */

-- Return a random vehicle model with a uniform distribution

CREATE OR REPLACE FUNCTION berlinmod_vehicleModel()
  RETURNS text AS $$
DECLARE
  -------------------------
  -- CONSTANT PARAMETERS --
  -------------------------
  P_VEHICLE_MODELS text[] = '{"Mercedes-Benz", "Volkswagen", "Maybach",
    "Porsche", "Opel", "BMW", "Audi", "Acabion", "Borgward", "Wartburg",
    "Sachsenring", "Multicar"}';
  ---------------
  -- Variables --
  ---------------
  index int;
BEGIN
  index = random_int(1, array_length(P_VEHICLE_MODELS, 1));
    RETURN P_VEHICLE_MODELS[index];
END;
$$ LANGUAGE plpgsql STRICT;

/*
 SELECT berlinmod_vehicleModel(), COUNT(*)
 FROM generate_series(1, 1e5)
 GROUP BY 1
 ORDER BY 1;
 */

-- Generate the trips for a given number vehicles and days starting at a day.
-- The argument disturbData correspond to the parameter P_DISTURB_DATA

CREATE OR REPLACE FUNCTION berlinmod_createTrips(noVehicles int, noDays int,
  startDay date, disturbData boolean, messages text, tripGeneration text)
RETURNS void AS $$
DECLARE
  -- Loops over the days for which we generate the data
  d date;
  -- 0 (Sunday) to 6 (Saturday)
  weekday int;
  -- Current timestamp
  t timestamptz;
  -- Temporal point obtained from a path
  trip tgeompoint;
  -- Home and work nodes
  homeN bigint; workN bigint;
  -- Source and target nodes of one subtrip of a leisure trip
  sourceN bigint; targetN bigint;
  -- Paths betwen source and target nodes
  homework step[]; workhome step[]; path step[];
  -- Number of leisure trips and number of subtrips of a leisure trip
  noLeisTrip int; noSubtrips int;
  -- Morning or afternoon (1 or 2) leisure trip
  leisNo int;
  -- Number of previous trips generated so far
  tripSeq int = 0;
  -- Loop variables
  vehId int; j int; leis int; dest int;
BEGIN
  IF messages = 'minimal' THEN
    RAISE INFO 'Creation of the Trips table';
  ELSE
    RAISE INFO 'Creation of the Trips table started at %', clock_timestamp();
  END IF;
  DROP TABLE IF EXISTS Trips CASCADE;
  CREATE TABLE Trips(tripId SERIAL PRIMARY KEY, vehicleId int, startDate date,
    seqNo int, trip tgeompoint, trajectory geometry,
    UNIQUE (vehicleId, startDate, seqNo));
  -- Loop for each vehicle
  FOR vehId IN 1..noVehicles LOOP
    IF messages = 'medium' OR messages = 'verbose' THEN
      RAISE INFO '-- Vehicle %', vehId;
    ELSEIF vehId % 100 = 1 THEN
      RAISE INFO '  Vehicles % to %', vehId, least(vehId + 99, noVehicles);
    END IF;
    -- Get home -> work and work -> home paths
    SELECT homeNode, workNode INTO homeN, workN
    FROM VehicleNodes V WHERE V.vehicleId = vehId;
    SELECT array_agg((geom, speed, category)::step ORDER BY seqNo) INTO homework
    FROM Paths
    WHERE vehicleId = vehId AND start_vid = homeN AND end_vid = workN;
    SELECT array_agg((geom, speed, category)::step ORDER BY seqNo) INTO workhome
    FROM Paths
    WHERE vehicleId = vehId AND start_vid = workN AND end_vid = homeN;
    d = startDay;
    -- Loop for each generation day
    FOR j IN 1..noDays LOOP
      IF messages = 'verbose' THEN
        RAISE INFO '  -- Day %', d;
      END IF;
      weekday = date_part('dow', d);
      -- 1: Monday, 5: Friday
      IF weekday BETWEEN 1 AND 5 THEN
        -- Home -> Work
        t = d + time '08:00:00' + CreatePauseN(120);
        IF messages = 'verbose' OR messages = 'debug' THEN
          RAISE INFO '    Home to work trip started at %', t;
        END IF;
        IF tripGeneration = 'C' THEN
          trip = create_trip(homework, t, disturbData, messages);
        ELSE
          trip = createTrip(homework, t, disturbData, messages);
        END IF;
        IF messages = 'medium' THEN
          RAISE INFO '    Home to work trip started at % and lasted %',
            t, endTimestamp(trip) - startTimestamp(trip);
        END IF;
        INSERT INTO Trips(vehicleId, startDate, seqNo, trip, trajectory) VALUES
          (vehId, d, 1, trip, trajectory(trip));
        -- Work -> Home
        t = d + time '16:00:00' + CreatePauseN(120);
        IF messages = 'verbose' OR messages = 'debug' THEN
          RAISE INFO '    Work to home trip started at %', t;
        END IF;
        IF tripGeneration = 'C' THEN
          trip = create_trip(workhome, t, disturbData, messages);
        ELSE
          trip = createTrip(workhome, t, disturbData, messages);
        END IF;
        IF messages = 'medium' THEN
          RAISE INFO '    Work to home trip started at % and lasted %',
            t, endTimestamp(trip) - startTimestamp(trip);
        END IF;
        INSERT INTO Trips(vehicleId, startDate, seqNo, trip, trajectory) VALUES
          (vehId, d, 2, trip, trajectory(trip));
        tripSeq = 2;
      END IF;
      -- Get the number of leisure trips
      SELECT COUNT(DISTINCT tripNo) INTO noLeisTrip
      FROM LeisureTrips L
      WHERE L.vehicleId = vehId AND L.startDate = d;
      IF noLeisTrip = 0 AND messages = 'verbose' or messages = 'debug' THEN
        RAISE INFO '    No leisure trip';
      END IF;
      -- Loop for each leisure trip (0, 1, or 2)
      FOR leis IN 1..noLeisTrip LOOP
        IF weekday BETWEEN 1 AND 5 THEN
          t = d + time '20:00:00' + CreatePauseN(90);
          IF messages = 'medium' THEN
            RAISE INFO '    Weekday leisure trips started at %', t;
          END IF;
          leisNo = 1;
        ELSE
          -- Determine whether there is a morning/afternoon (1/2) trip
          IF noLeisTrip = 2 THEN
            leisNo = leis;
          ELSE
            SELECT tripNo INTO leisNo
            FROM LeisureTrips L
            WHERE L.vehicleId = vehId AND L.startDate = d
            LIMIT 1;
          END IF;
          -- Determine the start time
          IF leisNo = 1 THEN
            t = d + time '09:00:00' + CreatePauseN(120);
            IF messages = 'medium' THEN
              RAISE INFO '    Weekend morning trips started at %', t;
            END IF;
          ELSE
            t = d + time '17:00:00' + CreatePauseN(120);
            IF messages = 'medium' OR messages = 'verbose' or messages = 'debug' THEN
              RAISE INFO '    Weekend afternoon trips started at %', t;
            END IF;
          END IF;
        END IF;
        -- Get the number of subtrips (number of destinations + 1)
        SELECT COUNT(*) INTO noSubtrips
        FROM LeisureTrips L
        WHERE L.vehicleId = vehId AND L.tripNo = leisNo AND L.startDate = d;
        FOR dest IN 1..noSubtrips LOOP
          -- Get the source and destination nodes of the subtrip
          SELECT sourceNode, targetNode INTO sourceN, targetN
          FROM LeisureTrips L
          WHERE L.vehicleId = vehId AND L.startDate = d AND L.tripNo = leisNo AND L.seqNo = dest;
          -- Get the path
          SELECT array_agg((geom, speed, category)::step ORDER BY seqNo) INTO path
          FROM Paths P
          WHERE vehicleId = vehId AND start_vid = sourceN AND end_vid = targetN AND edge > 0;
          IF messages = 'verbose' OR messages = 'debug' THEN
            RAISE INFO '    Leisure trip from % to % started at %', sourceN, targetN, t;
          END IF;
          IF tripGeneration = 'C' THEN
            trip = create_trip(path, t, disturbData, messages);
          ELSE
            trip = createTrip(path, t, disturbData, messages);
          END IF;
          IF messages = 'medium' THEN
            RAISE INFO '    Leisure trip started at % and lasted %',
              t, endTimestamp(trip) - startTimestamp(trip);
          END IF;
          tripSeq = tripSeq + 1;
          INSERT INTO Trips(vehicleId, startDate, seqNo, trip, trajectory) VALUES
            (vehId, d, tripSeq, trip, trajectory(trip));
          -- Add a delay time in [0, 120] min using a bounded Gaussian distribution
          t = endTimestamp(trip) + createPause();
        END LOOP;
      END LOOP;
      d = d + 1 * interval '1 day';
    END LOOP;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE plpgsql STRICT;

/*
SELECT berlinmod_createTrips(2, 2, '2020-05-10', 'Fastest Path', false, 'C');
*/

-------------------------------------------------------------------------------
-- Main Function
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION berlinmod_datagenerator(
  scaleFactor float DEFAULT NULL,
  noVehicles int DEFAULT NULL, noDays int DEFAULT NULL,
  startDay date DEFAULT NULL, pathMode text DEFAULT NULL,
  nodeChoice text DEFAULT NULL, disturbData boolean DEFAULT NULL,
  messages text DEFAULT NULL, tripGeneration text DEFAULT NULL, 
  indexType text DEFAULT NULL)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  ----------------------------------------------------------------------
  -- Primary parameters, which are optional arguments of the function
  ----------------------------------------------------------------------

  -- Scale factor
  -- Set value to 1.0 or bigger for a full-scaled benchmark
  P_SCALE_FACTOR float = 0.005;

  -- By default, the scale factor determine the number of cars and the
  -- number of days they are observed as follows
  --    noVehicles int = round((2000 * sqrt(P_SCALE_FACTOR))::numeric, 0)::int;
  --    noDays int = round((sqrt(P_SCALE_FACTOR) * 28)::numeric, 0)::int;
  -- For example, for P_SCALE_FACTOR = 1.0 these values will be
  --    noVehicles = 2000
  --    noDays int = 28
  -- Alternatively, you can manually set these parameters to arbitrary
  -- values using the optional arguments in the function call.

  -- The day the observation starts ===
  -- default: P_START_DAY = monday 06/01/2020)
  P_START_DAY date = '2020-06-01';

  -- Method for selecting a path between source and target nodes.
  -- Possible values are 'Fastest Path' (default) and 'Shortest Path'
  P_PATH_MODE text = 'Fastest Path';

  -- Method for selecting home and work nodes.
  -- Possible values are 'Network Based' for chosing the nodes with a
  -- uniform distribution among all nodes (default) and 'Region Based'
  -- to use the population and number of enterprises statistics in the
  -- Regions tables
  P_NODE_CHOICE text = 'Network Based';

  -- Choose imprecise data generation. Possible values are
  -- FALSE (no imprecision, default) and TRUE (disturbed data)
  P_DISTURB_DATA boolean = FALSE;

  -------------------------------------------------------------------------
  --  Secondary Parameters
  -------------------------------------------------------------------------

  -- Seed for the random generator used to ensure deterministic results
  P_RANDOM_SEED float = 0.5;

  -- Radius in meters defining a node's neigbourhood
  -- Default= 3 km
  P_NEIGHBOURHOOD_RADIUS float = 3000.0;

  -- Size for sample relations
  P_SAMPLE_SIZE int = 100;

  -- Number of paths sent in a batch to pgRouting
  P_PGROUTING_BATCH_SIZE int = 1e5;

  -- Quantity of messages shown describing the generation process
  -- Possible values are 'minimal', 'medium', 'verbose', and 'debug'
  P_MESSAGES text = 'minimal';

  -- Determine the language used to generate the trips.
  -- Possible values are 'C' (default) and 'SQL'
  P_TRIP_GENERATION text = 'C';

  -- Determine the type of indices.
  -- Possible values are 'GiST' (default) and 'SPGiST'
  P_INDEX_TYPE text = 'GiST';

  ----------------------------------------------------------------------
  --  Variables
  ----------------------------------------------------------------------

  -- Number of nodes in the graph
  noNodes int;
  -- Number of nodes in the neighbourhood of the home node of a vehicle
  noNeigh int;
  -- Number of leisure trips (1 or 2 on week/weekend) in a day
  noLeisTrips int;
  -- Number of paths
  noPaths int;
  -- Number of calls to pgRouting
  noCalls int;
  -- Number of trips generated
  noTrips int;
  -- Loop variables
  vehId int; dayNo int; c int; leis int;
  -- Home and work node identifiers
  homeN bigint; workN bigint;
  -- Node identifiers of a trip within a chain of leisure trips
  sourceN bigint; targetN bigint;
  -- Day for generating a leisure trip
  d date;
  -- Week day 0 -> 6: Sunday -> Saturday
  weekDay int;
  -- Attributes of table Vehicles
  lic text; vehType text; model text;
  -- Start and end time of the generation
  startTime timestamptz; endTime timestamptz;
  -- Start and end time of the batch call to pgRouting
  startPgr timestamptz; EndTimegr timestamptz;
  -- Queries sent to pgrouting for choosing the path according to P_PATH_MODE
  -- and the number of records defined by LIMIT/OFFSET
  query1_pgr text; query2_pgr text;
  -- Random number of destinations (between 1 and 3)
  noDest int;
  -- String to generate the trace message
  str text;
BEGIN

  -------------------------------------------------------------------------
  --  Initialize parameters and variables using default values if not provided
  -------------------------------------------------------------------------

  -- Setting the parameters of the generation
  IF scaleFactor IS NULL THEN
    scaleFactor = P_SCALE_FACTOR;
  END IF;
  IF noVehicles IS NULL THEN
    noVehicles = round((2000 * sqrt(scaleFactor))::numeric, 0)::int;
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
  IF nodeChoice IS NULL THEN
    nodeChoice = P_NODE_CHOICE;
  END IF;
  IF disturbData IS NULL THEN
    disturbData = P_DISTURB_DATA;
  END IF;
  IF messages IS NULL THEN
    messages = P_MESSAGES;
  END IF;
  IF tripGeneration IS NULL THEN
    tripGeneration = P_TRIP_GENERATION;
  END IF;
  IF indexType IS NULL THEN
    indexType = P_INDEX_TYPE;
  END IF;

  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Starting the BerlinMOD data generator with scale factor %', scaleFactor;
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Parameters:';
  RAISE INFO '------------';
  RAISE INFO 'No. of vehicles = %, No. of days = %, Start day = %',
    noVehicles, noDays, startDay;
  RAISE INFO 'Path mode = %, Disturb data = %', pathMode, disturbData;
  RAISE INFO 'Verbosity = %, Trip generation = %, Index type = %', 
    messages, tripGeneration, indexType;
  startTime = clock_timestamp();
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO '------------------------------------------------------------------';

  -------------------------------------------------------------------------
  --  Creating the base data
  -------------------------------------------------------------------------

  -- Set the seed so that the random function will return a repeatable
  -- sequence of random numbers that is derived from the P_RANDOM_SEED.
  PERFORM setseed(P_RANDOM_SEED);

  -- Create a table accumulating all pairs (source, target) that will be
  -- sent to pgRouting in a single call. We DO NOT test whether we are
  -- inserting duplicates in the table, the query sent to the pgr_dijkstra
  -- function MUST use 'SELECT DISTINCT ...'

  RAISE INFO 'Creating the Destinations table';
  DROP TABLE IF EXISTS Destinations CASCADE;
  CREATE TABLE Destinations(vehicleId int, sourceNode bigint, targetNode bigint,
    PRIMARY KEY (vehicleId, sourceNode, targetNode));

  -- Create a relation with all vehicles, their home and work node and the
  -- number of neighbourhood nodes

  RAISE INFO 'Creating the VehicleNodes, Vehicles, and Neighbourhoods tables';
  DROP TABLE IF EXISTS VehicleNodes CASCADE;
  CREATE TABLE VehicleNodes(vehicleId int PRIMARY KEY, homeNode bigint NOT NULL,
    workNode bigint NOT NULL, noNeighbours int);
  DROP TABLE IF EXISTS Vehicles CASCADE;
  CREATE TABLE Vehicles(vehicleId int PRIMARY KEY, licence text, vehType text,
    model text);
  DROP TABLE IF EXISTS Neighbourhoods CASCADE;
  CREATE TABLE Neighbourhoods(vehicleId int, seqNo int, node bigint NOT NULL,
    PRIMARY KEY (vehicleId, seqNo));

  -- Get the number of nodes
  SELECT COUNT(*) INTO noNodes FROM Nodes;

  FOR vehId IN 1..noVehicles LOOP
    IF nodeChoice = 'Network Based' THEN
      homeN = random_int(1, noNodes);
      workN = random_int(1, noNodes);
    ELSE
      homeN = berlinmod_selectHomeNode();
      workN = berlinmod_selectWorkNode();
    END IF;
    IF homeN IS NULL OR workN IS NULL THEN
      RAISE EXCEPTION '    The home and the work nodes cannot be NULL';
    END IF;
    INSERT INTO VehicleNodes(vehicleId, homeNode, workNode) VALUES
      (vehId, homeN, workN);
    -- Destinations
    INSERT INTO Destinations(vehicleId, sourceNode, targetNode) VALUES
      (vehId, homeN, workN), (vehId, workN, homeN);
    -- Vehicles
    lic = berlinmod_createLicence(vehId);
    vehType = berlinmod_vehicleType();
    model = berlinmod_vehicleModel();
    INSERT INTO Vehicles(vehicleId, licence, vehType, model) VALUES
      (vehId, lic, vehType, model);

    INSERT INTO Neighbourhoods(vehicleId, seqNo, node)
    WITH Temp(vehicle, n) AS (
      SELECT vehId, N2.id
      FROM Nodes N1, Nodes N2
      WHERE N1.id = homeN AND N1.id <> N2.id AND
        ST_DWithin(N1.geom, N2.geom, P_NEIGHBOURHOOD_RADIUS)
    )
    SELECT vehicle, ROW_NUMBER() OVER (), n
    FROM Temp;
  END LOOP;

  UPDATE VehicleNodes V
  SET noNeighbours = (SELECT COUNT(*) FROM Neighbourhoods N WHERE N.vehicleId = V.vehicleId);

  -------------------------------------------------------------------------
  -- Create auxiliary benchmarking data
  -- The number of rows these tables is determined by P_SAMPLE_SIZE
  -------------------------------------------------------------------------

  RAISE INFO 'Creating the Licences table';

  DROP TABLE IF EXISTS Licences CASCADE;
  CREATE TABLE Licences(licenceId int PRIMARY KEY, licence text, 
    vehicleId int REFERENCES Vehicles(VehicleId));
  INSERT INTO Licences(licenceId, licence, vehicleId)
  WITH Temp(licenceId, vehicleId) AS (
    SELECT licenceId, random_int(1, noVehicles)
    FROM generate_series(1, P_SAMPLE_SIZE) licenceId
  )
  SELECT T.licenceId, V.licence, V.vehicleId
  FROM Temp T, Vehicles V
  WHERE T.vehicleId = V.vehicleId;

  CREATE INDEX Licences_VehicleId_idx ON Licences USING btree (VehicleId);

  CREATE VIEW Licences1 (LicenceId, Licence, vehicleId) AS
  SELECT LicenceId, Licence, vehicleId
  FROM Licences
  LIMIT 10;

  CREATE VIEW Licences2 (LicenceId, Licence, vehicleId) AS
  SELECT LicenceId, Licence, vehicleId
  FROM Licences
  LIMIT 10 OFFSET 10;

  RAISE INFO 'Creating the Points and Regions tables';

  -- Random points
  DROP TABLE IF EXISTS Points CASCADE;
  CREATE TABLE Points(pointId int PRIMARY KEY, geom geometry(Point, 3857));
  INSERT INTO Points(pointId, geom)
  WITH Temp(pointId, nodeId) AS (
    SELECT pointId, random_int(1, noNodes)
    FROM generate_series(1, P_SAMPLE_SIZE) pointId
  )
  SELECT T.pointId, N.geom
  FROM Temp T, Nodes N
  WHERE T.nodeId = N.id;

  RAISE NOTICE 'Creating indexes on table Points';
  IF indexType = 'GiST' THEN
    CREATE INDEX IF NOT EXISTS Points_geom_gist_idx ON Points USING gist(geom);
  ELSE
    CREATE INDEX IF NOT EXISTS Points_geom_spgist_idx ON Points USING spgist(geom);
  END IF;

  CREATE VIEW Points1 (PointId, geom) AS
  SELECT PointId, geom
  FROM Points
  LIMIT 10;

  -- Random regions
  DROP TABLE IF EXISTS Regions CASCADE;
  CREATE TABLE Regions(regionId int PRIMARY KEY, geom geometry(Polygon, 3857));
  INSERT INTO Regions(regionId, geom)
  WITH Temp(regionId, nodeId) AS (
    SELECT regionId, random_int(1, noNodes)
    FROM generate_series(1, P_SAMPLE_SIZE) regionId
  )
  SELECT T.regionId, ST_Buffer(N.geom, random_int(1, 997) + 3.0, random_int(0, 25)) AS geom
  FROM Temp T, Nodes N
  WHERE T.nodeId = N.id;

  RAISE NOTICE 'Creating indexes on table Regions';
  IF indexType = 'GiST' THEN
    CREATE INDEX IF NOT EXISTS Regions_geom_gist_idx ON Regions USING gist (geom);
  ELSE
    CREATE INDEX IF NOT EXISTS Regions_geom_spgist_idx ON Regions USING spgist (geom);
  END IF;

  CREATE VIEW Regions1 (RegionId, geom) AS
  SELECT RegionId, geom
  FROM Regions
  LIMIT 10;

  -- Random instants
  RAISE INFO 'Creating the Instants and Periods tables';
  DROP TABLE IF EXISTS Instants CASCADE;
  CREATE TABLE Instants(instantId int PRIMARY KEY, instant timestamptz);
  INSERT INTO Instants(instantId, instant)
  SELECT id, startDay + (random() * noDays) * interval '1 day' AS instant
  FROM generate_series(1, P_SAMPLE_SIZE) id;

  CREATE INDEX IF NOT EXISTS Instants_instant_idx ON Instants USING btree(Instant);

  CREATE VIEW Instants1 (InstantId, Instant) AS
  SELECT InstantId, Instant
  FROM Instants
  LIMIT 10;

  -- Random periods
  DROP TABLE IF EXISTS Periods CASCADE;
  CREATE TABLE Periods(periodId int PRIMARY KEY, period tstzspan);
  INSERT INTO Periods(periodId, period)
  WITH Instants AS (
    SELECT id, startDay + (random() * noDays) * interval '1 day' AS instant
    FROM generate_series(1, P_SAMPLE_SIZE) id
  )
  SELECT id, span(instant, instant + abs(random_gauss()) * interval '1 day',
    true, true) AS period
  FROM Instants;

  RAISE NOTICE 'Creating indexes on table Periods';
  IF indexType = 'GiST' THEN
    CREATE INDEX IF NOT EXISTS Periods_Period_gist_idx ON Periods USING gist (Period);
  ELSE
    CREATE INDEX IF NOT EXISTS Periods_Period_spgist_idx ON Periods USING spgist (Period);
  END IF;

  CREATE VIEW Periods1 (PeriodId, Period) AS
  SELECT PeriodId, Period
  FROM Periods
  LIMIT 10;

  -------------------------------------------------------------------------
  -- Generate the leisure trips.
  -- There is at most 1 leisure trip during the week (evening) and at most
  -- 2 leisure trips during the weekend (morning and afternoon).
  -- The value of attribute tripNo is 1 for evening and morning trips
  -- and is 2 for afternoon trips.
  -------------------------------------------------------------------------

  RAISE INFO 'Creating the LeisureTrips table';
  DROP TABLE IF EXISTS LeisureTrips CASCADE;
  CREATE TABLE LeisureTrips(vehicleId int, startDate date, tripNo int, seqNo int,
    sourceNode bigint, targetNode bigint,
    PRIMARY KEY (vehicleId, startDate, tripNo, seqNo));
  -- Loop for every vehicle
  FOR vehId IN 1..noVehicles LOOP
    IF messages = 'verbose' THEN
      RAISE INFO '-- Vehicle %', vehId;
    END IF;
    -- Get home node and number of neighbour nodes
    SELECT homeNode, noNeighbours INTO homeN, noNeigh
    FROM VehicleNodes V WHERE V.vehicleId = vehId;
    d = startDay;
    -- Loop for every generation day
    FOR dayNo IN 1..noDays LOOP
      IF messages = 'verbose' THEN
        RAISE INFO '  -- Day %', d;
      END IF;
      weekday = date_part('dow', d);
      -- Generate leisure trips (if any)
      -- 1: Monday, 5: Friday
      IF weekday BETWEEN 1 AND 5 THEN
        noLeisTrips = 1;
      ELSE
        noLeisTrips = 2;
      END IF;
      -- Loop for every leisure trip in a day (1 or 2)
      FOR leis IN 1..noLeisTrips LOOP
        -- Generate a set of leisure trips with a probability 0.4
        IF random() <= 0.4 THEN
          -- Select a number of destinations between 1 and 3
          IF random() < 0.8 THEN
            noDest = 1;
          ELSIF random() < 0.5 THEN
            noDest = 2;
          ELSE
            noDest = 3;
          END IF;
          IF messages = 'verbose' THEN
            IF weekday BETWEEN 1 AND 5 THEN
              str = '    Evening';
            ELSE
              IF leis = 1 THEN
                str = '    Morning';
              ELSE
                str = '    Afternoon';
              END IF;
            END IF;
            RAISE INFO '% leisure trip with % destinations', str, noDest;
          END IF;
          sourceN = homeN;
          FOR dest IN 1..noDest + 1 LOOP
            IF dest <= noDest THEN
              targetN = berlinmod_selectDestNode(vehId, noNeigh, noNodes);
            ELSE
              targetN = homeN;
            END IF;
            IF targetN IS NULL THEN
              RAISE EXCEPTION '    Destination node cannot be NULL';
            END IF;
            IF messages = 'verbose' THEN
              RAISE INFO '    Leisure trip from % to %', sourceN, targetN;
            END IF;
            INSERT INTO LeisureTrips(vehicleId, startDate, tripNo, seqNo, sourceNode, targetNode) VALUES
              (vehId, d, leis, dest, sourceN, targetN);
            INSERT INTO Destinations(vehicleId, sourceNode, targetNode)
              VALUES (vehId, sourceN, targetN)
              ON CONFLICT DO NOTHING;
            sourceN = targetN;
          END LOOP;
        ELSE
          IF messages = 'verbose' THEN
            RAISE INFO '    No leisure trip';
          END IF;
        END IF;
      END LOOP;
      d = d + 1 * interval '1 day';
    END LOOP;
  END LOOP;

  -- Build indexes to speed up processing
  CREATE INDEX Destinations_vehicleId_idx ON Destinations USING BTREE(vehicleId);

  -------------------------------------------------------------------------
  -- Call pgRouting to generate the paths
  -------------------------------------------------------------------------

  IF messages = 'minimal' THEN
    RAISE INFO 'Creating the Paths table';
  ELSE
    RAISE INFO 'Creation of the Paths table started at %', clock_timestamp();
  END IF;
  DROP TABLE IF EXISTS Paths CASCADE;
  CREATE TABLE Paths(
    -- This attribute is needed for partitioning the table for big scale factors
    vehicleId int,
    -- The following attributes are generated by pgRouting
    start_vid bigint, end_vid bigint, seqNo int, node bigint, edge bigint,
    -- The following attributes are filled in the subsequent update
    geom geometry NOT NULL, speed float NOT NULL, category int NOT NULL,
    PRIMARY KEY (vehicleId, start_vid, end_vid, seqNo));

  -- Select query sent to pgRouting
  IF pathMode = 'Fastest Path' THEN
    query1_pgr = 'SELECT SegmentId AS id, sourcenode AS source, targetnode AS target, cost_s AS cost, reverse_cost_s as reverse_cost FROM RoadSegments';
  ELSE
    query1_pgr = 'SELECT SegmentId AS id, sourcenode AS source, targetnode AS target, SegmentLength AS cost, SegmentLength * sign(reverse_cost_s) AS reverse_cost FROM RoadSegments';
  END IF;
  -- Get the total number of paths and number of calls to pgRouting
  SELECT COUNT(*) INTO noPaths
  FROM (SELECT DISTINCT sourceNode, targetNode FROM Destinations) AS T;
  noCalls = ceiling(noPaths / P_PGROUTING_BATCH_SIZE::float);
  IF messages = 'minimal' OR messages = 'medium' OR messages = 'verbose' THEN
    IF noCalls = 1 THEN
      RAISE INFO 'Call to pgRouting to compute % paths', noPaths;
    ELSE
      RAISE INFO 'Call to pgRouting to compute % paths in % calls with % (source, target) couples each',
        noPaths, noCalls, P_PGROUTING_BATCH_SIZE;
    END IF;
  END IF;

  startPgr = clock_timestamp();
  FOR c IN 1..noCalls LOOP
    query2_pgr = format('SELECT DISTINCT sourceNode AS source, targetNode AS target '
      'FROM Destinations ORDER BY sourceNode, targetNode LIMIT %s OFFSET %s',
      P_PGROUTING_BATCH_SIZE, (c - 1) * P_PGROUTING_BATCH_SIZE);
    IF messages = 'medium' OR messages = 'verbose' THEN
      IF noCalls = 1 THEN
        RAISE INFO '  Call started at %', clock_timestamp();
      ELSE
        RAISE INFO '  Call number % started at %', c, clock_timestamp();
      END IF;
    END IF;
    INSERT INTO Paths(vehicleId, start_vid, end_vid, seqNo, node, edge, geom, speed, category)
    WITH Temp AS (
      SELECT start_vid, end_vid, path_seq, node, edge
      FROM pgr_dijkstra(query1_pgr, query2_pgr, true)
      WHERE edge > 0
    )
    SELECT D.vehicleId, start_vid, end_vid, path_seq, node, edge,
      -- adjusting directionality
      CASE
        WHEN T.node = S.sourceNode THEN S.SegmentGeo
        ELSE ST_Reverse(S.SegmentGeo)
      END AS geom, S.MaxSpeedFwd AS speed,
      berlinmod_roadCategory(S.tag_id) AS category
    FROM Destinations D, Temp T, RoadSegments S
    WHERE D.sourceNode = T.start_vid AND D.targetNode = T.end_vid AND
      S.SegmentId = T.edge;
    IF messages = 'medium' OR messages = 'verbose' THEN
      IF noCalls = 1 THEN
        RAISE INFO '  Call ended at %', clock_timestamp();
      ELSE
        RAISE INFO '  Call number % ended at %', i, clock_timestamp();
      END IF;
    END IF;
  END LOOP;
  EndTimegr = clock_timestamp();

  -- Build index to speed up processing
  CREATE INDEX Paths_vehicle_start_vid_end_vid_idx ON Paths
  USING BTREE(vehicleId, start_vid, end_vid);

  -------------------------------------------------------------------------
  -- Generate the trips
  -------------------------------------------------------------------------

  PERFORM berlinmod_createTrips(noVehicles, noDays, startDay, disturbData,
    messages, tripGeneration);

  RAISE NOTICE 'Creating indexes on table Trips';
  CREATE INDEX IF NOT EXISTS Trips_VehicleId_idx ON Trips USING btree(VehicleId);
  IF indexType = 'GiST' THEN
    CREATE INDEX IF NOT EXISTS Trips_trip_gist_idx ON Trips USING gist(trip);
  ELSE
    CREATE INDEX IF NOT EXISTS Trips_trip_spgist_idx ON Trips USING spgist(trip);
  END IF;

  -------------------------------------------------------------------------
  -- Print generation summary
  -------------------------------------------------------------------------

  -- Get the number of trips generated
  SELECT COUNT(*) INTO noTrips FROM Trips;

  SELECT clock_timestamp() INTO endTime;
  IF messages = 'medium' OR messages = 'verbose' THEN
    RAISE INFO '-----------------------------------------------------------------------';
    RAISE INFO 'BerlinMOD data generator with scale factor %', scaleFactor;
    RAISE INFO '-----------------------------------------------------------------------';
    RAISE INFO 'Parameters:';
    RAISE INFO '------------';
    RAISE INFO 'No. of vehicles = %, No. of days = %, Start day = %',
      noVehicles, noDays, startDay;
    RAISE INFO 'Path mode = %, Disturb data = %', pathMode, disturbData;
    RAISE INFO 'Verbosity = %, Trip generation = %, Index type = %', 
      messages, tripGeneration, indexType;
  END IF;
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Execution finished at %', endTime;
  RAISE INFO 'Execution time %', endTime - startTime;
  RAISE INFO 'Call to pgRouting with % paths lasted %',
    noPaths, EndTimegr - startPgr;
  RAISE INFO 'Number of trips generated %', noTrips;
  RAISE INFO '------------------------------------------------------------------';

  -------------------------------------------------------------------

  return 'THE END';
END; $$;

/*
select berlinmod_generate();
select berlinmod_generate(scaleFactor := 0.005);
select berlinmod_generate(noVehicles := 2, noDays := 2);
*/

-------------------------------------------------------------------------------
-- THE END
-------------------------------------------------------------------------------
