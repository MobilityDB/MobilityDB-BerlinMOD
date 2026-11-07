/******************************************************************************
 * Exports trips and municipalities as GeoJSON files for kepler.gl
 * - Trips per day: trips_<date>.geojson
 * Geometry is a LineString with coordinates [lon, lat, 0, epoch_ts]
 *
 * Usage (psql):
 *   SELECT export_trips('/tmp/trips_kepler/', 'YYYY-MM-DD');
 *   SELECT export_municipalities('/tmp/');
 *****************************************************************************/

DROP FUNCTION IF EXISTS export_trips(fullpath text, target_date date); 
CREATE OR REPLACE FUNCTION export_trips(fullpath text, target_date date) 
RETURNS text AS $$ 
DECLARE 
  dir     text; 
  outpath text; 
  trip_count integer;
  filename text;
  filtered_trip_ids bigint[];
  start_time timestamp;
  end_time timestamp;
BEGIN 
  dir := regexp_replace(fullpath, '/+$', '') || '/'; 
  BEGIN 
    PERFORM 1 FROM pg_ls_dir(dir) LIMIT 1; 
  EXCEPTION WHEN OTHERS THEN 
    RAISE EXCEPTION 'Output directory "%" does not exist or is not accessible', dir; 
  END;   
  
  start_time := target_date::timestamp;  -- 00:00:00
  end_time := target_date::timestamp + interval '23 hours 59 minutes 59 seconds';  -- 23:59:59
    
  filename := 'trips_' || to_char(target_date, 'YYYY-MM-DD') || '.geojson';
  outpath := dir || filename;
  
  WITH trip_times AS (
    SELECT DISTINCT
      t.tripid,
      MIN(getTimestamp(i)) OVER (PARTITION BY t.tripid) as trip_start,
      MAX(getTimestamp(i)) OVER (PARTITION BY t.tripid) as trip_end
    FROM trips t 
    CROSS JOIN LATERAL unnest(instants(t.trip)) AS i
  )
  SELECT array_agg(tripid) INTO filtered_trip_ids
  FROM trip_times
  WHERE trip_start >= start_time AND trip_end <= end_time;

  trip_count := COALESCE(array_length(filtered_trip_ids, 1), 0);

  IF trip_count = 0 THEN
    EXECUTE format('COPY (SELECT %L) TO %L', 
      '{"type":"FeatureCollection","features":[]}', 
      outpath);
  ELSE    
    EXECUTE format($q$ 
      COPY ( 
        WITH pts AS ( 
          SELECT 
            t.tripid, 
            t.vehicleid, 
            ST_X(ST_Transform(getValue(i), 4326)) AS lon, 
            ST_Y(ST_Transform(getValue(i), 4326)) AS lat, 
            EXTRACT(EPOCH FROM getTimestamp(i))::bigint AS ts
          FROM trips t 
          CROSS JOIN LATERAL unnest(instants(t.trip)) AS i 
          WHERE t.tripid = ANY(%L::bigint[])
        ), 
        features AS ( 
          SELECT 
            tripid, 
            vehicleid, 
            MIN(ts) AS t0, 
            json_build_object( 
              'type','Feature', 
              'properties', json_build_object( 
                'tripId', tripid, 
                'vehicleId', vehicleid, 
                't0', MIN(ts), 
                't1', MAX(ts), 
                'points', COUNT(*) 
              ), 
              'geometry', json_build_object( 
                'type','LineString', 
                'coordinates', json_agg(json_build_array(lon, lat, 0, ts) ORDER BY ts) 
              ) 
            ) AS f 
          FROM pts 
          GROUP BY tripid, vehicleid 
        ) 
        SELECT (json_build_object( 
          'type','FeatureCollection', 
          'features', json_agg(f ORDER BY t0) 
        ))::text 
        FROM features 
      ) TO %L 
    $q$, filtered_trip_ids, outpath);
  END IF;
 
  RETURN format('Exported %s trips for date %s (from %s to %s) to %s', trip_count, target_date, start_time, end_time, outpath); 
END; 
$$ LANGUAGE plpgsql;

-- Export Municipalities 
DROP FUNCTION IF EXISTS export_municipalities(fullpath text);
CREATE OR REPLACE FUNCTION export_municipalities(fullpath text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  dir     text;
  outpath text;
BEGIN  
  dir := regexp_replace(fullpath, '/+$', '') || '/';
  BEGIN
    PERFORM 1 FROM pg_ls_dir(dir) LIMIT 1;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Output directory "%" does not exist or is not accessible', dir;
  END;

  outpath := dir || 'municipalities.geojson';

  EXECUTE format($Q$
    COPY (
      SELECT (
        jsonb_build_object(
          'type','FeatureCollection',
          'features',
          jsonb_agg(
            jsonb_build_object(
              'type','Feature',
              'properties', jsonb_build_object(
                'MunicipalityName', m.MunicipalityName,
                'Population', m.Population,
                'PopDensityKm2', m.PopDensityKm2
              ),
              'geometry',
                ST_AsGeoJSON(
                  ST_Transform(m.MunicipalityGeo, 4326)
                )::jsonb
            )
          )
        )
      )::text
      FROM Municipalities m
    ) TO %L
  $Q$, outpath);

  RETURN format('Exported Municipalities to %s', outpath);
END;
$$;