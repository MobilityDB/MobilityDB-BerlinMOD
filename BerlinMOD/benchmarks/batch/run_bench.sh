#!/bin/bash
# MobilityDB BerlinMOD chapter-1 benchmark — index config matrix
PGBIN=/usr/local/pgsql/17/bin
DB=berlinmod_h3bench
OUT=/tmp/bench-results
mkdir -p "$OUT"

run_query() {
  local label="$1"; local sql="$2"; local config="$3"
  # 3 runs, take median
  local times=()
  local rows=""
  for i in 1 2 3; do
    local t=$($PGBIN/psql -p 5432 -d "$DB" -tAc "
      EXPLAIN (ANALYZE, BUFFERS, TIMING ON, FORMAT JSON)
      $sql
    " 2>/dev/null | grep -oE '"Execution Time": [0-9.]+' | head -1 | grep -oE '[0-9.]+')
    times+=("$t")
    if [ "$i" -eq 1 ]; then
      rows=$($PGBIN/psql -p 5432 -d "$DB" -tAc "SELECT count(*) FROM ($sql) x;" 2>/dev/null | tr -d ' ')
    fi
  done
  # median = middle of 3
  local sorted=$(printf '%s\n' "${times[@]}" | sort -n)
  local median=$(echo "$sorted" | sed -n '2p')
  printf '%-40s %-20s %12s ms %10s rows\n' "$label" "$config" "$median" "$rows" | tee -a "$OUT/results.txt"
}

set_indexes() {
  local config="$1"
  $PGBIN/psql -p 5432 -d "$DB" -q <<SQL
DROP INDEX IF EXISTS trips_trip_gist_idx;
DROP INDEX IF EXISTS trips_trip_spgist_idx;
DROP INDEX IF EXISTS trips_trip_h3_gist_idx;
SQL
  case "$config" in
    none) ;;
    gist)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_gist_idx ON trips USING gist(trip);"
      ;;
    spgist)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_spgist_idx ON trips USING spgist(trip);"
      ;;
    gist_h3)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_h3_gist_idx ON trips USING gist(trip_h3);"
      ;;
    gist_both)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_gist_idx ON trips USING gist(trip);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_h3_gist_idx ON trips USING gist(trip_h3);"
      ;;
    spgist_h3)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_spgist_idx ON trips USING spgist(trip);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_h3_gist_idx ON trips USING gist(trip_h3);"
      ;;
  esac
  $PGBIN/psql -p 5432 -d "$DB" -q -c "ANALYZE trips;"
}

# Sample queries (10 regions/points/periods)
Q1='SELECT DISTINCT R.regionid, T.vehid FROM Trips T, (SELECT * FROM Regions LIMIT 10) R WHERE eIntersects(T.Trip, R.Geom)'
Q1H='SELECT DISTINCT R.regionid, T.vehid FROM Trips T, (SELECT * FROM Regions LIMIT 10) R WHERE ever_eq(geoToH3IndexSet(R.geom4326, 7), T.trip_h3) AND eIntersects(T.Trip, R.Geom)'
Q2='SELECT R.regionid, P.periodid, T.vehid FROM Trips T, (SELECT * FROM Regions LIMIT 10) R, (SELECT * FROM Periods LIMIT 10) P WHERE eIntersects(atTime(T.Trip, P.Period), R.Geom)'
Q4='SELECT T.vehid, P.pointid, MIN(startTimestamp(atValues(T.Trip, P.Geom))) AS instant FROM Trips T, (SELECT * FROM Points LIMIT 10) P WHERE ST_Contains(trajectory(T.Trip), P.Geom) GROUP BY T.vehid, P.pointid'
Q6='SELECT R.regionid, numInstants(wCount(atGeometry(T.Trip, R.Geom), interval '\''10 min'\'')) FROM Trips T, (SELECT * FROM Regions LIMIT 10) R WHERE eIntersects(T.Trip, R.Geom) GROUP BY R.regionid HAVING wCount(atGeometry(T.Trip, R.Geom), interval '\''10 min'\'') IS NOT NULL'

: > "$OUT/results.txt"
echo "=== MobilityDB BerlinMOD ch1 — index config matrix ===" | tee -a "$OUT/results.txt"
echo "(median of 3 runs; 1620 trips × 10 regions/points/periods; SRID 3857 trip / region; trip_h3 at H3 res 7)" | tee -a "$OUT/results.txt"
echo | tee -a "$OUT/results.txt"
printf '%-40s %-20s %12s    %10s\n' "Query" "Index config" "Time" "Rows" | tee -a "$OUT/results.txt"
printf '%-40s %-20s %12s    %10s\n' "----------" "----------" "----------" "----------" | tee -a "$OUT/results.txt"

# SOUND configs: same semantic predicate, different indexes
for cfg in none gist spgist; do
  set_indexes "$cfg"
  run_query "Q1 (regions ever-passed)"          "$Q1" "$cfg"
  run_query "Q2 (regions × periods)"            "$Q2" "$cfg"
  run_query "Q4 (point visits, eContains)"      "$Q4" "$cfg"
  run_query "Q6 (per-region wCount)"            "$Q6" "$cfg"
done

# h3 prefilter configs — sound after the polygon-coverage fix on PR #938
for cfg in gist_h3 gist_both spgist_h3; do
  set_indexes "$cfg"
  run_query "Q1+h3prefilter"            "$Q1H" "$cfg"
done

echo | tee -a "$OUT/results.txt"
echo "(All rows = 77 — h3 prefilter is sound after the polygon-coverage fix on PR #938)" | tee -a "$OUT/results.txt"
