#!/bin/bash
# MobilityDB BerlinMOD full 17-query index-matrix benchmark.
PGBIN=/usr/local/pgsql/17/bin
DB=berlinmod_h3bench
OUT=/tmp/bench-results
mkdir -p "$OUT"

set_indexes() {
  local config="$1"
  $PGBIN/psql -p 5432 -d "$DB" -q <<SQL
DROP INDEX IF EXISTS trips_trip_gist_idx;
DROP INDEX IF EXISTS trips_trip_spgist_idx;
DROP INDEX IF EXISTS trips_trajectory_gist_idx;
DROP INDEX IF EXISTS trips_trajectory_spgist_idx;
DROP INDEX IF EXISTS trips_trip_h3_gist_idx;
DROP TABLE IF EXISTS execution_tests_explain;
SQL
  case "$config" in
    none) ;;
    gist)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_gist_idx       ON trips USING gist(trip);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_gist_idx ON trips USING gist(trajectory);"
      ;;
    spgist)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_spgist_idx       ON trips USING spgist(trip);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_spgist_idx ON trips USING spgist(trajectory);"
      ;;
    gist_h3)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_h3_gist_idx ON trips USING gist(trip_h3);"
      ;;
    gist_both)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_gist_idx       ON trips USING gist(trip);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_gist_idx ON trips USING gist(trajectory);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_h3_gist_idx   ON trips USING gist(trip_h3);"
      ;;
  esac
  $PGBIN/psql -p 5432 -d "$DB" -q -c "ANALYZE trips;"
}

run_matrix() {
  local config="$1"
  set_indexes "$config"
  echo "=== Config: $config ===" | tee -a "$OUT/full_results.txt"
  $PGBIN/psql -p 5432 -d "$DB" -tAc "SELECT berlinmod_R_queries(1, false);" 2>&1 \
    | grep "^INFO:" | sed -E 's|^INFO:  Query: (Q[0-9]+), Total Duration: ([0-9:.]+), Number of Rows: ([0-9]+)|\1\t\2\t\3|' \
    | tee -a "$OUT/full_results.txt"
  echo | tee -a "$OUT/full_results.txt"
}

: > "$OUT/full_results.txt"
echo "MobilityDB BerlinMOD 17-query × index-config matrix" | tee -a "$OUT/full_results.txt"
echo "(single run per cell; timings via berlinmod_R_queries function)" | tee -a "$OUT/full_results.txt"
echo "1620 trips × 100 vehicles × parameter sets 1, …" | tee -a "$OUT/full_results.txt"
echo | tee -a "$OUT/full_results.txt"

for cfg in none gist spgist; do
  run_matrix "$cfg"
done
