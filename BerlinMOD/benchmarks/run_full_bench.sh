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
DROP INDEX IF EXISTS trips_trip_mgist_idx;
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
    spgist|spgist_quadtree)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_spgist_idx       ON trips USING spgist(trip tgeompoint_quadtree_ops);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_spgist_idx ON trips USING spgist(trajectory);"
      ;;
    spgist_kdtree)
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trip_spgist_idx       ON trips USING spgist(trip tgeompoint_kdtree_ops);"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_spgist_idx ON trips USING spgist(trajectory);"
      ;;
    mest_mrtree_4|mest_mrtree_8|mest_equisplit_4|mest_equisplit_8)
      local n=${config##*_}
      $PGBIN/psql -p 5432 -d "$DB" -q -c "LOAD '\$libdir/libMobilityDB-1.4'; CREATE INDEX trips_trip_mgist_idx ON trips USING mgist(trip tgeompoint_mrtree_equisplit_ops (num_boxes = $n));"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_gist_idx ON trips USING gist(trajectory);"
      ;;
    mest_mquadtree_4|mest_mquadtree_8)
      local n=${config##*_}
      $PGBIN/psql -p 5432 -d "$DB" -q -c "LOAD '\$libdir/libMobilityDB-1.4'; CREATE INDEX trips_trip_mgist_idx ON trips USING mspgist(trip tgeompoint_mquadtree_equisplit_ops (num_boxes = $n));"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_gist_idx ON trips USING gist(trajectory);"
      ;;
    mest_mkdtree_4|mest_mkdtree_8)
      local n=${config##*_}
      $PGBIN/psql -p 5432 -d "$DB" -q -c "LOAD '\$libdir/libMobilityDB-1.4'; CREATE INDEX trips_trip_mgist_idx ON trips USING mspgist(trip tgeompoint_mkdtree_equisplit_ops (num_boxes = $n));"
      $PGBIN/psql -p 5432 -d "$DB" -q -c "CREATE INDEX trips_trajectory_gist_idx ON trips USING gist(trajectory);"
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
  $PGBIN/psql -p 5432 -d "$DB" -tAc "LOAD '\$libdir/libMobilityDB-1.4'; SELECT berlinmod_R_queries(1, false);" 2>&1 \
    | grep "^INFO:" | sed -E 's|^INFO:  Query: (Q[0-9]+), Total Duration: ([0-9:.]+), Number of Rows: ([0-9]+)|\1\t\2\t\3|' \
    | tee -a "$OUT/full_results.txt"
  echo | tee -a "$OUT/full_results.txt"
}

: > "$OUT/full_results.txt"
echo "MobilityDB BerlinMOD 17-query × index-config matrix" | tee -a "$OUT/full_results.txt"
echo "(single run per cell; timings via berlinmod_R_queries function)" | tee -a "$OUT/full_results.txt"
echo "1620 trips × 100 vehicles × parameter sets 1, …" | tee -a "$OUT/full_results.txt"
echo | tee -a "$OUT/full_results.txt"

: "${CONFIGS:=none gist spgist_quadtree spgist_kdtree}"
for cfg in $CONFIGS; do
  run_matrix "$cfg"
done
