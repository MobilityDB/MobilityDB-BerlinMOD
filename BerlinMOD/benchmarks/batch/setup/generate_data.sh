#!/usr/bin/env bash
# BerlinMOD cross-platform data generator
#
# Produces the shared CSV corpus that every platform runner (bench_mbdb.sh,
# bench_mduck.sh, bench_mspark.sh) loads, so all three benchmark the identical
# data.  It orchestrates the canonical MobilityDB scripts — nothing bespoke:
#
#   1. berlinmod_datagenerator.sql : SELECT berlinmod_generate(scaleFactor := S)
#      generates the raw BerlinMOD trips/vehicles over the Brussels road network.
#   2. berlinmod_export.sql        : SELECT berlinmod_portability_export(DIR, SRID)
#      writes the cross-platform CSVs (geometries reprojected + SRID-tagged):
#        vehicles.csv trips.csv query_licences.csv query_instants.csv
#        query_points.csv query_periods.csv query_regions.csv
#
# PREREQUISITE (one-time, environment-specific): the generator needs the Brussels
# road-network graph (RoadSegments + Nodes) already loaded in the database,
# typically built with osm2pgrouting from an OSM extract — see the header of
# berlinmod_datagenerator.sql.  Pass --no-generate to skip step 1 and export from
# a database that already holds a generated BerlinMOD instance.
#
# Usage:
#   setup/generate_data.sh [options]
#
# Options:
#   --scalefactor S   BerlinMOD scale factor        (default: 0.005 = 1620 trips)
#   --output DIR      Directory for the CSV corpus   (default: <batch>/data)
#   --dbname NAME     Generation/source database     (default: berlinmod_gen)
#   --srid N          Output SRID for geometries      (default: 4326, WGS84)
#   --no-generate     Skip generation; export from an existing populated database

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BATCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BERLINMOD_DIR="$(cd "${BATCH_DIR}/../.." && pwd)"   # repo BerlinMOD/ (canonical scripts)

SCALEFACTOR=0.005
OUTPUT="${BATCH_DIR}/data"
DBNAME="berlinmod_gen"
SRID=4326
GENERATE=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scalefactor) SCALEFACTOR="$2"; shift 2 ;;
    --output)      OUTPUT="$2";      shift 2 ;;
    --dbname)      DBNAME="$2";      shift 2 ;;
    --srid)        SRID="$2";        shift 2 ;;
    --no-generate) GENERATE=false;   shift   ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

command -v psql >/dev/null 2>&1 || { echo "psql not found on PATH"; exit 1; }
mkdir -p "$OUTPUT"
# COPY … TO writes as the server user; give it a trailing slash and a writable dir.
OUTPUT="$(cd "$OUTPUT" && pwd)/"

_psql() { psql -d "$DBNAME" -v ON_ERROR_STOP=1 -q "$@"; }

if $GENERATE; then
  echo "=== Generating BerlinMOD (scale factor ${SCALEFACTOR}) in ${DBNAME} ==="
  createdb "$DBNAME" 2>/dev/null || true
  _psql -c "CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;"
  _psql -f "${BERLINMOD_DIR}/berlinmod_datagenerator.sql"
  _psql -c "SELECT berlinmod_generate(scaleFactor := ${SCALEFACTOR});"
fi

echo "=== Exporting cross-platform CSVs to ${OUTPUT} (SRID ${SRID}) ==="
_psql -f "${BERLINMOD_DIR}/berlinmod_export.sql"
_psql -c "SELECT berlinmod_portability_export('${OUTPUT}', ${SRID});"

echo "=== Done.  Corpus:"
ls -1 "${OUTPUT}"*.csv 2>/dev/null | sed 's|^|    |'
echo ""
echo "Point a runner at it, e.g.:"
echo "    bench/bench_mbdb.sh --data ${OUTPUT%/} --runs 1"
