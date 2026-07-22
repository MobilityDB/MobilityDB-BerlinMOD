#!/usr/bin/env bash
# BerlinMOD timing runner — MobilityDB / PostgreSQL
#
# Loads data once, runs each query RUNS times, records wall-clock time per run,
# and writes a JSON file suitable for report.py.  When --queries is given only
# the selected queries are re-run and merged into an existing output file.
#
# Usage:
#   bench_mbdb.sh [options]
#
# Options:
#   --dbname  NAME     PostgreSQL database name  (default: berlinmod_bench)
#   --data    DIR      Directory containing the shared CSV files
#   --runs    N        Timed runs per query      (default: 3)
#   --queries RANGE    Comma/range query selector: "q04", "q04,q05", "q02-q05"
#                      Default: all queries in canonical order
#   --output  FILE     Path to write results JSON (default: results/mbdb.json)
#   --no-load          Skip data loading (reuse existing tables in DBNAME)
#   --tier    {0,1,2,3}  Index-acceleration tier (default: 3 = production-realistic)
#                      0 = no indexes (th3index prefilter scanned, common basis with Spark)
#                      1 = th3index columnar prefilter only (drop GiST/SP-GiST on trip)
#                      2 = native GiST/SP-GiST on trip only  (drop GiST on trip_h3)
#                      3 = both                              (default — current behaviour)
#                      See ../README.md "Three-tier index framework" for details.
#
# Requirements:
#   psql on PATH; MobilityDB installed and available for CREATE EXTENSION.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BMROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"   # repo BerlinMOD/ dir (canonical loaders)
QUERIES_SQL="${SCRIPT_DIR}/queries.sql"          # single canonical query source

# ── defaults ──────────────────────────────────────────────────────────────────
DBNAME="berlinmod_bench"
DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTPUT="${SCRIPT_DIR}/results/mbdb.json"
LOAD=true
QUERIES_ARG=""
TIER=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dbname)   DBNAME="$2";       shift 2 ;;
    --data)     DATADIR="$2";      shift 2 ;;
    --runs)     RUNS="$2";         shift 2 ;;
    --queries)  QUERIES_ARG="$2";  shift 2 ;;
    --output)   OUTPUT="$2";       shift 2 ;;
    --no-load)  LOAD=false;        shift   ;;
    --tier)     TIER="$2";         shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

case "$TIER" in
  0|1|2|3) ;;
  *) echo "Invalid --tier '$TIER' (must be 0, 1, 2, or 3)"; exit 1 ;;
esac

ALL_QUERIES=(q01 q02 q03 q04 q05 q06 q07 q08 qrt q09 q10 q11 q12 q13 q14 q15 q16 q17)

# Resolve QUERIES from QUERIES_ARG (comma/range syntax) or use all
resolve_queries() {
  local arg="$1"
  if [[ -z "$arg" || "$arg" == "all" ]]; then
    echo "${ALL_QUERIES[@]}"
    return
  fi
  local result=()
  IFS=',' read -ra tokens <<< "$arg"
  for token in "${tokens[@]}"; do
    token="${token// /}"
    if [[ "$token" == *-* && "$token" != qrt ]]; then
      local from to
      from="${token%%-*}"
      to="${token##*-}"
      # Normalize: bare numbers → q0N
      [[ "$from" =~ ^[0-9]+$ ]] && from=$(printf "q%02d" "$from")
      [[ "$to"   =~ ^[0-9]+$ ]] && to=$(printf "q%02d" "$to")
      local in_range=false
      for q in "${ALL_QUERIES[@]}"; do
        [[ "$q" == "$from" ]] && in_range=true
        $in_range && result+=("$q")
        [[ "$q" == "$to" ]] && in_range=false
      done
    else
      [[ "$token" =~ ^[0-9]+$ ]] && token=$(printf "q%02d" "$token")
      result+=("$token")
    fi
  done
  echo "${result[@]}"
}

QUERIES=($(resolve_queries "$QUERIES_ARG"))

_psql() { psql -d "$DBNAME" -q "$@"; }

# ── load data ─────────────────────────────────────────────────────────────────
if $LOAD; then
  echo "=== Creating database: $DBNAME ==="
  createdb "$DBNAME" 2>/dev/null || true
  echo "=== Loading data (canonical berlinmod_load + th3index setup) ==="
  _psql -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;"
  _psql -v ON_ERROR_STOP=1 -f "${BMROOT}/berlinmod_load.sql"
  _psql -v ON_ERROR_STOP=1 -c "SELECT berlinmod_load('${DATADIR}/', true);"
  _psql -v ON_ERROR_STOP=1 -f "${BMROOT}/berlinmod_th3index_setup.sql"
  echo "    done."
fi

# ── tier-specific index activation ───────────────────────────────────────────
# Tier 1 (th3index only)  → drop GiST/SP-GiST on the raw `trip` column.
# Tier 2 (native only)    → drop GiST on `trip_h3` so the th3 prefilter has no
#                           index acceleration (queries still pass the
#                           th3 predicate but the planner falls back).
# Tier 3 (combined)       → all indexes (no drops; loader's default).
case "$TIER" in
  0)
    echo "=== Tier 0: dropping all spatial indexes (th3index prefilter scanned, no index) ==="
    _psql -c "DROP INDEX IF EXISTS trips_trip_gist_idx;"
    _psql -c "DROP INDEX IF EXISTS trips_trip_spgist_idx;"
    _psql -c "DROP INDEX IF EXISTS trips_trip_h3_gist_idx;"
    _psql -c "ANALYZE Trips;"
    ;;
  1)
    echo "=== Tier 1: dropping native spatial indexes (keeping trip_h3 GiST) ==="
    _psql -c "DROP INDEX IF EXISTS trips_trip_gist_idx;"
    _psql -c "DROP INDEX IF EXISTS trips_trip_spgist_idx;"
    _psql -c "ANALYZE Trips;"
    ;;
  2)
    echo "=== Tier 2: dropping th3index GiST (keeping native trip GiST + SP-GiST) ==="
    _psql -c "DROP INDEX IF EXISTS trips_trip_h3_gist_idx;"
    _psql -c "ANALYZE Trips;"
    ;;
  3)
    echo "=== Tier 3: all indexes active (loader default) ==="
    ;;
esac

# ── version ───────────────────────────────────────────────────────────────────
MBDB_VER=$(_psql -t -c "SELECT mobilitydb_version();" | tr -d ' \n')
PG_VER=$(_psql -t -c "SELECT version();" | awk '{print $1, $2}' | tr -d '\n')
PLATFORM_VER="${MBDB_VER} on ${PG_VER}"

TRIP_COUNT=$(_psql -t -c "SELECT count(*) FROM Trips;" | tr -d ' \n')
VEH_COUNT=$(_psql  -t -c "SELECT count(*) FROM Vehicles;" | tr -d ' \n')

QUERIES_MSG="${QUERIES_ARG:-all}"
echo "=== Platform: ${PLATFORM_VER} ==="
echo "=== Dataset : ${VEH_COUNT} vehicles / ${TRIP_COUNT} trips ==="
echo "=== Runs    : ${RUNS} per query  (queries: ${QUERIES_MSG}) ==="
echo ""

TIMEFILE=$(mktemp)
QDIR=$(mktemp -d)
trap 'rm -f "$TIMEFILE"; rm -rf "$QDIR"' EXIT

# Split the single canonical queries.sql on its `-- @query <id>` markers.
for Q in "${QUERIES[@]}"; do
  awk -v s="-- @query ${Q}" '
    index($0, s)==1 { grab=1; next }
    grab && /^-- @query / { exit }
    grab { print }
  ' "$QUERIES_SQL" > "${QDIR}/${Q}.sql"
done

for Q in "${QUERIES[@]}"; do
  QFILE="${QDIR}/${Q}.sql"
  [[ -s "$QFILE" ]] || { echo "  [skip] ${Q} — not in queries.sql"; continue; }
  if ! ERR=$(_psql -v ON_ERROR_STOP=1 -o /dev/null -f "$QFILE" 2>&1); then
    echo "  [FAIL] ${Q} — ${ERR##*ERROR:  }"
    continue
  fi
  printf "  timing %-6s: " "$Q"
  for RUN in $(seq 1 "$RUNS"); do
    T0=$(date +%s%3N)
    _psql -o /dev/null -f "$QFILE" >/dev/null 2>&1
    T1=$(date +%s%3N)
    ELAPSED=$((T1 - T0))
    printf "%d " "$ELAPSED"
    echo "${Q} ${ELAPSED}" >> "$TIMEFILE"
  done
  echo "ms"
done

mkdir -p "$(dirname "$OUTPUT")"

# Merge new timings into existing output file (preserving unselected queries)
python3 - "$TIMEFILE" "$OUTPUT" "$PLATFORM_VER" "$TRIP_COUNT" "$VEH_COUNT" "$RUNS" "$TIER" <<'PYEOF'
import sys, json, collections, datetime, os

timefile, outfile, version, trips, vehicles, runs, tier = \
    sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])

QUERY_ORDER = ["q01","q02","q03","q04","q05","q06","q07","q08","qrt",
               "q09","q10","q11","q12","q13","q14","q15","q16","q17"]

# Load existing results to merge into
existing = {}
if os.path.exists(outfile):
    try:
        with open(outfile) as f:
            existing = json.load(f).get("queries", {})
    except Exception:
        pass

# Override with new timings
new_times = collections.defaultdict(list)
with open(timefile) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) == 2:
            new_times[parts[0]].append(int(parts[1]))
existing.update(new_times)

# Re-order by canonical order
ordered = {q: existing[q] for q in QUERY_ORDER if q in existing}
for q in existing:
    if q not in ordered:
        ordered[q] = existing[q]

result = {
    "platform": "mobilitydb",
    "version":  version,
    "tier":     tier,
    "data_vehicles": vehicles,
    "data_trips":    trips,
    "runs":          runs,
    "timestamp":     datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "queries":       ordered,
}
with open(outfile, "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")
print(f"\nResults written to {outfile}")
PYEOF
