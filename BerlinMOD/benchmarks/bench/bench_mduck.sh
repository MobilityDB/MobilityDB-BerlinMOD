#!/usr/bin/env bash
# BerlinMOD timing runner — MobilityDuck / DuckDB
#
# Loads data once into a file-based DuckDB database, runs each query RUNS times,
# and writes a JSON file suitable for report.py.  When --queries is given only
# the selected queries are re-run and merged into an existing output file.
#
# Usage:
#   bench_mduck.sh [options]
#
# Options:
#   --duckdb  PATH     Path to duckdb binary  (default: duckdb from PATH)
#   --data    DIR      Directory containing the shared CSV files
#   --runs    N        Timed runs per query   (default: 3)
#   --queries RANGE    Comma/range query selector: "q04", "q04,q05", "q02-q05"
#                      Default: all queries in canonical order
#   --output  FILE     Path to write results JSON (default: results/mduck.json)
#   --dbfile  PATH     DuckDB file to use  (default: /tmp/berlinmod_bench.duckdb)
#   --no-load          Skip data loading (reuse existing dbfile)
#   --tier    {1,2,3}  Index-acceleration tier (default: 3 = production-realistic)
#                      1 = th3index columnar prefilter only (no TRTREE on Trip)
#                      2 = native TRTREE on Trip only       (drop trip_h3 prefilter
#                                                            UDFs by NULL-ing the column)
#                      3 = both                             (default — current behaviour)
#                      See ../README.md "Three-tier index framework" for details.
#                      Tiers 2/3 require MobilityDuck PRs #143 + #144 in the loaded
#                      extension (pin via vcpkg_ports/meos/portfile.cmake or use
#                      the v1.0-preview-100pct release bundle).
#
# Requirements:
#   duckdb on PATH (or pass --duckdb); MobilityDuck extension loadable.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
DUCKDB="${DUCKDB:-duckdb}"
DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTPUT="${SCRIPT_DIR}/results/mduck.json"
DBFILE="/tmp/berlinmod_bench.duckdb"
LOAD=true
QUERIES_ARG=""
TIER=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duckdb)   DUCKDB="$2";       shift 2 ;;
    --data)     DATADIR="$2";      shift 2 ;;
    --runs)     RUNS="$2";         shift 2 ;;
    --queries)  QUERIES_ARG="$2";  shift 2 ;;
    --output)   OUTPUT="$2";       shift 2 ;;
    --dbfile)   DBFILE="$2";       shift 2 ;;
    --no-load)  LOAD=false;        shift   ;;
    --tier)     TIER="$2";         shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

case "$TIER" in
  1|2|3) ;;
  *) echo "Invalid --tier '$TIER' (must be 1, 2, or 3)"; exit 1 ;;
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

# Detect local vs community MobilityDuck build
DUCKDB_ABS="$(command -v "$DUCKDB" 2>/dev/null || echo "$DUCKDB")"
DUCKDB_DIR="$(cd "$(dirname "$DUCKDB_ABS")" 2>/dev/null && pwd || true)"
if [ -d "${DUCKDB_DIR}/extension/mobilityduck" ]; then
    MOBILITY_LOAD="LOAD mobilityduck;"
else
    MOBILITY_LOAD="INSTALL mobilitydb FROM community; LOAD mobilitydb;"
fi

_duck() { "$DUCKDB" "$DBFILE" -c "$1" 2>/dev/null; }
_duck_q() { "$DUCKDB" "$DBFILE" -noheader -list -c "$1" 2>/dev/null; }

# ── load data ─────────────────────────────────────────────────────────────────
if $LOAD; then
  echo "=== Loading data into: $DBFILE ==="
  rm -f "$DBFILE"
  LOAD_BODY="$(sed '/^SET VARIABLE DATADIR/d' "${BERLINMOD_DIR}/load_mduck.sql")"
  LOAD_SQL="${MOBILITY_LOAD} SET VARIABLE DATADIR='${DATADIR}/'; ${LOAD_BODY}"
  "$DUCKDB" "$DBFILE" -c "$LOAD_SQL"
  echo "    done."
fi

# ── tier-specific index activation ───────────────────────────────────────────
# Tier 1 (th3index only) → loader's default (no native TRTREE created).
# Tier 2 (native only)   → create TRTREE on Trip; NULL-out trip_h3 to defeat
#                          the th3 prefilter UDFs in the queries.
# Tier 3 (combined)      → create TRTREE on Trip; keep trip_h3 active.
#
# Tiers 2/3 require MobilityDuck PRs #143 (multi-entry TRTREE) and #144
# (constant-geometry pushdown) to be present in the loaded extension.  If the
# CREATE INDEX errors out, see ../README.md "MobilityDuck TRTREE dependency".
case "$TIER" in
  1)
    echo "=== Tier 1: th3index prefilter only (no native TRTREE) ==="
    ;;
  2)
    echo "=== Tier 2: native TRTREE on Trip; clearing trip_h3 prefilter ==="
    "$DUCKDB" "$DBFILE" -c "${MOBILITY_LOAD} CREATE INDEX IF NOT EXISTS trips_trtree_idx ON Trips USING TRTREE (trip);" || \
      { echo "    !!! TRTREE index creation failed — extension does not yet expose TRTREE"; \
        echo "    !!! See README.md 'MobilityDuck TRTREE dependency'."; }
    "$DUCKDB" "$DBFILE" -c "${MOBILITY_LOAD} UPDATE Trips SET trip_h3 = NULL;"
    ;;
  3)
    echo "=== Tier 3: th3index + native TRTREE (production-realistic) ==="
    "$DUCKDB" "$DBFILE" -c "${MOBILITY_LOAD} CREATE INDEX IF NOT EXISTS trips_trtree_idx ON Trips USING TRTREE (trip);" || \
      { echo "    !!! TRTREE index creation failed — falling back to Tier 1 acceleration"; \
        echo "    !!! See README.md 'MobilityDuck TRTREE dependency'."; }
    ;;
esac

# ── version ───────────────────────────────────────────────────────────────────
MDUCK_VER=$(_duck_q "SELECT mobilityduck_version();" 2>/dev/null | head -1 || echo "unknown")
DUCK_VER=$(_duck_q  "SELECT version();" 2>/dev/null | head -1 || echo "unknown")
PLATFORM_VER="${MDUCK_VER} on DuckDB ${DUCK_VER}"

TRIP_COUNT=$(_duck_q "SELECT count(*) FROM Trips;"    || echo 0)
VEH_COUNT=$( _duck_q "SELECT count(*) FROM Vehicles;" || echo 0)

QUERIES_MSG="${QUERIES_ARG:-all}"
echo "=== Platform: ${PLATFORM_VER} ==="
echo "=== Dataset : ${VEH_COUNT} vehicles / ${TRIP_COUNT} trips ==="
echo "=== Runs    : ${RUNS} per query  (queries: ${QUERIES_MSG}) ==="
echo ""

TIMEFILE=$(mktemp)
trap 'rm -f "$TIMEFILE"' EXIT

for Q in "${QUERIES[@]}"; do
  QFILE="${BERLINMOD_DIR}/${Q}.sql"
  [[ -f "$QFILE" ]] || { echo "  [skip] ${Q} — SQL file not found"; continue; }
  QSQL=$(grep -v '^\s*--' "$QFILE" | tr '\n' ' ')
  printf "  timing %-6s: " "$Q"
  for RUN in $(seq 1 "$RUNS"); do
    T0=$(date +%s%3N)
    "$DUCKDB" "$DBFILE" -c "${MOBILITY_LOAD} SET search_path='portable,main'; ${QSQL}" \
      > /dev/null 2>&1 || true
    T1=$(date +%s%3N)
    ELAPSED=$((T1 - T0))
    printf "%d " "$ELAPSED"
    echo "${Q} ${ELAPSED}" >> "$TIMEFILE"
  done
  echo "ms"
done

mkdir -p "$(dirname "$OUTPUT")"

# Merge new timings into existing output file (preserving unselected queries)
python3 - "$TIMEFILE" "$OUTPUT" "$PLATFORM_VER" "$TRIP_COUNT" "$VEH_COUNT" "$RUNS" <<'PYEOF'
import sys, json, collections, datetime, os

timefile, outfile, version, trips, vehicles, runs = \
    sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])

QUERY_ORDER = ["q01","q02","q03","q04","q05","q06","q07","q08","qrt",
               "q09","q10","q11","q12","q13","q14","q15","q16","q17"]

existing = {}
if os.path.exists(outfile):
    try:
        with open(outfile) as f:
            existing = json.load(f).get("queries", {})
    except Exception:
        pass

new_times = collections.defaultdict(list)
with open(timefile) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) == 2:
            ms = int(parts[1])
            if ms > 0:
                new_times[parts[0]].append(ms)
existing.update(new_times)

ordered = {q: existing[q] for q in QUERY_ORDER if q in existing}
for q in existing:
    if q not in ordered:
        ordered[q] = existing[q]

result = {
    "platform": "mobilityduck",
    "version":  version,
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
