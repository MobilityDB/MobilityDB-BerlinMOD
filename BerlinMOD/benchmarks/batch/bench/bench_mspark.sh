#!/usr/bin/env bash
# BerlinMOD timing runner — MobilitySpark / Apache Spark
#
# Builds the fat JAR if necessary, then runs BerlinMODBench in a single
# Spark session (avoiding JVM startup overhead per query).  BerlinMODBench
# writes a JSON results file directly.
#
# Usage:
#   bench_mspark.sh [options]
#
# Options:
#   --spark-submit PATH  Path to spark-submit binary (default: spark-submit from PATH)
#   --data   DIR         Directory containing the shared CSV files
#   --runs   N           Timed runs per query  (default: 3)
#   --quick              Run each query once (--runs 1); useful for crash-safety checks
#   --queries RANGE      Page-range query selector: "3", "2-5", "q02-q05", "qrt", "q04,qrt"
#                        Default: all queries in canonical order
#   --output FILE        Path to write results JSON (default: results/mspark.json)
#   --jar    PATH        Pre-built fat JAR (skip mvn build)
#
# Tier:
#   Spark always runs the BerlinMOD benchmark at TIER 1 (the th3index
#   columnar prefilter is the only common-denominator acceleration that
#   Spark can use; native spatial indexes are PG/Duck-only).  See
#   ../README.md "Three-tier index framework" for context.  No --tier
#   flag is exposed on this runner.
#
# NxN mitigations for the Trips × Trips queries (Q5, Q6, Q10, Q16):
#   The four queries use Spark-optimised UNNEST + equi-join variants
#   (`q05_spark.sql`, etc.) that BerlinMODBench auto-prefers over the
#   portable form when present.  PG / DuckDB always run the portable
#   `<query>.sql` (they have spatial indexes; the portable form is
#   already optimal there).  See ../README.md "NxN mitigations on
#   Spark" for the rewrite strategy.
#
# Requirements:
#   spark-submit on PATH (or --spark-submit); Java 11/17/21; Maven for building.
#   Run setup/install_spark.sh if spark-submit is missing.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTPUT="${SCRIPT_DIR}/results/mspark.json"
QUERIES=""
JAR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spark-submit) SPARK_SUBMIT="$2"; shift 2 ;;
    --data)         DATADIR="$2";      shift 2 ;;
    --runs)         RUNS="$2";         shift 2 ;;
    --quick)        RUNS=1;            shift   ;;
    --queries)      QUERIES="$2";      shift 2 ;;
    --output)       OUTPUT="$2";       shift 2 ;;
    --jar)          JAR="$2";          shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── verify spark-submit ───────────────────────────────────────────────────────
if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
  echo "ERROR: spark-submit not found."
  echo "  Run: ${REPO_ROOT}/setup/install_spark.sh"
  echo "  or pass: --spark-submit /opt/spark/bin/spark-submit"
  exit 1
fi

# ── build fat JAR if needed ───────────────────────────────────────────────────
if [[ -z "$JAR" ]]; then
  JAR=$(ls "${REPO_ROOT}/target/"*-spark.jar 2>/dev/null | head -1 || true)
fi

if [[ -z "$JAR" ]]; then
  echo "=== No fat JAR found — building with mvn package ==="
  if ! command -v mvn >/dev/null 2>&1; then
    echo "ERROR: mvn not found.  Install Maven:"
    echo "  sudo apt-get install -y maven"
    exit 1
  fi
  (cd "$REPO_ROOT" && mvn package -DskipTests -q)
  JAR=$(ls "${REPO_ROOT}/target/"*-spark.jar | head -1)
fi
echo "=== Using JAR: $JAR ==="

LIBMEOS_DIR="${LIBMEOS_DIR:-/usr/local/lib}"

mkdir -p "$(dirname "$OUTPUT")"

# Suppress core dumps: a JVM crash produces a 3-5 GB core file that can OOM WSL2.
ulimit -c 0

QUERIES_MSG="${QUERIES:-all}"
echo "=== Running BerlinMODBench (${RUNS} runs/query, queries=${QUERIES_MSG}) on MobilitySpark ==="
# --driver-memory 6g: each BerlinMOD trip row is ~36 KB hex-WKB; cross-join queries (Q2, Q4)
# hold ~1 GB of trip strings in heap simultaneously.  A 6 g heap gives the GC enough headroom
# to avoid spilling to off-heap and prevents WSL2 OOM kills when queries run back-to-back.
#
# --conf spark.sql.autoBroadcastJoinThreshold=200m: BerlinMOD's small dimension
# tables (Vehicles, QueryPoints, QueryRegions, QueryInstants, QueryPeriods,
# QueryLicences) are all under 200 KB; broadcasting them is always profitable
# but the default 10 MB threshold occasionally falls back to shuffle when
# Catalyst's size estimate is conservative (e.g. on a cached relational plan).
# 200 m makes broadcast the deterministic choice for these dim tables.
#
# --conf spark.sql.adaptive.enabled=true / .skewJoin.enabled: Adaptive Query
# Execution can convert sort-merge joins to broadcast joins at runtime once
# actual table sizes are known, and rebalance skewed join keys.  Useful for
# Q10/Q11/Q12 where one side of a Trips×Trips Cartesian-style join has a
# small materialised intermediate (e.g. WITH Temp AS (...)).
"$SPARK_SUBMIT" \
  --class org.mobilitydb.spark.demo.BerlinMODBench \
  --master "local[2]" \
  --driver-memory 6g \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=${LIBMEOS_DIR} -Dlog4j.logger.org.apache=WARN -Dberlinmod.bench.query.cap.ms=1013250" \
  --conf "spark.sql.autoBroadcastJoinThreshold=200m" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  "$JAR" \
  "$DATADIR" \
  "$OUTPUT" \
  "$RUNS" \
  ${QUERIES:+"$QUERIES"}

echo "=== Results written to ${OUTPUT} ==="
