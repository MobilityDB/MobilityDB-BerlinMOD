#!/usr/bin/env bash
# BerlinMOD cross-platform benchmark — main entry point
#
# Runs the BerlinMOD portable SQL queries on all three platforms and generates
# a markdown performance report with machine specifications.
#
# Usage:
#   ./berlinmod/bench/bench.sh [options]
#
# Options:
#   --data   DIR         Shared CSV data directory (default: berlinmod/data)
#                        Use setup/generate_data.sh to generate larger datasets.
#   --runs   N           Timed runs per query per platform (default: 3)
#   --output DIR         Directory for results JSON + report (default: berlinmod/bench/results)
#   --dbname NAME        PostgreSQL database name (default: berlinmod_bench)
#   --duckdb PATH        DuckDB binary path (default: duckdb from PATH)
#   --spark-submit PATH  spark-submit binary (default: spark-submit from PATH)
#   --skip-mbdb          Skip MobilityDB
#   --skip-mduck         Skip MobilityDuck
#   --skip-mspark        Skip MobilitySpark
#
# Quick start (all three platforms, synthetic data):
#   ./berlinmod/bench/bench.sh
#
# With real BerlinMOD data:
#   ./setup/generate_data.sh --scalefactor 0.005 --output berlinmod/data
#   ./berlinmod/bench/bench.sh --data berlinmod/data
#
# Output:
#   results/mbdb.json, results/mduck.json, results/mspark.json  — raw timings
#   results/report.md                                            — markdown table

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTDIR="${SCRIPT_DIR}/results"
DBNAME="berlinmod_bench"
DUCKDB="${DUCKDB:-duckdb}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
RUN_MBDB=true
RUN_MDUCK=true
RUN_MSPARK=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data)         DATADIR="$2";       shift 2 ;;
    --runs)         RUNS="$2";          shift 2 ;;
    --output)       OUTDIR="$2";        shift 2 ;;
    --dbname)       DBNAME="$2";        shift 2 ;;
    --duckdb)       DUCKDB="$2";        shift 2 ;;
    --spark-submit) SPARK_SUBMIT="$2";  shift 2 ;;
    --skip-mbdb)    RUN_MBDB=false;     shift   ;;
    --skip-mduck)   RUN_MDUCK=false;    shift   ;;
    --skip-mspark)  RUN_MSPARK=false;   shift   ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

mkdir -p "$OUTDIR"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  BerlinMOD Cross-Platform Benchmark                  ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Data   : $DATADIR"
echo "Runs   : $RUNS per query"
echo "Output : $OUTDIR"
echo ""

# ── MobilityDB ────────────────────────────────────────────────────────────────
if $RUN_MBDB; then
  if ! command -v psql >/dev/null 2>&1; then
    echo "[SKIP] MobilityDB — psql not found"
  else
    echo "──────────────────────────────────────────────────────"
    echo " MobilityDB / PostgreSQL"
    echo "──────────────────────────────────────────────────────"
    "${SCRIPT_DIR}/bench_mbdb.sh" \
      --dbname "$DBNAME" \
      --data   "$DATADIR" \
      --runs   "$RUNS" \
      --output "${OUTDIR}/mbdb.json"
  fi
fi

# ── MobilityDuck ──────────────────────────────────────────────────────────────
if $RUN_MDUCK; then
  if ! command -v "$DUCKDB" >/dev/null 2>&1; then
    echo "[SKIP] MobilityDuck — duckdb not found (pass --duckdb PATH)"
  else
    echo ""
    echo "──────────────────────────────────────────────────────"
    echo " MobilityDuck / DuckDB"
    echo "──────────────────────────────────────────────────────"
    "${SCRIPT_DIR}/bench_mduck.sh" \
      --duckdb "$DUCKDB" \
      --data   "$DATADIR" \
      --runs   "$RUNS" \
      --output "${OUTDIR}/mduck.json"
  fi
fi

# ── MobilitySpark ─────────────────────────────────────────────────────────────
if $RUN_MSPARK; then
  if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
    echo ""
    echo "[SKIP] MobilitySpark — spark-submit not found"
    echo "       Run: ${REPO_ROOT}/setup/install_spark.sh"
  else
    echo ""
    echo "──────────────────────────────────────────────────────"
    echo " MobilitySpark / Apache Spark"
    echo "──────────────────────────────────────────────────────"
    "${SCRIPT_DIR}/bench_mspark.sh" \
      --spark-submit "$SPARK_SUBMIT" \
      --data         "$DATADIR" \
      --runs         "$RUNS" \
      --output       "${OUTDIR}/mspark.json"
  fi
fi

# ── Report ────────────────────────────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────────────────"
echo " Generating report"
echo "──────────────────────────────────────────────────────"
REPORT="${OUTDIR}/report.md"
python3 "${SCRIPT_DIR}/report.py" \
  --results "$OUTDIR" \
  --output  "$REPORT"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Done.  Report: ${REPORT}"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
cat "$REPORT"
