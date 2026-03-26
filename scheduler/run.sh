#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Gas Risk Daily Job
# ============================================================

JOB_START_TS="$(date -u)"

# ------------------------------------------------------------
# Reporting window configuration
# ------------------------------------------------------------

REPORT_LOOKBACK_DAYS="${REPORT_LOOKBACK_DAYS:-3}"

END_DATE="$(date -u +%Y-%m-%d)"
START_DATE="$(date -u -d "${REPORT_LOOKBACK_DAYS} days ago" +%Y-%m-%d)"

echo "================================================"
echo "Energy Atlas Job started at ${JOB_START_TS}"
echo "================================================"

# ------------------------------------------------------------
# Environment sanity checks
# ------------------------------------------------------------
: "${EIA_API_KEY:?Missing EIA_API_KEY}"

export PYTHONUNBUFFERED=1



# ============================================================
# Helper functions (safe, minimal abstraction)
# ============================================================

step() {
  echo
  echo "------------------------------------------------"
  echo "$1"
  echo "------------------------------------------------"
}

substep() {
  echo
  echo "  → $1"
}

run_cmd() {
  echo "    $ $*"
  "$@"
}

# ============================================================
# STEP 1 — Scrapy EBB ingestion (pipelines)
# ============================================================

step "[STEP 1] Scrapy: EBB pipeline ingestion"

cd /app/scrapy

# ------------------------------
# PIPELINE: Algonquin
# ------------------------------
substep "Pipeline: Algonquin — Capacity"
run_cmd scrapy crawl algonquin_capacity -a days_ago=3 -s LOG_LEVEL=INFO

echo "[STEP 1] Scrapy ingestion completed"


JOB_END_TS="$(date -u)"

echo
echo "================================================"
echo "Energy Atlas Daily Job finished at ${JOB_END_TS}"
echo "================================================"
