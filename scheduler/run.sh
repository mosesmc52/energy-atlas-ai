#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/workspace}"

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
# STEP 1 — NOAA weather aggregation
# ============================================================

step "[STEP 1] NOAA weather aggregation"
run_cmd python "${WORKSPACE_ROOT}/scripts/noaa/download_and_aggregate_ghcnd.py"

# ============================================================
# STEP 2 — EIA crawlers
# ============================================================

step "[STEP 2] EIA crawlers"
run_cmd python "${WORKSPACE_ROOT}/scripts/eia/crawlers/run_all.py"

# ============================================================
# STEP 3 — Pipeline projects ingestion
# ============================================================

step "[STEP 3] Pipeline projects ingestion"
run_cmd python "${WORKSPACE_ROOT}/scripts/eia/ng/pipelines/ingest_pipeline_projects.py"

# ============================================================
# STEP 4 — State-to-state capacity ingestion
# ============================================================

step "[STEP 4] State-to-state capacity ingestion"
run_cmd python "${WORKSPACE_ROOT}/scripts/eia/ng/pipelines/ingest_state_to_state_capacity.py"


JOB_END_TS="$(date -u)"

echo
echo "================================================"
echo "Energy Atlas Daily Job finished at ${JOB_END_TS}"
echo "================================================"
