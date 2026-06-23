#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/workspace}"

JOB_START_TS="$(date -u)"

echo "================================================"
echo "Energy Atlas EIA report crawl started at ${JOB_START_TS}"
echo "================================================"

export PYTHONUNBUFFERED=1

step() {
  echo
  echo "------------------------------------------------"
  echo "$1"
  echo "------------------------------------------------"
}

run_cmd() {
  echo "    $ $*"
  "$@"
}

step "[STEP 1] EIA report crawlers"
run_cmd python "${WORKSPACE_ROOT}/scripts/eia/crawlers/run_all.py"

JOB_END_TS="$(date -u)"

echo
echo "================================================"
echo "Energy Atlas EIA report crawl finished at ${JOB_END_TS}"
echo "================================================"
