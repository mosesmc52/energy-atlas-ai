#!/usr/bin/env bash
set -euo pipefail

USER_NAME="zilla"
REMOTE_ROOT="/opt"
PROJECT_NAME="energy-atlas-ai"
SSH_KEY=""
ENV_FILE=""
HOST=""

die() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  cat >&2 <<EOF
Usage: $0 <host> [--user zilla] [--remote-root /opt] [--ssh-key ~/.ssh/id_rsa] [--env-file .env.production]

Uploads the current project to a remote server, syncs code into the existing deploy
directory without deleting persistent data, and restarts the Docker Compose stack.

Examples:
  $0 203.0.113.10
  $0 203.0.113.10 --user zilla --remote-root /opt --ssh-key ~/.ssh/id_rsa --env-file .env.production
EOF
  exit 2
}

if [[ $# -lt 1 ]]; then
  usage
fi

HOST="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      USER_NAME="${2:-}"
      shift 2
      ;;
    --remote-root)
      REMOTE_ROOT="${2:-}"
      shift 2
      ;;
    --ssh-key)
      SSH_KEY="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    *)
      die "Unknown arg: $1"
      ;;
  esac
done

[[ -n "${HOST}" ]] || die "Host is required"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REMOTE_DIR="${REMOTE_ROOT}/${PROJECT_NAME}"
ARCHIVE_NAME="${PROJECT_NAME}-update.tgz"
TMP_ARCHIVE="$(mktemp "/tmp/${ARCHIVE_NAME}.XXXXXX")"
REMOTE_STAGE_BASE="/tmp/${PROJECT_NAME}-update"

SSH_OPTS=(
  -o StrictHostKeyChecking=accept-new
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=6
)

if [[ -n "${SSH_KEY}" ]]; then
  [[ -f "${SSH_KEY}" ]] || die "SSH key not found: ${SSH_KEY}"
  SSH_OPTS+=(-i "${SSH_KEY}" -o IdentitiesOnly=yes)
fi

if [[ -n "${ENV_FILE}" ]]; then
  [[ -f "${ENV_FILE}" ]] || die "Env file not found: ${ENV_FILE}"
fi

remote() {
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@${HOST}" "$@" </dev/null
}

copy_to_remote() {
  scp "${SSH_OPTS[@]}" "$1" "${USER_NAME}@${HOST}:$2" </dev/null
}

cleanup() {
  rm -f "${TMP_ARCHIVE}"
}
trap cleanup EXIT

echo "==> Preparing update archive from ${REPO_ROOT}"
COPYFILE_DISABLE=1 tar \
  --exclude=".git" \
  --exclude=".env" \
  --exclude=".env.*" \
  --exclude=".DS_Store" \
  --exclude=".venv" \
  --exclude="__pycache__" \
  --exclude=".pytest_cache" \
  --exclude=".mypy_cache" \
  --exclude=".ruff_cache" \
  --exclude="infra/terraform/.terraform" \
  --exclude="infra/terraform/*.tfstate" \
  --exclude="infra/terraform/*.tfstate.*" \
  --exclude="data" \
  --exclude="docker_volumes" \
  -C "${REPO_ROOT}" \
  -czf "${TMP_ARCHIVE}" \
  .

echo "==> Ensuring remote directory ${REMOTE_DIR}"
remote "set -euo pipefail;
  sudo mkdir -p '${REMOTE_DIR}';
  sudo chown -R '${USER_NAME}:${USER_NAME}' '${REMOTE_DIR}'"

echo "==> Uploading update archive"
copy_to_remote "${TMP_ARCHIVE}" "/tmp/${ARCHIVE_NAME}"

if [[ -n "${ENV_FILE}" ]]; then
  echo "==> Uploading env file ${ENV_FILE}"
  copy_to_remote "${ENV_FILE}" "/tmp/${PROJECT_NAME}.env"
fi

echo "==> Syncing code on remote host"
remote "set -euo pipefail;
  STAGE_DIR='${REMOTE_STAGE_BASE}-\$\$';
  rm -rf \"\${STAGE_DIR}\";
  mkdir -p \"\${STAGE_DIR}\";
  tar -xzf '/tmp/${ARCHIVE_NAME}' -C \"\${STAGE_DIR}\";
  rm -f '/tmp/${ARCHIVE_NAME}';

  if ! command -v rsync >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo apt-get install -y rsync
  fi

  rsync -a --delete \
    --exclude '.env' \
    --exclude 'data/' \
    --exclude 'docker_volumes/' \
    --exclude 'secrets/' \
    --exclude 'config/' \
    \"\${STAGE_DIR}/\" '${REMOTE_DIR}/';

  if [ -f '/tmp/${PROJECT_NAME}.env' ]; then
    mv '/tmp/${PROJECT_NAME}.env' '${REMOTE_DIR}/.env';
    chmod 600 '${REMOTE_DIR}/.env';
  fi;

  mkdir -p '${REMOTE_DIR}/data' '${REMOTE_DIR}/docker_volumes' '${REMOTE_DIR}/secrets' '${REMOTE_DIR}/config';
  rm -rf \"\${STAGE_DIR}\""

echo "==> Ensuring bind-mounted directories are writable"
remote "set -euo pipefail;
  sudo chown -R 1000:1000 '${REMOTE_DIR}/data' '${REMOTE_DIR}/docker_volumes' '${REMOTE_DIR}/secrets' '${REMOTE_DIR}/config';
  sudo chmod -R u+rwX '${REMOTE_DIR}/data' '${REMOTE_DIR}/docker_volumes' '${REMOTE_DIR}/secrets' '${REMOTE_DIR}/config'"

echo "==> Rebuilding and restarting Docker Compose stack"
remote "set -euo pipefail;
  cd '${REMOTE_DIR}';

  if sudo docker compose version >/dev/null 2>&1; then
    DC='sudo docker compose'
  elif command -v docker-compose >/dev/null 2>&1; then
    DC='sudo docker-compose'
  else
    echo 'ERROR: neither docker compose nor docker-compose is installed on the server' >&2
    exit 127
  fi

  \$DC -f docker/docker-compose.production.yml build
  \$DC -f docker/docker-compose.production.yml up -d
  \$DC -f docker/docker-compose.production.yml ps"

echo "==> Update complete: ${USER_NAME}@${HOST}:${REMOTE_DIR}"
