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

Uploads the current project to a remote server and starts the Docker Compose stack.

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
ARCHIVE_NAME="${PROJECT_NAME}-deploy.tgz"
TMP_ARCHIVE="$(mktemp "/tmp/${ARCHIVE_NAME}.XXXXXX")"

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

echo "==> Preparing deploy archive from ${REPO_ROOT}"
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
  -C "${REPO_ROOT}" \
  -czf "${TMP_ARCHIVE}" \
  .

echo "==> Ensuring remote directory ${REMOTE_DIR}"
remote "set -e;
  sudo mkdir -p '${REMOTE_DIR}';
  sudo chown -R '${USER_NAME}:${USER_NAME}' '${REMOTE_DIR}'"

echo "==> Uploading project archive"
copy_to_remote "${TMP_ARCHIVE}" "/tmp/${ARCHIVE_NAME}"

if [[ -n "${ENV_FILE}" ]]; then
  echo "==> Uploading env file ${ENV_FILE}"
  copy_to_remote "${ENV_FILE}" "/tmp/${PROJECT_NAME}.env"
fi

echo "==> Extracting project on remote host"
remote "set -euo pipefail;
  sudo rm -rf '${REMOTE_DIR}';
  sudo mkdir -p '${REMOTE_DIR}';
  sudo tar -xzf '/tmp/${ARCHIVE_NAME}' -C '${REMOTE_DIR}';
  if [ -f '/tmp/${PROJECT_NAME}.env' ]; then
    sudo mv '/tmp/${PROJECT_NAME}.env' '${REMOTE_DIR}/.env';
    sudo chmod 600 '${REMOTE_DIR}/.env';
  fi;
  sudo chown -R '${USER_NAME}:${USER_NAME}' '${REMOTE_DIR}';
  rm -f '/tmp/${ARCHIVE_NAME}'"

echo "==> Ensuring bind-mounted directories are writable"
remote "set -euo pipefail;
  sudo mkdir -p '${REMOTE_DIR}/data' '${REMOTE_DIR}/secrets' '${REMOTE_DIR}/config';
  sudo chown -R 1000:1000 '${REMOTE_DIR}/data' '${REMOTE_DIR}/secrets' '${REMOTE_DIR}/config';
  sudo chmod -R u+rwX '${REMOTE_DIR}/data' '${REMOTE_DIR}/secrets' '${REMOTE_DIR}/config'"

echo "==> Ensuring swap is available for Docker builds"
remote "set -euo pipefail;
  if ! sudo swapon --show | grep -q '/swapfile'; then
    if [ ! -f /swapfile ]; then
      sudo fallocate -l 4G /swapfile 2>/dev/null || sudo dd if=/dev/zero of=/swapfile bs=1M count=4096
      sudo chmod 600 /swapfile
      sudo mkswap /swapfile
    fi
    sudo swapon /swapfile
  fi"

echo "==> Starting Docker Compose stack"
remote "set -euo pipefail;
  cd '${REMOTE_DIR}';

  if ! command -v docker >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo apt-get install -y ca-certificates curl gnupg lsb-release
    sudo install -m 0755 -d /etc/apt/keyrings
    if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
      curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      sudo chmod a+r /etc/apt/keyrings/docker.gpg
      echo \
        \"deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        \$(. /etc/os-release && echo \$VERSION_CODENAME) stable\" | \
        sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
      sudo apt-get update
    fi
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo systemctl enable --now docker
  fi

  if sudo docker compose version >/dev/null 2>&1; then
    DC='sudo docker compose'
  elif command -v docker-compose >/dev/null 2>&1; then
    DC='sudo docker-compose'
  else
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo apt-get install -y docker-compose-plugin || sudo apt-get install -y docker-compose
    if sudo docker compose version >/dev/null 2>&1; then
      DC='sudo docker compose'
    elif command -v docker-compose >/dev/null 2>&1; then
      DC='sudo docker-compose'
    else
      echo 'ERROR: neither docker compose nor docker-compose is installed on the server' >&2
      exit 127
    fi
  fi

  \$DC -f docker/docker-compose.yml build
  \$DC -f docker/docker-compose.yml up -d
  \$DC -f docker/docker-compose.yml ps"

echo "==> Deployment complete: ${USER_NAME}@${HOST}:${REMOTE_DIR}"
