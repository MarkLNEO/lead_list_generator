#!/usr/bin/env bash

# Simple deployment helper for the lead list orchestrator.
# Usage: ./deploy_to_ec2.sh user@host [remote_dir] [--no-service]

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 user@host [remote_dir] [--no-service]" >&2
  exit 1
fi

HOST="$1"
shift

REMOTE_DIR="~/lead_list_generator"
CREATE_SERVICE=1
SSH_OPTS="${SSH_OPTS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-service)
      CREATE_SERVICE=0
      ;;
    *)
      REMOTE_DIR="$1"
      ;;
  esac
  shift
done

if [[ ! -f ".env.local" ]]; then
  echo ".env.local not found; create it before deploying." >&2
  exit 1
fi

echo ">>> Ensuring remote directory ${REMOTE_DIR}"
ssh ${SSH_OPTS:+$SSH_OPTS }"$HOST" "mkdir -p ${REMOTE_DIR}"

REMOTE_ABS=$(ssh ${SSH_OPTS:+$SSH_OPTS }"$HOST" "cd ${REMOTE_DIR} && pwd")
echo ">>> Resolved remote path: ${REMOTE_ABS}"

echo ">>> Syncing repository to ${HOST}:${REMOTE_DIR}"
RSYNC_SSH="ssh"
if [[ -n "$SSH_OPTS" ]]; then
  RSYNC_SSH="ssh $SSH_OPTS"
fi
rsync -az --delete \
  -e "$RSYNC_SSH" \
  --exclude '.git/' \
  --exclude 'runs/' \
  --exclude 'logs/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.mypy_cache/' \
  --exclude '.venv/' \
  ./ "$HOST:${REMOTE_DIR}/"

echo ">>> Creating virtual environment and upgrading pip"
ssh ${SSH_OPTS:+$SSH_OPTS }"$HOST" "cd ${REMOTE_DIR} && python3 -m venv .venv && source .venv/bin/activate && pip install --upgrade pip >/dev/null"

if [[ $CREATE_SERVICE -eq 1 ]]; then
  echo ">>> Installing lead queue systemd service"
  SERVICE_UNIT="[Unit]
Description=Lead List Queue Worker
After=network.target

[Service]
Type=simple
WorkingDirectory=${REMOTE_ABS}
Environment=PYTHONUNBUFFERED=1
ExecStart=${REMOTE_ABS}/.venv/bin/python ${REMOTE_ABS}/lead_pipeline.py --process-request-queue --request-limit 1
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
"
  ssh ${SSH_OPTS:+$SSH_OPTS }"$HOST" "printf '%s\n' \"$SERVICE_UNIT\" | sudo tee /etc/systemd/system/lead-list-queue.service >/dev/null"
  ssh ${SSH_OPTS:+$SSH_OPTS }"$HOST" "sudo systemctl daemon-reload && sudo systemctl enable --now lead-list-queue.service"
else
  echo ">>> Service installation skipped (--no-service)"
fi

echo "Deployment complete."
