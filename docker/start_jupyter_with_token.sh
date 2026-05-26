#!/bin/bash
# Start Jupyter with token auth and live mounts for notebooks/ and src/.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

echo "Starting Jupyter Notebook server with token authentication..."
echo "=========================================="

TOKEN=$(openssl rand -hex 32)

echo "Access Jupyter at: http://localhost:8888/?token=$TOKEN"
echo "Token: $TOKEN"
echo "Repo: ${REPO_ROOT}"
echo "Press Ctrl+C to stop."
echo "=========================================="

ENV_ARGS=()
if [[ -f "${ENV_FILE}" ]]; then
  ENV_ARGS=(--env-file "${ENV_FILE}" -v "${ENV_FILE}:/app/.env:ro")
fi

docker compose run --rm \
  -p 8888:8888 \
  -v "${REPO_ROOT}/WebScraping:/app/WebScraping" \
  -v "${REPO_ROOT}/Storage:/app/Storage" \
  -v "${REPO_ROOT}/src:/app/src" \
  -v "${REPO_ROOT}/notebooks:/app/notebooks" \
  -v "${REPO_ROOT}/logs:/app/logs" \
  -v "${REPO_ROOT}/data:/app/data" \
  -e PYTHONPATH=/app/src:/app/Storage:/app \
  "${ENV_ARGS[@]}" \
  scraper bash -c "
    cd /app &&
    pip install notebook 'openai>=1.0.0' -q &&
    python -m notebook \
      --ip=0.0.0.0 \
      --port=8888 \
      --no-browser \
      --allow-root \
      --notebook-dir=/app \
      --NotebookApp.token='$TOKEN'
  "
