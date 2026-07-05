#!/bin/bash
# Start Jupyter with live mounts for notebooks/ and src/ (required for DataIngestion fixes).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

echo "Starting Jupyter Notebook server..."
echo "=========================================="
echo "Repo: ${REPO_ROOT}"
echo "Jupyter: http://localhost:8888/tree"
echo "Notebook: http://localhost:8888/notebooks/notebooks/ingestion/DataIngestion-Text.ipynb"
if [[ -f "${ENV_FILE}" ]]; then
  echo "Env file: ${ENV_FILE}"
else
  echo "Warning: .env not found — copy env.example to .env and set AWS/Postgres vars."
fi
echo "Press Ctrl+C to stop."
echo "=========================================="

# Container runs as appuser (uid 1000); ensure mounted logs dir is writable
mkdir -p "${REPO_ROOT}/logs"
chmod a+rwx "${REPO_ROOT}/logs" 2>/dev/null || true

ENV_ARGS=()
if [[ -f "${ENV_FILE}" ]]; then
  ENV_ARGS=(--env-from-file "${ENV_FILE}" -v "${ENV_FILE}:/app/.env:ro")
fi

docker-compose run --rm \
  --shm-size=2g \
  --security-opt seccomp=unconfined \
  -p 8888:8888 \
  -v "${REPO_ROOT}/config:/app/config:ro" \
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
    pip install notebook 'openai>=1.0.0' 'future>=0.18.3' -q &&
    python -m notebook \
      --ip=0.0.0.0 \
      --port=8888 \
      --no-browser \
      --allow-root \
      --notebook-dir=/app \
      --NotebookApp.token='' \
      --NotebookApp.password='' \
      --NotebookApp.base_url='/jupyter' \
      --NotebookApp.allow_origin='*'
  "
