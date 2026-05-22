#!/bin/bash
# Start Jupyter with token auth. See docs/jupyter-docker.md

set -e

TOKEN=$(openssl rand -hex 32)

echo "Access Jupyter at: http://localhost:8888/?token=$TOKEN"
echo "Token: $TOKEN"
echo "Press Ctrl+C to stop."
echo "=========================================="

docker compose run --rm \
  -p 8888:8888 \
  -v "${PWD}/WebScraping:/app/WebScraping" \
  -v "${PWD}/Storage:/app/Storage" \
  -v "${PWD}/logs:/app/logs" \
  -v "${PWD}/data:/app/data" \
  -e PYTHONPATH=/app/Storage \
  scraper bash -c "
    cd /app &&
    pip install jupyter -q &&
    jupyter notebook \
      --ip=0.0.0.0 \
      --port=8888 \
      --no-browser \
      --notebook-dir=/app \
      --ServerApp.token='$TOKEN'
  "
