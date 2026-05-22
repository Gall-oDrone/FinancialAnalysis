#!/bin/bash
# Start Jupyter in the scraper container. See docs/jupyter-docker.md

set -e

echo "Starting Jupyter Notebook server..."
echo "=========================================="
echo "Open: http://localhost:8888"
echo "Notebook: WebScraping/notebooks/NewsCollector-Staging.ipynb"
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
      --ServerApp.token='' \
      --ServerApp.password='' \
      --ServerApp.allow_origin='*' \
      --ServerApp.disable_check_xsrf=True
  "
