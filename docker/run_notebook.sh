#!/bin/bash
# Script to run the NewsCollector-Staging notebook in Docker

echo "Running NewsCollector-Staging.ipynb in Docker container..."
echo "=========================================="

docker-compose run --rm \
  -v "${PWD}/WebScraping:/app/WebScraping:ro" \
  -v "${PWD}/Storage:/app/Storage:ro" \
  -v "${PWD}/logs:/app/logs" \
  -v "${PWD}/data:/app/data" \
  scraper bash -c "
    cd /app && 
    pip install papermill -q && 
    PYTHONPATH=/app/WebScraping/src:/app/Storage:/app:\$PYTHONPATH \
    papermill \
      /app/WebScraping/notebooks/NewsCollector-Staging.ipynb \
      /app/data/NewsCollector-Staging-executed.ipynb \
      --log-output
  "

echo ""
echo "=========================================="
echo "Notebook execution completed!"
echo "Output saved to: data/NewsCollector-Staging-executed.ipynb"

