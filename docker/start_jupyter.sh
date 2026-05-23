#!/bin/bash
# Script to start Jupyter Notebook server in Docker container with port forwarding

echo "Starting Jupyter Notebook server..."
echo "=========================================="
echo ""
echo "The Jupyter server will be accessible at: http://localhost:8888"
echo "Check the output below for the access token."
echo ""
echo "Press Ctrl+C to stop the server."
echo "=========================================="
echo ""

# Start Jupyter with proper configuration
docker-compose run --rm \
  -p 8888:8888 \
  -v "${PWD}/WebScraping:/app/WebScraping" \
  -v "${PWD}/Storage:/app/Storage" \
  -v "${PWD}/src:/app/src" \
  -v "${PWD}/notebooks:/app/notebooks" \
  -v "${PWD}/logs:/app/logs" \
  -v "${PWD}/data:/app/data" \
  -e PYTHONPATH=/app/src:/app/Storage:/app \
  scraper bash -c "
    cd /app && 
    pip install jupyter -q && 
    jupyter notebook \
      --ip=0.0.0.0 \
      --port=8888 \
      --no-browser \
      --allow-root \
      --notebook-dir=/app \
      --NotebookApp.token='' \
      --NotebookApp.password='' \
      --NotebookApp.allow_origin='*'
  "

