#!/bin/bash
# Script to start Jupyter Notebook server with token authentication

echo "Starting Jupyter Notebook server with token authentication..."
echo "=========================================="
echo ""

# Generate a random token
TOKEN=$(openssl rand -hex 32)

echo "Access Jupyter at: http://localhost:8888/?token=$TOKEN"
echo "Token: $TOKEN"
echo ""
echo "Press Ctrl+C to stop the server."
echo "=========================================="
echo ""

# Start Jupyter with token
docker-compose run --rm \
  -p 8888:8888 \
  -v "${PWD}/WebScraping:/app/WebScraping" \
  -v "${PWD}/Storage:/app/Storage" \
  -v "${PWD}/logs:/app/logs" \
  -v "${PWD}/data:/app/data" \
  -e PYTHONPATH=/app/WebScraping/src:/app/Storage:/app \
  scraper bash -c "
    cd /app && 
    pip install jupyter -q && 
    jupyter notebook \
      --ip=0.0.0.0 \
      --port=8888 \
      --no-browser \
      --allow-root \
      --notebook-dir=/app \
      --NotebookApp.token='$TOKEN'
  "

