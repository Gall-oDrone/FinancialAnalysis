# Start Jupyter with live mounts for notebooks/ and src/ (Windows / PowerShell).
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Write-Host "Starting Jupyter Notebook server..."
Write-Host "=========================================="
Write-Host "Repo: $RepoRoot"
Write-Host "Jupyter: http://localhost:8888/tree"
Write-Host "Notebook: http://localhost:8888/notebooks/notebooks/ingestion/DataIngestion-Text.ipynb"
Write-Host "Press Ctrl+C to stop."
Write-Host "=========================================="

Set-Location $RepoRoot

$jupyterCmd = "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/app --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'"

docker compose run --rm `
  -p 8888:8888 `
  -v "${RepoRoot}/WebScraping:/app/WebScraping" `
  -v "${RepoRoot}/Storage:/app/Storage" `
  -v "${RepoRoot}/src:/app/src" `
  -v "${RepoRoot}/notebooks:/app/notebooks" `
  -v "${RepoRoot}/logs:/app/logs" `
  -v "${RepoRoot}/data:/app/data" `
  -e PYTHONPATH=/app/src:/app/Storage:/app `
  scraper bash -c $jupyterCmd
