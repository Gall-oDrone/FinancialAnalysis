# Start Jupyter with live mounts for notebooks/ and src/ (Windows / PowerShell).
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$EnvFile = Join-Path $RepoRoot ".env"

Write-Host "Starting Jupyter Notebook server..."
Write-Host "=========================================="
Write-Host "Repo: $RepoRoot"
Write-Host "Jupyter: http://localhost:8888/tree"
Write-Host "Notebook: http://localhost:8888/notebooks/notebooks/ingestion/DataIngestion-Text.ipynb"
if (Test-Path $EnvFile) {
    Write-Host "Env file: $EnvFile"
} else {
    Write-Host "Warning: .env not found — copy env.example to .env and set AWS/Postgres vars."
}
Write-Host "Press Ctrl+C to stop."
Write-Host "=========================================="

Set-Location $RepoRoot

$jupyterCmd = "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/app --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'"

$composeArgs = @(
    "compose", "run", "--rm",
    "-p", "8888:8888",
    "-v", "${RepoRoot}/WebScraping:/app/WebScraping",
    "-v", "${RepoRoot}/Storage:/app/Storage",
    "-v", "${RepoRoot}/src:/app/src",
    "-v", "${RepoRoot}/notebooks:/app/notebooks",
    "-v", "${RepoRoot}/logs:/app/logs",
    "-v", "${RepoRoot}/data:/app/data",
    "-e", "PYTHONPATH=/app/src:/app/Storage:/app"
)

if (Test-Path $EnvFile) {
    $composeArgs += @("--env-file", $EnvFile, "-v", "${EnvFile}:/app/.env:ro")
}

$composeArgs += @("scraper", "bash", "-c", $jupyterCmd)

docker @composeArgs
