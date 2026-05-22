# Running Jupyter in Docker

PostgreSQL and Chrome/ChromeDriver run via Docker Compose. Jupyter runs in the `scraper` image so Selenium and DB access match production.

## Prerequisites

```powershell
docker compose up -d postgres
cp env.example .env   # edit passwords if needed
```

## Start Jupyter

**Windows PowerShell:**

```powershell
docker compose run --rm -p 8888:8888 `
  -v "${PWD}/WebScraping:/app/WebScraping" `
  -v "${PWD}/Storage:/app/Storage" `
  -v "${PWD}/logs:/app/logs" `
  -v "${PWD}/data:/app/data" `
  -e PYTHONPATH=/app/Storage `
  scraper bash -c "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --notebook-dir=/app --ServerApp.token='' --ServerApp.password='' --ServerApp.allow_origin='*' --ServerApp.disable_check_xsrf=True"
```

Or use the helper script:

```powershell
./docker/start_jupyter.sh
```

## Open the notebook

- File browser: http://localhost:8888/tree
- News collector: http://localhost:8888/notebooks/WebScraping/notebooks/NewsCollector-Staging.ipynb

Inside the container, `PGDBHOST=postgres`. On the host, use `PGDBHOST=localhost` in `.env` if you connect from outside Docker.

## Notes

- `PYTHONPATH=/app/Storage` is enough at startup; the notebook adds `WebScraping/src/selectors` without putting `WebScraping/src` on the path (avoids shadowing stdlib `selectors`).
- Scraper image includes Chromium and chromedriver; no separate ChromeDriver service.
- Press `Ctrl+C` in the terminal to stop Jupyter.

## Token auth (optional)

```powershell
$token = -join ((48..57) + (65..90) + (97..122) | Get-Random -Count 32 | ForEach-Object {[char]$_})
docker compose run --rm -p 8888:8888 `
  -v "${PWD}/WebScraping:/app/WebScraping" `
  -v "${PWD}/Storage:/app/Storage" `
  -e PYTHONPATH=/app/Storage `
  scraper bash -c "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --notebook-dir=/app --ServerApp.token='$token'"
Write-Host "http://localhost:8888/?token=$token"
```
