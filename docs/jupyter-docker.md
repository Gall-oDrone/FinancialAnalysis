# Running Jupyter in Docker

PostgreSQL and Chrome/ChromeDriver run via Docker Compose. Jupyter runs in the `scraper` image so Selenium and DB access match production.

## Prerequisites

```powershell
docker compose up -d postgres
cp env.example .env   # edit passwords if needed
```

## Start Jupyter

**Windows PowerShell (recommended):**

```powershell
./docker/start_jupyter.ps1
```

**Git Bash / Linux / macOS:**

```bash
./docker/start_jupyter.sh
```

These scripts mount `src/` and `notebooks/` from your repo. Without those mounts, Docker serves an **old notebook copy** baked into the image (you will see the `.str.startswith` / `datetime64` error again).

## Open the notebook

- File browser: http://localhost:8888/tree
- Data ingestion: http://localhost:8888/notebooks/notebooks/ingestion/DataIngestion-Text.ipynb

If you still see old code (`etl_process.filter_by_current_date()`), close the tab, reopen the link above, then **Kernel → Restart** and run cells 1, 2, and 7.

Inside the container, `PGDBHOST=postgres`. On the host, use `PGDBHOST=localhost` in `.env` if you connect from outside Docker.

## Notes

- Use `./docker/start_jupyter.ps1` (Windows) or `./docker/start_jupyter.sh` so `src/` and `notebooks/` are bind-mounted.
- `PYTHONPATH=/app/src:/app/Storage:/app` — do **not** add `WebScraping/src` (shadows stdlib `selectors`).
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
