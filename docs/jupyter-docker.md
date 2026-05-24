# Running Jupyter in Docker

PostgreSQL and Chromium/ChromeDriver run via Docker Compose. Jupyter runs in the `scraper` image so Selenium and database access match production.

## Prerequisites

```powershell
docker compose up -d postgres
cp env.example .env   # set POSTGRES_PASSWORD / PGDBPASS to the same value
```

Ensure `.env` uses the Compose database (not legacy localhost-only names unless you run Postgres locally):

| Variable | Docker Jupyter | Host-only scripts |
|----------|----------------|-------------------|
| `PGDBHOST` | `postgres` (set by `start_jupyter.ps1`) | `localhost` |
| `PGDBNAME` | `financial_db` | `financial_db` or `cryptostocks` |
| `PGDBUSER` | `financial_user` | `financial_user` or `postgres` |
| `PGDBPASS` | same as `POSTGRES_PASSWORD` | your password |

## Quick workflow

From the repo root:

```powershell
docker compose up -d postgres
.\docker\start_jupyter.ps1
```

Then in the browser:

- File browser: http://localhost:8888/tree
- Stock collector: http://localhost:8888/notebooks/WebScraping/notebooks/StockCollector.ipynb
- News collector: http://localhost:8888/notebooks/WebScraping/notebooks/NewsCollector-Staging.ipynb
- Data ingestion: http://localhost:8888/notebooks/notebooks/ingestion/DataIngestion-Text.ipynb

In Jupyter: **Kernel → Restart**, then re-run setup cells (imports, `pg_conn`, Chrome driver, scrape).

## Start Jupyter

**Windows PowerShell (recommended):**

```powershell
.\docker\start_jupyter.ps1
```

**Git Bash / Linux / macOS:**

```bash
./docker/start_jupyter.sh
```

These scripts bind-mount `WebScraping/`, `Storage/`, `src/`, and `notebooks/` from your repo. Without those mounts, Docker serves an **old copy** of notebooks and code baked into the image.

`start_jupyter.ps1` also sets `PGDBHOST=postgres` and loads `.env` via `--env-file`.

## Restart Jupyter (without restarting Postgres)

You do **not** need to restart Docker or Postgres for most code edits.

1. Stop Jupyter: **Ctrl+C** in the terminal running `start_jupyter.ps1`, or:

   ```powershell
   docker ps --filter "publish=8888" --format "{{.Names}}"
   docker stop <container-name>
   ```

2. Start again:

   ```powershell
   .\docker\start_jupyter.ps1
   ```

3. In the notebook: **Kernel → Restart** and re-run setup cells.

Postgres can stay up: `docker compose up -d postgres`.

## When to restart what

| Change | Action |
|--------|--------|
| Notebook cell edits | Re-run the cell (or **Kernel → Restart** if imports look stale) |
| `Storage/pgConn.py`, `src/`, other imported `.py` files | **Kernel → Restart**, then re-run cells that import them |
| `.env` credentials | Restart Jupyter container (`Ctrl+C` → `start_jupyter.ps1`) |
| `Dockerfile` / `requirements.txt` | `docker compose build scraper` then start Jupyter again |
| Postgres data / compose services | `docker compose down` (optional) → `docker compose up -d postgres` |

## Selenium (ChromeDriver)

Inside the container, use the system Chromium and chromedriver (versions are matched in the image):

- `CHROME_BIN=/usr/bin/chromium`
- `CHROMEDRIVER_PATH=/usr/bin/chromedriver`

Notebooks such as `StockCollector.ipynb` prefer the system driver when those paths exist; `webdriver_manager` is only a fallback on the host.

Do **not** rely on `ChromeDriverManager().install()` alone in Docker—it may download a driver that does not match container Chromium.

## PostgreSQL from notebooks

Use environment-based connection (do not hardcode `dbname="cryptostocks"` in Docker unless that database exists):

```python
import pgConn
import PostgresSQL_table_queries

pg_conn = pgConn.PgConn(tablename=PostgresSQL_table_queries.HISTORICAL_CRYPTO_STOCKS_TABLE_NAME)
pg_conn.init_db(PostgresSQL_table_queries.HISTORICAL_CRYPTO_STOCKS_TABLE_QUERY)
```

After the init cell, confirm output includes `Connection to the database successful!` and the DSN shows `host=postgres` and your `PGDBNAME` when running in Docker.

If a scrape run ends in `finally` and calls `close_connection()`, re-run the `pg_conn` init cell before saving again.

## Notes

- `PYTHONPATH=/app/src:/app/Storage:/app` — do **not** add `WebScraping/src` (shadows stdlib `selectors`).
- Scraper image includes Chromium and chromedriver; no separate ChromeDriver service.
- Press **Ctrl+C** in the terminal to stop Jupyter.
- Smoke test (DB + Selenium): `docker compose run --rm scraper python /app/docker/notebook_smoke_test.py`

## Token auth (optional)

```powershell
$token = -join ((48..57) + (65..90) + (97..122) | Get-Random -Count 32 | ForEach-Object {[char]$_})
docker compose run --rm -p 8888:8888 `
  -v "${PWD}/WebScraping:/app/WebScraping" `
  -v "${PWD}/Storage:/app/Storage" `
  -v "${PWD}/src:/app/src" `
  -e PYTHONPATH=/app/src:/app/Storage:/app `
  -e PGDBHOST=postgres `
  --env-file .env `
  scraper bash -c "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --notebook-dir=/app --ServerApp.token='$token'"
Write-Host "http://localhost:8888/?token=$token"
```
