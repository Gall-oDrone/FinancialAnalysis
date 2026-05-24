# Running Jupyter Notebook in Docker

> **Canonical guide:** [docs/jupyter-docker.md](docs/jupyter-docker.md)

## Quick start

```powershell
docker compose up -d postgres
.\docker\start_jupyter.ps1
```

Open http://localhost:8888/tree

## After code changes

| Change | What to do |
|--------|------------|
| Notebook cells | Re-run the cell |
| `Storage/*.py`, `src/**/*.py` | **Kernel → Restart**, re-run import/setup cells |
| `.env` | Stop Jupyter (Ctrl+C), run `.\docker\start_jupyter.ps1` again |

You do **not** need to restart Postgres or rebuild the image for ordinary notebook/Python edits.

## Notebooks

| Notebook | URL path under http://localhost:8888/notebooks/ |
|----------|--------------------------------------------------|
| Stock collector | `WebScraping/notebooks/StockCollector.ipynb` |
| News collector | `WebScraping/notebooks/NewsCollector-Staging.ipynb` |
| Data ingestion | `notebooks/ingestion/DataIngestion-Text.ipynb` |

## Stopping Jupyter

Press **Ctrl+C** in the terminal running `start_jupyter.ps1`, or:

```powershell
docker ps --filter "publish=8888" --format "{{.Names}}"
docker stop <container-name>
```

See [docs/jupyter-docker.md](docs/jupyter-docker.md) for Selenium/ChromeDriver notes, database env vars, and optional token auth.
