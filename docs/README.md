# Documentation

| Guide | Description |
|-------|-------------|
| [jupyter-docker.md](jupyter-docker.md) | Run Jupyter and notebooks in the Docker scraper container |
| [../DEPLOYMENT.md](../DEPLOYMENT.md) | Production deployment |
| [../PRODUCTION_PLAN.md](../PRODUCTION_PLAN.md) | Production roadmap |
| [../WebScraping/README.md](../WebScraping/README.md) | Web scraping module layout |

## Docker layout

- **Repo root:** `Dockerfile`, `docker-compose.yml`, `.dockerignore`, `env.example`
- **`docker/`:** entrypoint, healthcheck, DB init SQL, Jupyter/run helpers

Database schema for new Postgres volumes is defined only in `docker/init-db/`.
