# Apache Airflow (Development)

This folder contains Helm values used by the GitHub Actions deployment workflow
to install or upgrade Apache Airflow in the development EKS cluster.

## Files

- `values-development.yaml`: baseline Airflow configuration with:
  - webserver, scheduler, and worker replicas
  - ALB ingress configuration
  - DAG git-sync settings

## Option B orchestration model

Airflow is the scheduler/orchestrator of record for daily workloads:

- DAG source: `dags/financial_analysis_daily.py`
- Schedule: once every 24 hours (`0 0 * * *`)
- Task runtime: Kubernetes pods in `financial-analysis-dev` namespace
  using the existing scraper/etl images
- Dependency graph:
  - `scrape_news -> transform_news`
  - `scrape_stocks -> transform_stocks`

The deploy workflow overrides `dags.gitSync.branch` with `${GITHUB_REF_NAME}`
to sync DAGs from the branch currently being deployed.

## Notes

- Replace the default admin credentials before using in shared environments.
- Update `ingress.web.hosts` with a real DNS name for your environment.
- If DAGs come from a private repository, configure git-sync credentials
  (for example, via Kubernetes Secret and chart values).
