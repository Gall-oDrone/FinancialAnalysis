# Documentation

| Guide | Description |
|-------|-------------|
| [application-integration-workload.md](application-integration-workload.md) | EKS integration and workload design |
| [../eks/infrastructure/terraform/EKS-MIGRATION-PLAN.md](../eks/infrastructure/terraform/EKS-MIGRATION-PLAN.md) | Terraform env layout migration (`envs/` vs legacy `environments/`) |
| [../eks/infrastructure/README.md](../eks/infrastructure/README.md) | EKS/CloudFormation IaC entry point |
| [jupyter-docker.md](jupyter-docker.md) | Run Jupyter and notebooks in the Docker scraper container |
| [ETL_AND_TRANSFORMS.md](ETL_AND_TRANSFORMS.md) | ETL pipeline and transforms |
| [AGENTIC_AI_AND_BRANCHING.md](AGENTIC_AI_AND_BRANCHING.md) | Agentic AI enrichment and branching |
| [TOOLS_AND_MCP.md](TOOLS_AND_MCP.md) | CLI tools and MCP |
| [KUBERNETES-DEPLOYMENT-PLAN.md](KUBERNETES-DEPLOYMENT-PLAN.md) | Kubernetes deployment plan |
| [NEXT-STEPS.md](NEXT-STEPS.md) | Implementation checklist |
| [../DEPLOYMENT.md](../DEPLOYMENT.md) | Production deployment |
| [../PRODUCTION_PLAN.md](../PRODUCTION_PLAN.md) | Production roadmap |
| [../k8s/README.md](../k8s/README.md) | Kustomize manifests and overlays |
| [../WebScraping/README.md](../WebScraping/README.md) | Web scraping module layout |

## Docker layout

- **Repo root:** `docker-compose.yml`, `.dockerignore`, `env.example`
- **Services:** `services/scraper/Dockerfile`, `services/etl/Dockerfile`
- **`docker/`:** entrypoint, healthcheck, DB init SQL, Jupyter/run helpers

Database schema for new Postgres volumes is defined only in `docker/init-db/`.
