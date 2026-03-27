# Kubernetes deployment plan — Financial Analysis

This document records the **branch integration decision**, how it compares to the `microservices-trading-bot` **feat/k8s-deployment-manifests** model, and the **sequenced rollout** for a production-ready deployment on EKS.

## 1. Branch analysis

| Branch | Role |
|--------|------|
| `webscraping` | News/stock scraping code under `WebScraping/`, shared `config/`, `Storage/`, and related tests. |
| `feature/etl-transforms` | ETL pipelines, `src/` packages (`pipelines`, `transform`, `export`, etc.), docs, and additional `eks/` samples (merge may overlap with `feat/eks-infrastructure-baseline` Terraform — treat **application** `k8s/` as canonical for workloads). |
| `feat/k8s-deployment-manifests` | Kustomize manifests (`k8s/`), Docker/compose, and GitHub Actions for building and deploying workloads. |

Note: the repository uses **`feature/etl-transforms`** (not `feat/etl-transforms`).

## 2. Is merging into `feat/k8s-deployment-manifests` optimal?

**Yes, as an integration branch**, for the same reasons as `microservices-trading-bot`:

- **Single deployable unit**: one branch produces **tested images + manifests + CI** that match each other.
- **Clear promotion path**: `dev → staging → prod` via Kustomize overlays and image tags.
- **Parity with reference repo**: application code, `docker-compose`, `k8s/`, `.github/workflows`, and `ci-cd/` stay co-located.

**Caveats (true for any monorepo integration branch):**

- **Not “production-ready” by itself**: you still need hardened secrets (External Secrets / AWS Secrets Manager), network policies, backups, SLOs, and change management.
- **Duplicate infra samples**: after merging `feature/etl-transforms`, you may have both `k8s/` (preferred for app workloads) and `eks/infrastructure/kubernetes/` (older samples). Prefer **`k8s/`** for application deployment unless you explicitly align the other tree.
- **Large images**: the ETL image includes ML stack (`torch`, `transformers`, `spacy`); use node selectors / larger nodes if needed.

## 3. What was implemented on `feat/k8s-deployment-manifests`

1. **Merges**: `webscraping` and `feature/etl-transforms` merged into `feat/k8s-deployment-manifests` (notebooks conflicts resolved toward ETL layout; `Storage/__pycache__` removed from version control).
2. **Per-service Dockerfiles** (build context = repo root):
   - `services/scraper/Dockerfile` — Chromium + Selenium; **no** PyTorch (smaller image).
   - `services/etl/Dockerfile` — `python -m pipelines.etl_cli` entrypoint for ETL / GenAI export.
3. **`docker-compose.yml`** — Postgres (existing), scraper build updated to `services/scraper/Dockerfile`; **`etl`** service added under profile **`etl`** (`docker compose --profile etl up`).
4. **Kubernetes** — `k8s/base/cronjobs-etl.yaml` (ETL CronJobs; **suspended** until you enable them), images `financial-analysis-etl` in `kustomization.yaml`.
5. **CI/CD** — `.github/workflows/ecr-publish.yml`, `docker-build.yml`, `deploy-development.yml`; `ci-cd/github-actions/` mirrors; `ci-cd/scripts/trigger-*.sh`.
6. **`build.yml`** — scraper build uses `services/scraper/Dockerfile`.

## 4. Recommended execution order (aligned with `application-integration-workload.md`)

1. **Infra**: keep `feat/eks-infrastructure-baseline` (or current Terraform) as the source of truth for VPC, EKS, ECR, IAM.
2. **Images**: run **ECR publish** (or GHCR `build.yml` for scraper-only) for `financial-analysis-scraper` and `financial-analysis-etl`.
3. **Secrets**: ensure AWS Secrets Manager (or sealed secrets) provides DB password and optional AWS keys; sync into the cluster (External Secrets).
4. **Deploy**: `kubectl apply -k k8s/overlays/development` (or GitHub **Deploy (Development)** workflow).
5. **ETL CronJobs**: set `suspend: false` only after DB connectivity and `transform-news` / `transform-stocks` commands succeed as one-off Jobs.
6. **Promotion**: clone overlays for `staging` / `prod`, tighten resources, use private subnets and RDS instead of in-cluster Postgres where required.

## 5. Reference: microservices-trading-bot

The [feat/k8s-deployment-manifests](https://github.com/Gall-oDrone/microservices-trading-bot/tree/feat/k8s-deployment-manifests) branch uses:

- `services/<name>/Dockerfile` per workload  
- `ci-cd/github-actions/` + `.github/workflows/`  
- ECR publish via OIDC and deploy via `kubectl apply -k ...`

This repo mirrors that pattern with **scraper** + **etl** instead of Go microservices.

## 6. Open items (next PRs)

- [ ] Replace `PLACEHOLDER_OWNER` / registry in `k8s/overlays/development/kustomization.yaml` with your GHCR or ECR URLs.
- [ ] Wire **External Secrets** for `financial-analysis-secrets` in development EKS.
- [ ] Unsuspend and tune **CronJob** schedules after smoke tests.
- [ ] Add **staging/prod** overlays with stricter limits and RDS endpoints.
- [ ] Optional: **GPU node group** + NeMo enrichment (see `docs/application-integration-workload.md`).
