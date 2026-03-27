# Next steps to implement — Financial Analysis

This checklist continues from `docs/KUBERNETES-DEPLOYMENT-PLAN.md` and `docs/application-integration-workload.md`. Work through items in order where dependencies apply.

## 1. Registry and deploy wiring

**Goal:** Kubernetes pulls real images from ECR or GHCR (no `PLACEHOLDER_OWNER` / placeholder registry).

- [ ] Choose **ECR** (recommended for EKS) or **GHCR**.
- [ ] **ECR:** In the deploy workflow (or a pre-deploy step), mirror [microservices-trading-bot](https://github.com/Gall-oDrone/microservices-trading-bot): resolve `AWS_ACCOUNT_ID` with `aws sts get-caller-identity`, set `ECR_REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"`, then run `kustomize edit set image` for `financial-analysis-scraper` and `financial-analysis-etl` to `${ECR_REGISTRY}/<repo>:<tag>`, or replace `images` in `k8s/overlays/development/kustomization.yaml` with the full ECR URLs.
- [ ] **GHCR:** Set `newName` to `ghcr.io/Gall-oDrone/financial-analysis-scraper` (and etl) and ensure CI publishes matching tags.
- [ ] Align `.github/workflows/ecr-publish.yml` / `build.yml` with the chosen registry and repository names.

## 2. Secrets and configuration

- [ ] Replace ad-hoc secrets with **External Secrets Operator** + **AWS Secrets Manager** (or Sealed Secrets) for `financial-analysis-secrets` and optional AWS API keys.
- [ ] Document and freeze **environment variables** for scrapers, ETL, and export (Postgres, S3, optional LLM keys) in one place (e.g. `env.example` + runbook).

## 3. Validate images and pipelines

- [ ] **Scraper:** Build `services/scraper/Dockerfile`, run against Postgres (e.g. `docker compose` or EKS Job) and confirm a minimal scrape path works.
- [ ] **ETL:** Build `services/etl/Dockerfile`, run `python -m pipelines.etl_cli` for `transform-news`, `transform-stocks`, and `export-genai` with test data or a narrow date range.
- [ ] Fix any import/path issues discovered in container (compare to local `PYTHONPATH`).

## 4. Kubernetes rollout

- [ ] `kubectl apply -k k8s/overlays/development` (or the GitHub **Deploy (Development)** workflow) after images exist in the registry.
- [ ] Confirm **IRSA** / service accounts if workloads need S3 or Secrets Manager from pods.
- [ ] Set resource **requests/limits** and probes where missing.

## 5. ETL CronJobs

- [ ] In `k8s/base/cronjobs-etl.yaml`, CronJobs default to **`suspend: true`**. After DB and S3 checks pass, set **`suspend: false`** (or patch per overlay) and tune **schedules** for `etl-transform-news`, `etl-transform-stocks`, and `etl-export-genai`.
- [ ] Run a **manual Job** once before enabling CronJobs to avoid silent failures.

## 6. Staging and production

- [ ] Add **`k8s/overlays/staging`** and **`k8s/overlays/prod`** with namespaces, image tags, and RDS endpoints (if using managed Postgres instead of in-cluster Postgres).
- [ ] Use **GitHub Environments** (staging / production) with approval gates and environment-specific secrets (`EKS_CLUSTER_NAME_*`, etc.).

## 7. CI/CD polish

- [ ] Ensure **ECR publish** runs on the right branches/tags and tags images with `git sha` and `latest` as needed.
- [ ] Optionally add **dynamic `kustomize edit set image`** in `deploy-development.yml` so the repo never stores account-specific ECR URLs (same pattern as trading-bot `deploy.yml`).
- [ ] Add a **smoke test** step after deploy (e.g. `kubectl get pods`, optional HTTP health if applicable).

## 8. Security and operations

- [ ] **NetworkPolicies** for namespace `financial-analysis` (or per-env namespace).
- [ ] **PodDisruptionBudgets** for long-running deployments.
- [ ] **Alerts** for failed CronJobs, pod restarts, and DB connectivity (CloudWatch or Prometheus).
- [ ] **Backups:** RDS snapshots and/or Postgres backup strategy if still using in-cluster DB in any environment.

## 9. Data and quality gates

- [ ] Schema compatibility checks between raw and transformed tables.
- [ ] Light **data-quality** checks on transformed outputs and JSONL exports before promoting to staging/prod.

## 10. Future: AI / GPU (optional)

- [ ] When scrape + ETL are stable, add **GPU node group** in Terraform and a dedicated **enrichment** workload (e.g. NeMo) per `application-integration-workload.md`, decoupled from CronJob scrape/ETL paths.

---

## Quick reference

| Doc | Purpose |
|-----|---------|
| `docs/application-integration-workload.md` | Target topology, data flow, NeMo phase |
| `docs/KUBERNETES-DEPLOYMENT-PLAN.md` | Branch strategy, what was merged, open items |
| `k8s/README.md` | Kustomize layout and local/EKS apply |
