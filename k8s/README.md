# Kubernetes pipeline – Financial Analysis

This directory contains Kubernetes manifests for the financial-analysis pipeline (scraper, Postgres, and future ETL/MCP services), using [Kustomize](https://kustomize.io/) for base and environment-specific overlays.

**Cluster provisioning** (VPC, EKS, RDS, IAM, Helm platform addons): see [`eks/infrastructure/`](../eks/infrastructure/README.md).

## Layout

- **`base/`** – Shared resources: ConfigMap, Secret, Postgres Deployment/Service, Scraper Deployment/Service, ETL CronJobs (`cronjobs-etl.yaml`, suspended by default).
- **`overlays/development`** – Development (EKS or remote): namespace, image overrides (e.g. GHCR).
- **`overlays/dev`** – Local (kind/minikube): same resources, local image name.
- **`scripts/`** – Helper scripts to apply overlays.

## Prerequisites

- `kubectl` installed and configured for your cluster.
- For **dev (local)**:
  - [kind](https://kind.sigs.k8s.io/) or [minikube](https://minikube.sigs.k8s.io/).
  - Images built and loaded, e.g.:
    - `docker build -t financial-analysis-scraper:latest -f services/scraper/Dockerfile .`
    - `docker build -t financial-analysis-etl:latest -f services/etl/Dockerfile .`
    - `kind load docker-image financial-analysis-scraper:latest` (if using kind).
- For **development (remote/EKS)**:
  - Scraper image pushed to a registry (e.g. GHCR); set `newName`/`newTag` in `overlays/development/kustomization.yaml`.
  - Secret `financial-analysis-secrets` with at least `pg-password` (or use External Secrets).

## Quick start (local)

1. Create cluster (e.g. kind): `kind create cluster`
2. Build and load scraper image (see above).
3. Edit `k8s/base/secret.yaml` and set a safe `pg-password` (or use a patch in overlay).
4. Apply dev overlay:
   ```bash
   kubectl apply -k k8s/overlays/dev
   ```
   Or: `./k8s/scripts/apply-dev.sh`
5. Watch pods: `kubectl get pods -n financial-analysis-dev -w`

## Quick start (development / EKS)

1. Configure AWS and kubeconfig (e.g. `aws eks update-kubeconfig --name <cluster> --region <region>`).
2. Ensure scraper image is in registry and overlay `kustomization.yaml` has correct `images` entries.
3. Create or sync secrets (e.g. AWS Secrets Manager + External Secrets, or apply a secret patch).
4. Apply development overlay:
   ```bash
   kubectl apply -k k8s/overlays/development
   ```
   Or: `./k8s/scripts/apply-development.sh`
5. Wait for rollouts: `kubectl rollout status deployment/scraper -n financial-analysis-dev`

## Secrets

- **base/secret.yaml** – Placeholder; replace values via overlays or CI. In production, use External Secrets Operator or a secret manager (e.g. AWS Secrets Manager, Vault).
- Required key: `pg-password` (Postgres). Optional: `aws-access-key-id`, `aws-secret-access-key` for S3.

## CI/CD

- **GHCR (optional)**: `.github/workflows/build.yml` builds the scraper image and pushes to `ghcr.io`.
- **ECR**: `.github/workflows/ecr-publish.yml` builds `services/scraper` and `services/etl` and pushes to Amazon ECR (OIDC).
- **Docker validation**: `.github/workflows/docker-build.yml` builds both images on PRs (no push).
- **Deploy**: `.github/workflows/deploy-development.yml` applies `k8s/overlays/development` (configure `EKS_CLUSTER_NAME_DEVELOPMENT` and AWS OIDC secrets).
- **Scripts**: `ci-cd/scripts/trigger-ecr-publish.sh`, `trigger-deploy-development.sh` (requires [GitHub CLI](https://cli.github.com/)).

## ETL CronJobs

- Defined in `base/cronjobs-etl.yaml` (image `financial-analysis-etl`). They start **suspended**; edit `suspend: false` and fix schedules when ready.
- See `docs/KUBERNETES-DEPLOYMENT-PLAN.md` for rollout sequencing.

## Adding more workloads

- Add Deployments/CronJobs in `base/`, register in `base/kustomization.yaml`, and add an `images` entry if the workload uses a new image name.
- Reuse the same ConfigMap/Secret for DB and AWS config where possible.
