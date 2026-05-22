# Financial Analysis: Application Integration and Workload Design

This document describes how to integrate the `webscraping` and `feature/etl-transforms` branches on top of Terraform-provisioned EKS infrastructure, and how to evolve the platform to include NVIDIA NeMo later.

## Scope

- EKS infrastructure is assumed to be already deployed by Terraform.
- Focus is on workload topology, branch integration flow, deployment sequencing, and future AI integration.
- This is an implementation guide, not a code-level runbook.

## Branch Integration Strategy

### Source branches

- `feat/eks-infrastructure-baseline`
  - Terraform baseline for VPC, EKS, IAM, RDS, and security resources.
- `webscraping`
  - News and stock scraping producers.
- `feature/etl-transforms`
  - ETL pipelines, transforms, GenAI export workflow, and Kubernetes manifests.

### Integration order

1. Keep `feat/eks-infrastructure-baseline` as infra foundation.
2. Merge webscraping producers.
3. Merge ETL/transforms processors.
4. Standardize canonical package/runtime paths and environment contracts.
5. Deploy with Kustomize overlays by environment (`dev`, `staging`, `prod`).

## Target Platform Topology

### AWS components

- **EKS**: one cluster per environment (not one cluster per microservice).
- **ECR**: container image registry for workloads.
- **RDS PostgreSQL**: raw and transformed financial datasets.
- **S3**: raw files, transformed outputs, JSONL exports, embeddings/signals.
- **Secrets Manager**: credentials and provider/API secrets.
- **CloudWatch**: logs and metrics.

### Kubernetes namespace model

- Namespace: `financial-analysis`
- Per-workload service accounts with IRSA.
- External secrets synced from AWS Secrets Manager.
- ConfigMaps for non-sensitive runtime configuration.
- Network policies applied at namespace/workload level.

## Workload Design (Microservices/Jobs)

Use one shared image initially and configure behavior by `command` in each workload.

### Scraping workloads

- `scraper-news` (CronJob)
  - Schedule: every 6h
  - Output: `RDS(raw_news)` and/or `S3(raw/news)`
- `scraper-stocks` (CronJob)
  - Schedule: every 4h
  - Output: `RDS(raw_stocks)` and/or `S3(raw/stocks)`

### ETL workloads

- `etl-news-transform` (CronJob or Job)
  - Input: `RDS(raw_news)`
  - Output: `RDS(transformed_news)`, `S3(transformed/news)`
- `etl-stocks-transform` (CronJob or Job)
  - Input: `RDS(raw_stocks)`
  - Output: `RDS(transformed_stocks)`, `S3(transformed/stocks)`
- `export-genai-news` (CronJob or Job)
  - Input: transformed news
  - Output: `S3(news/jsonl)` for downstream AI pipelines

### Optional orchestration services

- Internal API service for manual triggering/retries.
- Workflow orchestrator (Argo/Airflow/EventBridge) for scrape -> transform dependency control.

## End-to-End Data Flow

### News path

1. `scraper-news` collects articles.
2. Writes raw records into PostgreSQL and/or S3.
3. `etl-news-transform` enriches text/sentiment/entities/tickers.
4. `export-genai-news` writes JSONL to S3 for AI consumption.

### Stocks path

1. `scraper-stocks` collects market/price data.
2. Writes raw records into PostgreSQL and/or S3.
3. `etl-stocks-transform` computes transformed features/indicators.
4. Writes transformed outputs to PostgreSQL and S3.

## CI/CD and Promotion Flow

1. Build/test image from integration branch.
2. Push image tags to ECR.
3. Deploy Kustomize overlay to `dev`.
4. Validate scrape -> transform -> export smoke tests.
5. Promote to `staging`.
6. Promote to `prod`.

## Validation Gates

- Schema compatibility checks for raw and transformed tables.
- End-to-end smoke tests for each domain (news and stocks).
- Scheduling/resource checks (runtime, retries, timeout, cost).
- Data quality checks on transformed outputs and JSONL exports.

## NVIDIA NeMo Integration (Future Phase)

NeMo should be introduced as a dedicated AI enrichment workload, decoupled from scraping jobs.

### Infrastructure additions

- Add GPU node group to EKS via Terraform (for example, `g5`/`g6` families).
- Configure autoscaling, taints/tolerations, and node selectors for GPU isolation.

### Runtime design

- Deploy NeMo enrichment service (Deployment/Job) on GPU nodes.
- Input: transformed data and/or GenAI JSONL from S3.
- Output: embeddings, summaries, classification signals back to S3 (and optionally RDS).
- Optional internal inference endpoint for downstream services.

### Operational guidance

- Start in `dev` with bounded datasets and strict runtime budgets.
- Keep NeMo asynchronous so scraping/ETL continue even if AI enrichment is degraded.
- Add dedicated observability for GPU utilization, queue lag, and model latency.

## Recommended Next Execution Steps

1. Align branch merge plan and resolve path/module overlaps.
2. Freeze environment variable and secrets contract for all workloads.
3. Add ETL CronJobs next to existing scraper CronJobs in Kubernetes manifests.
4. Validate full pipeline in `dev` with a short scheduling cadence.
5. Prepare Terraform GPU node-group design for NeMo phase.

