diagram FinancialAnalysis_Application_Integration_And_Workloads {
  meta {
    title: "Financial Analysis - Application Integration and Workload Design"
    version: "v1"
    owner: "Platform + Data Engineering"
    notes: "EKS is provisioned by Terraform. This diagram focuses on branch integration and Kubernetes workloads."
  }

  branches {
    eks_infra_baseline: "feat/eks-infrastructure-baseline\n- Terraform: VPC, EKS, IAM, RDS, security"
    webscraping_branch: "webscraping\n- News + stock scraping collectors"
    etl_transforms_branch: "feature/etl-transforms\n- ETL pipelines, transforms, GenAI export CLI, K8s manifests"
    integration_branch: "integration target branch\n- Unified runtime + deployment topology"
  }

  merge_flow {
    step_1: "1) Keep EKS Terraform baseline as infra foundation"
    step_2: "2) Merge webscraping data producer code"
    step_3: "3) Merge etl-transforms data processor code"
    step_4: "4) Standardize canonical package paths + env contracts"
    step_5: "5) Deploy workloads via Kustomize overlays (dev/staging/prod)"
  }

  aws {
    ecr: "Amazon ECR\n- Container images"
    eks_dev: "EKS Cluster (dev)"
    eks_staging: "EKS Cluster (staging)"
    eks_prod: "EKS Cluster (prod)"
    rds: "Amazon RDS PostgreSQL\n- Raw + transformed tables"
    s3: "Amazon S3\n- Raw, transformed, JSONL, embeddings"
    secrets: "AWS Secrets Manager\n- DB credentials, API keys, provider tokens"
    logs: "CloudWatch Logs/Metrics"
  }

  k8s_namespace {
    namespace: "namespace: financial-analysis"

    scraping_jobs {
      news_scraper_cron: "CronJob: scraper-news\nschedule: every 6h\nwrites: RDS(raw_news), S3(raw/news)"
      stocks_scraper_cron: "CronJob: scraper-stocks\nschedule: every 4h\nwrites: RDS(raw_stocks), S3(raw/stocks)"
    }

    etl_jobs {
      news_transform_cron: "CronJob: etl-news-transform\ninput: RDS(raw_news)\noutput: RDS(transformed_news), S3(transformed/news)"
      stocks_transform_cron: "CronJob: etl-stocks-transform\ninput: RDS(raw_stocks)\noutput: RDS(transformed_stocks), S3(transformed/stocks)"
      genai_export_cron: "CronJob: export-genai-news\ninput: transformed news\noutput: S3(news/jsonl)"
    }

    optional_services {
      api_service: "Deployment+Service (optional)\ninternal API for triggering ETL/jobs"
      workflow_orchestrator: "Argo/Airflow/EventBridge (optional)\njob dependency orchestration"
    }
  }

  nemo_integration {
    gpu_nodegroup: "EKS GPU Node Group (later)\nTerraform add-on: g5/g6 nodes + autoscaling"
    nemo_service: "NeMo Enrichment Service\nDeployment/Job on GPU nodes"
    nemo_inputs: "Input: S3 JSONL + transformed datasets"
    nemo_outputs: "Output: embeddings/summaries/signals -> S3 + optional RDS tables"
    inference_endpoint: "Optional internal endpoint for model inference"
  }

  data_flow {
    scrape_news_flow: "scraper-news -> RDS(raw_news) -> etl-news-transform -> S3(transformed/news) -> export-genai-news -> S3(JSONL)"
    scrape_stocks_flow: "scraper-stocks -> RDS(raw_stocks) -> etl-stocks-transform -> S3(transformed/stocks)"
    nemo_flow: "S3(JSONL + transformed data) -> NeMo service -> S3(embeddings/signals) -> downstream analytics"
  }

  security_and_config {
    irsa: "IRSA per workload service account"
    externalsecrets: "ExternalSecret -> Kubernetes Secret sync from Secrets Manager"
    configmap: "ConfigMap for non-sensitive runtime settings"
    network_policy: "Namespace-level network policies"
  }

  ci_cd {
    build: "CI builds and tests image from integration branch"
    publish: "Push image tags to ECR"
    deploy_dev: "Kustomize overlay deploy to dev"
    promote: "Promote to staging then prod after validation"
  }

  validation_gates {
    gate_1: "Schema compatibility checks (raw and transformed tables)"
    gate_2: "End-to-end smoke test: scrape -> transform -> export"
    gate_3: "Cost/performance checks for scraping + ETL schedules"
    gate_4: "NeMo pilot on dev with bounded data windows"
  }

  links {
    branches.eks_infra_baseline -> branches.integration_branch
    branches.webscraping_branch -> branches.integration_branch
    branches.etl_transforms_branch -> branches.integration_branch

    branches.integration_branch -> aws.ecr
    aws.ecr -> k8s_namespace.scraping_jobs.news_scraper_cron
    aws.ecr -> k8s_namespace.scraping_jobs.stocks_scraper_cron
    aws.ecr -> k8s_namespace.etl_jobs.news_transform_cron
    aws.ecr -> k8s_namespace.etl_jobs.stocks_transform_cron
    aws.ecr -> k8s_namespace.etl_jobs.genai_export_cron

    k8s_namespace.scraping_jobs.news_scraper_cron -> aws.rds
    k8s_namespace.scraping_jobs.news_scraper_cron -> aws.s3
    k8s_namespace.scraping_jobs.stocks_scraper_cron -> aws.rds
    k8s_namespace.scraping_jobs.stocks_scraper_cron -> aws.s3

    k8s_namespace.etl_jobs.news_transform_cron -> aws.rds
    k8s_namespace.etl_jobs.news_transform_cron -> aws.s3
    k8s_namespace.etl_jobs.stocks_transform_cron -> aws.rds
    k8s_namespace.etl_jobs.stocks_transform_cron -> aws.s3
    k8s_namespace.etl_jobs.genai_export_cron -> aws.s3

    aws.s3 -> nemo_integration.nemo_service
    nemo_integration.gpu_nodegroup -> nemo_integration.nemo_service
    nemo_integration.nemo_service -> aws.s3

    aws.secrets -> k8s_namespace.namespace
    k8s_namespace.namespace -> aws.logs
  }
}
