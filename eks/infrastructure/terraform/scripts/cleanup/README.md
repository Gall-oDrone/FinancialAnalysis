# Terraform Cleanup Scripts

Scripts for tearing down Financial Analysis EKS/Terraform environments with minimal manual AWS console work.

## Scripts

- **cleanup.sh** – Full EKS cleanup: K8s LoadBalancers/Ingresses, AWS LBs/Target Groups, ECR, CloudWatch Logs, KMS, Terraform state clean, then `terraform destroy`. Run with environment name.
- **cleanup-k8s-app.sh** – Delete only application resources in the `financial-analysis` namespace (ingresses, deployments, services, ExternalSecrets, namespace). Run before full cleanup if the cluster is still up.
- **cleanup-secrets.sh** – Permanently delete app secrets in AWS Secrets Manager (Bitso API, Redis). Does not delete RDS-managed secrets.

## Usage

Full infrastructure cleanup (recommended order):

```bash
# 1. Optional: remove app from cluster first (if cluster is still up)
./cleanup-k8s-app.sh development

# 2. Full EKS + Terraform cleanup
./cleanup.sh development
```

Secrets only:

```bash
./cleanup-secrets.sh
```

All scripts use `AWS_REGION` (default `us-east-1`). Environment must be one of: `development`, `staging`, `production` (folder names under `terraform/envs/`).
