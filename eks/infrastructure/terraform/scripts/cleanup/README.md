# Terraform cleanup scripts

Scripts for tearing down Financial Analysis EKS/Terraform environments with minimal manual AWS console work. Aligned with [microservices-trading-bot](https://github.com/Gall-oDrone/microservices-trading-bot) `infrastructure/terraform/scripts/cleanup`.

## Scripts

| Script | Purpose |
|--------|---------|
| **cleanup.sh** | Full teardown: K8s LoadBalancers/Ingresses, AWS LBs/target groups, ECR (`financial-analysis`), CloudWatch logs, KMS (EKS key), enhanced Terraform state cleanup, `terraform destroy` with retries, optional manual VPC cleanup, verification. |
| **addons.sh** | Uninstall Helm releases (metrics-server, AWS LB controller, External DNS, cert-manager, external-secrets, optional kube-prometheus if present) and related namespaces/CRDs; optional `financial-analysis` namespace. Does **not** run Terraform. |
| **cleanup-k8s-app.sh** | Delete application resources in the **`financial-analysis`** namespace only. Run first if the cluster is still up. |
| **cleanup-secrets.sh** | Permanently delete app secrets in AWS Secrets Manager (see script for list). Does not delete RDS-managed secrets. |
| **delete-stuck-addon.sh** | Delete a stuck EKS managed add-on (e.g. `aws-ebs-csi-driver`). Usage: `./delete-stuck-addon.sh <addon-name> [environment]`. |
| **cleanup-terraform-backend.sh** | Delete remote state **S3 bucket** + **DynamoDB** lock table (only after you no longer need Terraform state). |

## Recommended order

```bash
# 1. Optional: application workloads (cluster still up)
./cleanup-k8s-app.sh development

# 2. Optional: Helm add-ons (cluster still up)
./addons.sh development

# 3. Full AWS + Terraform cleanup
./cleanup.sh development
```

Secrets only:

```bash
./cleanup-secrets.sh
```

All scripts use `AWS_REGION` (default `us-east-1`). The environment argument must match a folder under `terraform/envs/`: `development`, `staging`, or `production`.

**Warning:** `cleanup.sh` and `addons.sh` are destructive. `cleanup-terraform-backend.sh` deletes all state objects in the bucket.
