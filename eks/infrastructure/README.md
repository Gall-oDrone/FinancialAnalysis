# Financial Analysis - EKS Infrastructure

Infrastructure as Code (IaC) for provisioning AWS resources and deploying the Financial Analysis platform on Amazon EKS.

## Directory structure

```
eks/infrastructure/
├── cloudformation/
│   ├── vpc/                          # VPC and networking (legacy stacks)
│   ├── eks/                          # EKS cluster (legacy stacks)
│   ├── rds/                          # RDS PostgreSQL (legacy stacks)
│   ├── monitoring/                   # CloudWatch and monitoring
│   ├── financial-analysis-ide-*.yaml # IDE workspace stacks (IAM, EC2, CloudFront)
│   └── scripts/                      # deploy-cfn.sh, cleanup-cfn.sh
├── terraform/
│   ├── envs/                         # Canonical per-environment configs
│   │   ├── development/
│   │   ├── staging/
│   │   └── production/
│   ├── environments/                 # Legacy layout (deprecated — see EKS-MIGRATION-PLAN.md)
│   ├── modules/                      # vpc, eks, rds, security, iam, github_actions_oidc
│   ├── scripts/
│   │   ├── cleanup/                  # Cluster, addons, secrets, backend teardown
│   │   ├── deploy/                   # deploy-development-eks.sh
│   │   └── secrets/                  # setup-secrets.sh
│   ├── global/                       # Shared backend config
│   └── EKS-MIGRATION-PLAN.md
├── kubernetes/                       # Legacy in-repo K8s manifests (see repo k8s/)
└── scripts/                          # Top-level setup-backend.sh, cleanup.sh
```

Application workloads on the cluster are deployed from the repo root [`k8s/`](../../k8s/README.md) directory (Kustomize). See also [`docs/KUBERNETES-DEPLOYMENT-PLAN.md`](../../docs/KUBERNETES-DEPLOYMENT-PLAN.md).

## Prerequisites

- AWS CLI v2 with credentials for the target account/region
- Terraform >= 1.5.0
- kubectl >= 1.28
- Helm >= 3.12

## Quick start — development EKS (recommended)

Bootstrap remote state (once per account), then deploy the development environment:

```bash
# From repo root
bash eks/infrastructure/terraform/scripts/deploy/deploy-development-eks.sh
```

Or manually:

```bash
cd eks/infrastructure/terraform/envs/development
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

Configure kubectl:

```bash
aws eks update-kubeconfig --name financial-analysis-dev --region us-east-1
```

Copy `backend.example.hcl` to `backend.hcl` and adjust if not using the deploy script defaults. See [terraform/envs/development/README.md](terraform/envs/development/README.md).

## Terraform environments

| Directory | Environment | Cluster (typical) |
|-----------|-------------|-------------------|
| `envs/development/` | dev | `financial-analysis-dev` |
| `envs/staging/` | staging | `financial-analysis-staging` |
| `envs/production/` | prod | `financial-analysis-prod` |

Helm addons (ExternalDNS, cert-manager, external-secrets, Apache Airflow, etc.) are defined in each env’s `helm-addons.tf`. GitHub Actions OIDC for ECR/EKS is in `modules/github_actions_oidc/`.

## Legacy layouts

- **`terraform/environments/{dev,staging,prod}/`** — older module wiring; prefer `envs/` for new work. Migration notes: [terraform/EKS-MIGRATION-PLAN.md](terraform/EKS-MIGRATION-PLAN.md).
- **`kubernetes/`** under this tree — superseded by root [`k8s/`](../../k8s/README.md) for application manifests.

## CloudFormation (VPC / IDE)

Legacy VPC/EKS/RDS stacks:

```bash
aws cloudformation deploy \
  --template-file eks/infrastructure/cloudformation/vpc/vpc-stack.yaml \
  --stack-name financial-analysis-vpc \
  --parameter-overrides Environment=dev
```

IDE workspace stacks: use `cloudformation/scripts/deploy-cfn.sh` and templates `financial-analysis-ide-*.yaml`.

## Operations

| Task | Location |
|------|----------|
| Deploy dev EKS | [terraform/scripts/deploy/README.md](terraform/scripts/deploy/README.md) |
| Teardown / cleanup | [terraform/scripts/cleanup/README.md](terraform/scripts/cleanup/README.md) |
| Secrets bootstrap | [terraform/scripts/secrets/README.md](terraform/scripts/secrets/README.md) |
| Integration design | [docs/application-integration-workload.md](../../docs/application-integration-workload.md) |

## Architecture

- **VPC**: Multi-AZ public/private subnets
- **EKS**: Managed cluster with IRSA-enabled workloads
- **RDS PostgreSQL**: Financial datasets (optional in dev)
- **ECR**: Container images for scraper/ETL
- **ALB Ingress**, **Secrets Manager**, **CloudWatch**

## License

See the main project LICENSE file.
