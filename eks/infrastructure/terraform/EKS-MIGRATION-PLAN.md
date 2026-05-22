# Financial Analysis - EKS Infrastructure Migration Plan

This document outlines the EKS infrastructure layout and migration notes for the Financial Analysis project.

## Structure (aligned with microservices-trading-bot)

```
terraform/
├── eks/                    # Root-level EKS placeholder / shared config
├── envs/                   # Environment-specific configurations
│   ├── development/
│   ├── staging/
│   └── production/
├── modules/                # Reusable modules (vpc, eks, rds, security)
├── scripts/                # Terraform helper scripts
│   ├── cleanup/
│   ├── deploy/
│   └── secrets/
└── EKS-MIGRATION-PLAN.md   # This file
```

## Environment Mapping

| Folder        | Environment var | Cluster name              | Backend state key     |
|---------------|-----------------|---------------------------|------------------------|
| development/ | dev             | financial-analysis-dev    | development/terraform.tfstate |
| staging/      | staging         | financial-analysis-staging| staging/terraform.tfstate    |
| production/   | prod            | financial-analysis-prod   | production/terraform.tfstate  |

## Migrating from old `environments/` layout

If you had state under `terraform/environments/dev`, `staging`, or `prod`:

1. Create the S3 backend with keys `development/`, `staging/`, `production/` (or keep existing keys).
2. To keep existing state key `dev/terraform.tfstate`, edit `envs/development/backend.tf` and set `key = "dev/terraform.tfstate"`.
3. Run `terraform init -migrate-state` from `envs/development` (or the target env) if moving state key.

## Terraform analysis and fixes (nested folders)

- **Cleanup script KMS state paths**: `cleanup.sh` was updated to use `module.eks.aws_kms_key.eks` and `module.eks.aws_kms_alias.eks` (this repo’s EKS module is flat; no nested `module.eks.module.kms`).
- **ExternalSecret key**: Base `external-secret.yaml` used literal `financial-analysis/${ENVIRONMENT}/db-credentials`, which doesn’t resolve. Base now uses `financial-analysis/dev/db-credentials`; staging and prod overlays patch the ExternalSecret `remoteRef.key` to `financial-analysis/staging/db-credentials` and `financial-analysis/prod/db-credentials` so they match Terraform RDS secret names.
- **Terraform version**: Env and modules require `>= 1.5.0`. Use Terraform 1.5+ for `terraform init`/`validate`/`apply`.

## Next steps

- Prefer Terraform over CloudFormation for net-new EKS work; CloudFormation templates remain for reference or hybrid use.
- Use `envs/<env>` for all environment-specific applies.
- Backend setup: run `eks/infrastructure/scripts/setup-backend.sh` before first apply.
