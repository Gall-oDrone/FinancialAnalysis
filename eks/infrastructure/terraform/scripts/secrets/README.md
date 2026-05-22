# Terraform / Application Secrets

Scripts to create and delete application secrets in AWS Secrets Manager. RDS credentials are created by the Terraform RDS module; these scripts are for Bitso API and Redis (and similar app secrets).

## setup-secrets.sh

Creates or updates secrets used by External Secrets Operator in Kubernetes:

- `financial-analysis/bitso-api-key`
- `financial-analysis/bitso-api-secret`
- `financial-analysis/redis-password` (optional)

**When to run:** After Terraform infrastructure is deployed. Prompts for values if not set in the script.

```bash
./setup-secrets.sh
```

Uses `AWS_REGION` (default `us-east-1`). Do not commit real secret values.

## cleanup-secrets.sh

Permanently deletes the above secrets from AWS Secrets Manager. Lives under `scripts/cleanup/`:

```bash
../cleanup/cleanup-secrets.sh
```

RDS credentials (e.g. `financial-analysis/${ENVIRONMENT}/db-credentials`) are managed by Terraform and are not deleted by this script.
