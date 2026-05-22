# Financial Analysis - Development Environment

Terraform configuration for the **development** environment.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.5.0
- Backend S3 bucket and DynamoDB table (see `backend.example.hcl` or run `../../scripts/setup-backend.sh` from infrastructure root)

## Usage

```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

## Configure kubectl

```bash
aws eks update-kubeconfig --name financial-analysis-dev --region us-east-1
```
