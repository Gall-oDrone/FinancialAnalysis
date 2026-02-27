# Financial Analysis - Staging Environment

Terraform configuration for the **staging** environment.

## Usage

```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

```bash
aws eks update-kubeconfig --name financial-analysis-staging --region us-east-1
```
