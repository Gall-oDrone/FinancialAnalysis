# Financial Analysis - Production Environment

Terraform configuration for the **production** environment.

## Usage

```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

```bash
aws eks update-kubeconfig --name financial-analysis-prod --region us-east-1
```
