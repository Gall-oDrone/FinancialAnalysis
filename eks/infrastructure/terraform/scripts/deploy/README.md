# Terraform Deploy Scripts

Scripts for deploying Financial Analysis Terraform environments.

## Usage

From infrastructure root:

```bash
# Deploy development
cd ../envs/development && terraform init && terraform apply

# Or use the main infrastructure deploy script
../../scripts/deploy.sh -e dev -t
```

Environment-specific state is in `terraform/envs/<env>/`.
