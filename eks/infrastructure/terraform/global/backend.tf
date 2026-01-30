# =============================================================================
# Financial Analysis - Terraform Backend Configuration
# =============================================================================
# This file documents the backend configuration.
# Run scripts/setup-backend.sh to create the required resources.
# =============================================================================

# Backend configuration is defined in each environment's main.tf
# 
# Example backend configuration:
#
# terraform {
#   backend "s3" {
#     bucket         = "financial-analysis-terraform-state"
#     key            = "dev/terraform.tfstate"  # Change per environment
#     region         = "us-east-1"
#     encrypt        = true
#     dynamodb_table = "financial-analysis-terraform-locks"
#   }
# }

# To initialize the backend:
# 1. Run scripts/setup-backend.sh to create S3 bucket and DynamoDB table
# 2. Navigate to the desired environment: cd terraform/environments/dev
# 3. Run: terraform init

