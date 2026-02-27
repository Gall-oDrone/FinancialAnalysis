# Copy to backend.hcl and fill in values, then: terraform init -backend-config=backend.hcl
# Or use default S3 bucket name below after running infrastructure/scripts/setup-backend.sh

bucket         = "financial-analysis-terraform-state"
key            = "development/terraform.tfstate"
region         = "us-east-1"
encrypt        = true
dynamodb_table = "financial-analysis-terraform-locks"
