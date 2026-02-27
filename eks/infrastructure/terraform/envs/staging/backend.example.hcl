# Copy to backend.hcl and fill in values, then: terraform init -backend-config=backend.hcl

bucket         = "financial-analysis-terraform-state"
key            = "staging/terraform.tfstate"
region         = "us-east-1"
encrypt        = true
dynamodb_table = "financial-analysis-terraform-locks"
