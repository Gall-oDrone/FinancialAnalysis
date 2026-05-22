# =============================================================================
# Financial Analysis - Production Environment Backend
# =============================================================================

terraform {
  backend "s3" {
    bucket         = "financial-analysis-terraform-state"
    key            = "production/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "financial-analysis-terraform-locks"
  }
}
