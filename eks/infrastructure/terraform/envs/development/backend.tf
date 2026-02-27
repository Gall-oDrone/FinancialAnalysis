# =============================================================================
# Financial Analysis - Development Environment Backend
# =============================================================================

terraform {
  backend "s3" {
    bucket         = "financial-analysis-terraform-state"
    key            = "development/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "financial-analysis-terraform-locks"
  }
}
