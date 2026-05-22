# =============================================================================
# Financial Analysis - Staging Environment Backend
# =============================================================================

terraform {
  backend "s3" {
    bucket         = "financial-analysis-terraform-state"
    key            = "staging/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "financial-analysis-terraform-locks"
  }
}
