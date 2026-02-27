# =============================================================================
# Financial Analysis - Staging Environment Configuration
# =============================================================================

project_name       = "financial-analysis"
environment        = "staging"
aws_region         = "us-east-1"
vpc_cidr           = "10.1.0.0/16"
kubernetes_version = "1.29"
db_name            = "financial_db"
db_username        = "financial_user"

tags = {
  Team       = "data-engineering"
  CostCenter = "staging"
}
