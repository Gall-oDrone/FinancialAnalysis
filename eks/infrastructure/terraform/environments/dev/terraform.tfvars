# =============================================================================
# Financial Analysis - Development Environment Configuration
# =============================================================================

project_name       = "financial-analysis"
environment        = "dev"
aws_region         = "us-east-1"
vpc_cidr           = "10.0.0.0/16"
kubernetes_version = "1.29"
db_name            = "financial_db"
db_username        = "financial_user"

tags = {
  Team        = "data-engineering"
  CostCenter  = "development"
}

