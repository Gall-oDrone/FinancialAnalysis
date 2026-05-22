# =============================================================================
# Financial Analysis - Development Environment Variables
# =============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "financial-analysis"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.29"
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "financial_db"
}

variable "db_username" {
  description = "Database master username"
  type        = string
  default     = "financial_user"
}

variable "enable_rds" {
  description = "Provision RDS PostgreSQL. Set false in dev when using Docker or another local DB."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags"
  type        = map(string)
  default     = {}
}

variable "github_organization" {
  description = "GitHub org for Actions OIDC trust (must match repo owner)"
  type        = string
  default     = "Gall-oDrone"
}

variable "github_repository" {
  description = "GitHub repository name for OIDC sub claim"
  type        = string
  default     = "FinancialAnalysis"
}

variable "create_github_oidc_provider" {
  description = "Create account-level OIDC provider for token.actions.githubusercontent.com. Set true only in a greenfield account; set false when the provider already exists (common in shared AWS accounts)."
  type        = bool
  default     = false
}

variable "github_oidc_provider_arn_override" {
  description = "When create_github_oidc_provider is false, set to existing provider ARN"
  type        = string
  default     = ""
}

variable "github_actions_enable_eks_access" {
  description = "Create EKS access entry + admin policy for the GitHub Actions role (kubectl in deploy workflow)"
  type        = bool
  default     = true
}
