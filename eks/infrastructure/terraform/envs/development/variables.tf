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

variable "tags" {
  description = "Additional tags"
  type        = map(string)
  default     = {}
}
