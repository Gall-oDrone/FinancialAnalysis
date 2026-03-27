# =============================================================================
# Financial Analysis - Security Module Variables
# =============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "financial-analysis"
}

variable "environment" {
  description = "Environment name"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "oidc_provider_arn" {
  description = "OIDC provider ARN for IRSA"
  type        = string
}

variable "oidc_provider_url" {
  description = "OIDC provider URL (without https://)"
  type        = string
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}

