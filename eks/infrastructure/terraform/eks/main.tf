# =============================================================================
# Financial Analysis - EKS Root / Placeholder
# =============================================================================
# This directory can hold root-level EKS Terraform configuration if desired.
# Environment-specific EKS is managed via envs/<env> calling modules/eks.
# =============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

provider "aws" {
  region = var.aws_region
}

# TODO: Add any shared EKS-related resources or data sources here if needed
