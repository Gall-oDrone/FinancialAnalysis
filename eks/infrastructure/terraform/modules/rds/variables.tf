# =============================================================================
# Financial Analysis - RDS Module Variables
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

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "db_subnet_group_name" {
  description = "Database subnet group name"
  type        = string
}

variable "allowed_security_group_ids" {
  description = "List of security group IDs allowed to access RDS"
  type        = list(string)
}

variable "instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "allocated_storage" {
  description = "Allocated storage in GB"
  type        = number
  default     = 20
}

variable "max_allocated_storage" {
  description = "Maximum storage for autoscaling in GB"
  type        = number
  default     = 100
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

variable "multi_az" {
  description = "Enable Multi-AZ deployment"
  type        = bool
  default     = false
}

variable "backup_retention_period" {
  description = "Backup retention period in days"
  type        = number
  default     = 7
}

variable "enable_performance_insights" {
  description = "Enable Performance Insights"
  type        = bool
  default     = false
}

variable "deletion_protection" {
  description = "Enable deletion protection"
  type        = bool
  default     = false
}

variable "alarm_actions" {
  description = "List of ARNs for alarm actions (SNS topics)"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}

