# =============================================================================
# Financial Analysis - Development Environment Outputs
# =============================================================================

# VPC Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = module.vpc.private_subnet_ids
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = module.vpc.public_subnet_ids
}

# EKS Outputs
output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "kubeconfig_command" {
  description = "Command to update kubeconfig"
  value       = module.eks.kubeconfig_command
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = module.eks.ecr_repository_url
}

output "alb_controller_irsa_role_arn" {
  description = "IRSA role ARN for AWS Load Balancer Controller (helm: serviceAccount.annotations eks.amazonaws.com/role-arn)"
  value       = module.iam_irsa.irsa_role_arns["alb"]
}

output "irsa_role_arns" {
  description = "Map of IRSA role keys to ARNs from modules/iam"
  value       = module.iam_irsa.irsa_role_arns
}

# RDS Outputs
output "rds_endpoint" {
  description = "RDS endpoint"
  value       = module.rds.endpoint
}

output "rds_secret_arn" {
  description = "RDS credentials secret ARN"
  value       = module.rds.secret_arn
}

# Security Outputs
output "application_role_arn" {
  description = "Application IAM role ARN"
  value       = module.security.application_role_arn
}

output "data_bucket_name" {
  description = "S3 data bucket name"
  value       = module.security.data_bucket_name
}
