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

output "external_dns_irsa_role_arn" {
  description = "IRSA role ARN for ExternalDNS"
  value       = module.iam_irsa.irsa_role_arns["external-dns"]
}

output "cert_manager_irsa_role_arn" {
  description = "IRSA role ARN for cert-manager (Route53 DNS-01 when configured)"
  value       = module.iam_irsa.irsa_role_arns["cert-manager"]
}

output "external_secrets_irsa_role_arn" {
  description = "IRSA role ARN for external-secrets operator"
  value       = module.iam_irsa.irsa_role_arns["external-secrets"]
}

# RDS Outputs (null when enable_rds is false)
output "rds_endpoint" {
  description = "RDS endpoint"
  value       = var.enable_rds ? module.rds[0].endpoint : null
}

output "rds_secret_arn" {
  description = "RDS credentials secret ARN"
  value       = var.enable_rds ? module.rds[0].secret_arn : null
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
