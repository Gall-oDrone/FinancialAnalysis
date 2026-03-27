# =============================================================================
# Financial Analysis - Development Environment (Modules)
# =============================================================================

locals {
  name = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# VPC Module
# -----------------------------------------------------------------------------
module "vpc" {
  source = "../../modules/vpc"

  project_name             = var.project_name
  environment              = var.environment
  vpc_cidr                 = var.vpc_cidr
  enable_nat_gateway       = true
  single_nat_gateway       = true # Cost savings for dev
  enable_flow_logs         = true
  flow_logs_retention_days = 14

  tags = var.tags
}

# -----------------------------------------------------------------------------
# EKS Module
# -----------------------------------------------------------------------------
module "eks" {
  source = "../../modules/eks"

  project_name       = var.project_name
  environment        = var.environment
  vpc_id             = module.vpc.vpc_id
  vpc_cidr           = module.vpc.vpc_cidr
  public_subnet_ids  = module.vpc.public_subnet_ids
  private_subnet_ids = module.vpc.private_subnet_ids

  kubernetes_version    = var.kubernetes_version
  node_instance_types   = ["t3.medium"]
  node_desired_capacity = 2
  node_min_size         = 1
  node_max_size         = 4
  node_disk_size        = 50
  use_spot_instances    = true # Cost savings for dev

  enable_public_access  = true
  enable_private_access = true

  tags = var.tags
}

# -----------------------------------------------------------------------------
# IRSA roles for cluster add-ons (ALB, ExternalDNS, cert-manager, external-secrets)
# Aligned with microservices-trading-bot module.iam_irsa
# -----------------------------------------------------------------------------
module "iam_irsa" {
  source = "../../modules/iam"

  cluster_name      = module.eks.cluster_name
  oidc_provider     = module.eks.oidc_provider_url
  oidc_provider_arn = module.eks.oidc_provider_arn

  irsa_policies = {
    "external-dns" = ["arn:aws:iam::aws:policy/AmazonRoute53FullAccess"]
    alb            = ["arn:aws:iam::aws:policy/ElasticLoadBalancingFullAccess"]
    "cert-manager" = ["arn:aws:iam::aws:policy/AmazonRoute53FullAccess"]
    "external-secrets" = [
      "arn:aws:iam::aws:policy/SecretsManagerReadWrite",
      "arn:aws:iam::aws:policy/AmazonSSMReadOnlyAccess",
    ]
  }
}

# -----------------------------------------------------------------------------
# RDS Module (optional — disabled by default in dev; use Docker Postgres locally)
# -----------------------------------------------------------------------------
module "rds" {
  count  = var.enable_rds ? 1 : 0
  source = "../../modules/rds"

  project_name               = var.project_name
  environment                = var.environment
  vpc_id                     = module.vpc.vpc_id
  db_subnet_group_name       = module.vpc.database_subnet_group_name
  allowed_security_group_ids = [module.eks.node_security_group_id]

  instance_class              = "db.t3.micro"
  allocated_storage           = 20
  max_allocated_storage       = 50
  db_name                     = var.db_name
  db_username                 = var.db_username
  multi_az                    = false
  backup_retention_period     = 7
  enable_performance_insights = false
  deletion_protection         = false

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Security Module
# -----------------------------------------------------------------------------
module "security" {
  source = "../../modules/security"

  project_name      = var.project_name
  environment       = var.environment
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_provider_url = module.eks.oidc_provider_url

  tags = var.tags
}

# -----------------------------------------------------------------------------
# GitHub Actions OIDC (ECR publish + EKS deploy workflows — same AWS account)
# -----------------------------------------------------------------------------
module "github_actions_oidc" {
  source = "../../modules/github_actions_oidc"

  name_prefix = local.name

  github_organization = var.github_organization
  github_repository   = var.github_repository

  create_oidc_provider = var.create_github_oidc_provider
  oidc_provider_arn    = var.github_oidc_provider_arn_override

  eks_cluster_name        = module.eks.cluster_name
  enable_eks_access_entry = var.github_actions_enable_eks_access

  tags = var.tags
}
