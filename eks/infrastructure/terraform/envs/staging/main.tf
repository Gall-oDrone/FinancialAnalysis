# =============================================================================
# Financial Analysis - Staging Environment (Modules)
# =============================================================================

module "vpc" {
  source = "../../modules/vpc"

  project_name             = var.project_name
  environment              = var.environment
  vpc_cidr                 = var.vpc_cidr
  enable_nat_gateway       = true
  single_nat_gateway       = true
  enable_flow_logs         = true
  flow_logs_retention_days = 30

  tags = var.tags
}

module "eks" {
  source = "../../modules/eks"

  project_name       = var.project_name
  environment        = var.environment
  vpc_id             = module.vpc.vpc_id
  vpc_cidr           = module.vpc.vpc_cidr
  public_subnet_ids  = module.vpc.public_subnet_ids
  private_subnet_ids = module.vpc.private_subnet_ids

  kubernetes_version    = var.kubernetes_version
  node_instance_types   = ["t3.large"]
  node_desired_capacity = 3
  node_min_size         = 2
  node_max_size         = 6
  node_disk_size        = 80
  use_spot_instances    = false

  enable_public_access  = true
  enable_private_access = true

  tags = var.tags
}

module "iam_irsa" {
  source = "../../modules/iam"

  cluster_name      = module.eks.cluster_name
  oidc_provider     = module.eks.oidc_provider_url
  oidc_provider_arn = module.eks.oidc_provider_arn

  irsa_policies = {
    alb = ["arn:aws:iam::aws:policy/ElasticLoadBalancingFullAccess"]
  }
}

module "rds" {
  source = "../../modules/rds"

  project_name               = var.project_name
  environment                = var.environment
  vpc_id                     = module.vpc.vpc_id
  db_subnet_group_name       = module.vpc.database_subnet_group_name
  allowed_security_group_ids = [module.eks.node_security_group_id]

  instance_class              = "db.t3.small"
  allocated_storage           = 50
  max_allocated_storage       = 100
  db_name                     = var.db_name
  db_username                 = var.db_username
  multi_az                    = false
  backup_retention_period     = 14
  enable_performance_insights = true
  deletion_protection         = false

  tags = var.tags
}

module "security" {
  source = "../../modules/security"

  project_name      = var.project_name
  environment       = var.environment
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_provider_url = module.eks.oidc_provider_url

  tags = var.tags
}
