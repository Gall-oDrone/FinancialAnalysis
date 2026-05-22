# =============================================================================
# Financial Analysis - Production Environment (Modules)
# =============================================================================

locals {
  name = "${var.project_name}-${var.environment}"
}

module "vpc" {
  source = "../../modules/vpc"

  project_name             = var.project_name
  environment              = var.environment
  vpc_cidr                 = var.vpc_cidr
  enable_nat_gateway       = true
  single_nat_gateway       = false # Multi-AZ NAT for production
  enable_flow_logs         = true
  flow_logs_retention_days = 90

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
  node_instance_types   = ["m5.large", "m5.xlarge"]
  node_desired_capacity = 4
  node_min_size         = 3
  node_max_size         = 10
  node_disk_size        = 100
  use_spot_instances    = false # On-demand for production

  enable_public_access  = false # Private API endpoint for production
  enable_private_access = true

  tags = var.tags
}

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
    airflow = [
      "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
      "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    ]
  }
}

module "rds" {
  source = "../../modules/rds"

  project_name               = var.project_name
  environment                = var.environment
  vpc_id                     = module.vpc.vpc_id
  db_subnet_group_name       = module.vpc.database_subnet_group_name
  allowed_security_group_ids = [module.eks.node_security_group_id]

  instance_class              = "db.r5.large"
  allocated_storage           = 100
  max_allocated_storage       = 500
  db_name                     = var.db_name
  db_username                 = var.db_username
  multi_az                    = true # Multi-AZ for production
  backup_retention_period     = 30
  enable_performance_insights = true
  deletion_protection         = true # Prevent accidental deletion

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
