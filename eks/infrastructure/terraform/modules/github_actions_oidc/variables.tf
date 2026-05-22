# GitHub Actions OIDC → IAM role (ECR push, EKS deploy from workflows)

variable "name_prefix" {
  description = "Prefix for IAM role name (e.g. financial-analysis-dev-github-actions)"
  type        = string
}

variable "github_organization" {
  description = "GitHub org or user (must match OIDC sub claim, case-sensitive)"
  type        = string
}

variable "github_repository" {
  description = "Repository name without org (e.g. FinancialAnalysis)"
  type        = string
}

variable "ecr_repository_names" {
  description = "ECR repository names workflows may push to"
  type        = list(string)
  default = [
    "financial-analysis-scraper",
    "financial-analysis-etl",
  ]
}

variable "create_oidc_provider" {
  description = "Create aws_iam_openid_connect_provider for token.actions.githubusercontent.com. Set false if it already exists in the account (e.g. second stack); then use oidc_provider_arn."
  type        = bool
  default     = true
}

variable "oidc_provider_arn" {
  description = "When create_oidc_provider is false: optional override for the existing GitHub OIDC provider ARN. If empty, uses arn:aws:iam::<account>:oidc-provider/token.actions.githubusercontent.com."
  type        = string
  default     = ""
}

variable "enable_eks_access_entry" {
  description = "Grant this IAM role cluster access via EKS access entries (kubectl apply in GitHub Actions)"
  type        = bool
  default     = true

  validation {
    condition     = !var.enable_eks_access_entry || var.eks_cluster_name != ""
    error_message = "When enable_eks_access_entry is true, eks_cluster_name must be non-empty."
  }
}

variable "eks_cluster_name" {
  description = "EKS cluster name (required if enable_eks_access_entry is true)"
  type        = string
  default     = ""
}

variable "eks_access_policy_arn" {
  description = "EKS cluster access policy (default: cluster admin for deploy workflow)"
  type        = string
  default     = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
}

variable "tags" {
  description = "Tags for IAM resources"
  type        = map(string)
  default     = {}
}
