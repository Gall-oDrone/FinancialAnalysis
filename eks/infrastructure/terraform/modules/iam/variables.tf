variable "cluster_name" {
  type        = string
  description = "EKS cluster name (prefix for IRSA role names)."
}

variable "oidc_provider_arn" {
  type        = string
  description = "EKS OIDC provider ARN."
}

variable "oidc_provider" {
  type        = string
  description = "OIDC issuer URL without https:// (same as EKS module oidc_provider_url)."
}

variable "irsa_policies" {
  type        = map(list(string))
  description = "Map of short role key to list of managed policy ARNs to attach (e.g. alb -> ElasticLoadBalancingFullAccess)."
  default     = {}
}
