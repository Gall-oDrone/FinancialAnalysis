output "github_actions_role_arn" {
  description = "Paste into GitHub secret AWS_GITHUB_ACTIONS_ROLE_ARN (OIDC role for workflows)"
  value       = aws_iam_role.github_actions.arn
}

output "github_actions_role_name" {
  description = "IAM role name for GitHub Actions"
  value       = aws_iam_role.github_actions.name
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN for token.actions.githubusercontent.com (account-level)"
  value       = local.oidc_provider_arn
}
