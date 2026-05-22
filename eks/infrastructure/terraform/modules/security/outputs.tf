# =============================================================================
# Financial Analysis - Security Module Outputs
# =============================================================================

output "cluster_autoscaler_role_arn" {
  description = "Cluster Autoscaler IAM role ARN"
  value       = aws_iam_role.cluster_autoscaler.arn
}

output "application_role_arn" {
  description = "Application IAM role ARN"
  value       = aws_iam_role.application.arn
}

output "data_bucket_name" {
  description = "S3 data bucket name"
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "S3 data bucket ARN"
  value       = aws_s3_bucket.data.arn
}

