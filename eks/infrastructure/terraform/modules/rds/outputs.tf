# =============================================================================
# Financial Analysis - RDS Module Outputs
# =============================================================================

output "endpoint" {
  description = "RDS endpoint"
  value       = aws_db_instance.main.address
}

output "port" {
  description = "RDS port"
  value       = aws_db_instance.main.port
}

output "instance_id" {
  description = "RDS instance ID"
  value       = aws_db_instance.main.id
}

output "instance_arn" {
  description = "RDS instance ARN"
  value       = aws_db_instance.main.arn
}

output "database_name" {
  description = "Database name"
  value       = aws_db_instance.main.db_name
}

output "master_username" {
  description = "Master username"
  value       = aws_db_instance.main.username
}

output "secret_arn" {
  description = "Secrets Manager secret ARN"
  value       = aws_secretsmanager_secret.db.arn
}

output "security_group_id" {
  description = "RDS security group ID"
  value       = aws_security_group.rds.id
}

output "connection_string" {
  description = "Database connection string (without password)"
  value       = "postgresql://${aws_db_instance.main.username}@${aws_db_instance.main.address}:${aws_db_instance.main.port}/${aws_db_instance.main.db_name}"
  sensitive   = false
}

output "kms_key_arn" {
  description = "KMS key ARN for RDS encryption"
  value       = aws_kms_key.rds.arn
}

