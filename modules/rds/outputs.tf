output "endpoint" {
  description = "RDS instance endpoint"
  value       = aws_db_instance.mysql.endpoint
}

output "connection_string" {
  description = "Database connection string"
  value       = "mysql://${var.db_username}:${var.db_password}@${aws_db_instance.mysql.endpoint}:3306/${var.db_name}"
  sensitive   = true
}

output "glue_connection_name" {
  description = "Glue connection name"
  value       = aws_glue_connection.mysql_connection.name
}

output "security_group_id" {
  description = "RDS security group ID"
  value       = aws_security_group.rds.id
}

output "glue_security_group_id" {
  description = "Glue connection security group ID"
  value       = aws_security_group.glue_connection.id
}
