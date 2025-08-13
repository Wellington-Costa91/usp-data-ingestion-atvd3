output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = module.s3.bucket_name
}

output "rds_endpoint" {
  description = "RDS endpoint"
  value       = module.rds.endpoint
}

output "step_function_arn" {
  description = "Step Function ARN"
  value       = module.step_functions.state_machine_arn
}

output "glue_jobs" {
  description = "Glue job names"
  value = {
    raw      = module.glue.raw_job_name
    trusted  = module.glue.trusted_job_name
    delivery = module.glue.delivery_job_name
  }
}

output "glue_connection_name" {
  description = "Glue connection name"
  value       = module.rds.glue_connection_name
}
