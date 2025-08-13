output "raw_job_name" {
  description = "Name of the raw Glue job"
  value       = aws_glue_job.raw_job.name
}

output "raw_job_arn" {
  description = "ARN of the raw Glue job"
  value       = aws_glue_job.raw_job.arn
}

output "trusted_job_name" {
  description = "Name of the trusted Glue job"
  value       = aws_glue_job.trusted_job.name
}

output "trusted_job_arn" {
  description = "ARN of the trusted Glue job"
  value       = aws_glue_job.trusted_job.arn
}

output "delivery_job_name" {
  description = "Name of the delivery Glue job"
  value       = aws_glue_job.delivery_job.name
}

output "delivery_job_arn" {
  description = "ARN of the delivery Glue job"
  value       = aws_glue_job.delivery_job.arn
}
