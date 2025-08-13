variable "s3_bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  type        = string
}

variable "db_connection_string" {
  description = "Database connection string"
  type        = string
  sensitive   = true
}

variable "glue_connection_name" {
  description = "Glue connection name for RDS"
  type        = string
}
variable "buckets" {
  description = "Buckets por camada"
  type = object({
    raw       = string
    trusted   = string
    delivered = string
  })
}

variable "glue_databases" {
  description = "Databases por camada"
  type = object({
    raw       = string
    trusted   = string
    delivered = string
  })
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
