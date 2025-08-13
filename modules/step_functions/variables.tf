variable "glue_job_raw_arn" {
  description = "ARN of the raw Glue job"
  type        = string
}

variable "glue_job_trusted_arn" {
  description = "ARN of the trusted Glue job"
  type        = string
}

variable "glue_job_delivery_arn" {
  description = "ARN of the delivery Glue job"
  type        = string
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
