variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "s3_lake_prefix" {
  description = "Prefixo base para os buckets do lake (ex: company-datalake)"
  type        = string
}

variable "glue_db_prefix" {
  description = "Prefixo para os databases do Glue (ex: dl)"
  type        = string
  default     = "dl"
}


variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for data storage"
  type        = string
  default = "exercicio2-usp"
}

variable "my_ip" {
  description = "S3 bucket name for data storage"
  type        = string
  default = "exercicio2-usp"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.20.0/24"]
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "etl_database"
}

variable "db_username" {
  description = "Database username"
  type        = string
  default     = "admin"
}

variable "db_password" {
  description = "Database password"
  type        = string
  sensitive   = true
  default = "SenhaForte123"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project = "ETL-Pipeline"
    Owner   = "DataTeam"
  }
}
