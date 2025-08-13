# Main Terraform configuration for ETL Pipeline
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# VPC Module
module "vpc" {
  source = "./modules/vpc"
  
  environment           = var.environment
  vpc_cidr             = var.vpc_cidr
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  tags                 = var.tags
}

# S3 Module for data storage
module "s3" {
  source = "./modules/s3"
  
  bucket_name = var.s3_bucket_name
  environment = var.environment
  tags        = var.tags
}

# RDS Module for MySQL database
module "rds" {
  source = "./modules/rds"
  
  db_name            = var.db_name
  db_username        = var.db_username
  db_password        = var.db_password
  environment        = var.environment
  vpc_id             = module.vpc.vpc_id
  vpc_cidr_block     = module.vpc.vpc_cidr_block
  private_subnet_ids = module.vpc.private_subnet_ids
  tags               = var.tags
}

# Glue Module for ETL jobs
module "glue" {
  source = "./modules/glue"
  
  s3_bucket_name       = module.s3.bucket_name
  s3_bucket_arn        = module.s3.bucket_arn
  db_connection_string = module.rds.connection_string
  glue_connection_name = module.rds.glue_connection_name
  environment          = var.environment
  buckets = {
    "delivered" = "usp-datalake-delivered-108216259037-us-east-1"
    "raw" = "usp-datalake-raw-108216259037-us-east-1"
    "trusted" = "usp-datalake-trusted-108216259037-us-east-1"
  }

  glue_databases = {
    "delivered" = "usp_dl_delivered"
    "raw" = "usp_dl_raw"
    "trusted" = "usp_dl_trusted"
  }
  tags                 = var.tags
}

# Step Functions Module for orchestration
module "step_functions" {
  source = "./modules/step_functions"
  
  glue_job_raw_arn      = module.glue.raw_job_arn
  glue_job_trusted_arn  = module.glue.trusted_job_arn
  glue_job_delivery_arn = module.glue.delivery_job_arn
  environment           = var.environment
  tags                 = var.tags
}

# Upload data files from Dados folder to S3
locals {
  data_files = {
    for file in [
      "Reclamacoes/2021_tri_01.csv",
      "Reclamacoes/2021_tri_02.csv", 
      "Reclamacoes/2021_tri_03.csv",
      "Reclamacoes/2021_tri_04.csv",
      "Reclamacoes/2022_tri_01.csv",
      "Reclamacoes/2022_tri_03.csv",
      "Reclamacoes/2022_tri_04.csv",
      "Bancos/EnquadramentoInicia_v2.tsv",
      "Empregados/glassdoor_consolidado_join_match_v2.csv",
      "Empregados/glassdoor_consolidado_join_match_less_v2.csv"
    ] : file => file
    if fileexists("${path.root}/Dados/${file}")
  }
}

resource "aws_s3_object" "dados_upload" {
  for_each = local.data_files
  
  bucket = module.s3.bucket_name
  key    = "raw/${each.value}"
  source = "${path.root}/Dados/${each.value}"
  etag   = filemd5("${path.root}/Dados/${each.value}")
  
  depends_on = [module.s3]
}
