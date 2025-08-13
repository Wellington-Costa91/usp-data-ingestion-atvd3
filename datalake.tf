############################################
# data sources
############################################
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

############################################
# locals
############################################
locals {
  layers = ["raw", "trusted", "delivered"]

  # nomes únicos de bucket: <prefix>-<layer>-<account>-<region>
  bucket_names = {
    for l in local.layers :
    l => lower(replace("${var.s3_lake_prefix}-${l}-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}", "_", "-"))
  }
}

############################################
# S3 buckets (um por camada)
############################################
resource "aws_s3_bucket" "layer" {
  for_each = local.bucket_names
  bucket   = each.value

  tags = merge(var.tags, {
    Name   = each.value
    Layer  = each.key
    Module = "lake-buckets"
  })
}

# Bloqueio de acesso público
resource "aws_s3_bucket_public_access_block" "layer" {
  for_each                = aws_s3_bucket.layer
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Criptografia padrão (SSE-S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versionamento (desativado por padrão; ajuste se desejar)
resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  versioning_configuration {
    status = "Disabled"
  }
}

############################################
# Glue Databases (um por camada)
############################################
resource "aws_glue_catalog_database" "db" {
  for_each = local.bucket_names

  name = "${var.glue_db_prefix}_${each.key}"

  # Aponta o DB para a raiz do bucket da camada correspondente
  location_uri = "s3://${each.value}/"

  # (Opcional) parâmetros de organização
  parameters = {
    "created_by" = "terraform"
    "layer"      = each.key
  }
}

############################################
# outputs
############################################
output "buckets" {
  description = "Buckets criados por camada"
  value       = { for k, v in aws_s3_bucket.layer : k => v.bucket }
}

output "glue_databases" {
  description = "Databases do Glue por camada"
  value       = { for k, v in aws_glue_catalog_database.db : k => v.name }
}
