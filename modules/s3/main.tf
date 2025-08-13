# S3 bucket for data storage
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.bucket_name

  tags = merge(var.tags, {
    Name = "${var.bucket_name}-${var.environment}"
  })
}

resource "aws_s3_bucket_versioning" "data_bucket_versioning" {
  bucket = aws_s3_bucket.data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_bucket_encryption" {
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Create folder structure
resource "aws_s3_object" "folders" {
  for_each = toset([
    "raw-data/",
    "trusted-data/",
    "delivery-data/",
    "glue-scripts/",
    "glue-logs/"
  ])
  
  bucket  = aws_s3_bucket.data_bucket.id
  key     = each.value
  content = ""
}
