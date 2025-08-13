# IAM Role for Glue Jobs
resource "aws_iam_role" "glue_role" {
  name = "${var.environment}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "${var.environment}-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*" 
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:AttachNetworkInterface",
          "ec2:DetachNetworkInterface"
        ]
        Resource = "*"
      }
    ]
  })
}

# Upload Glue scripts from separate files
resource "aws_s3_object" "glue_scripts" {
  for_each = {
    "raw_job.py"      = "${path.root}/glue_scripts/raw_job.py"
    "trusted_job.py"  = "${path.root}/glue_scripts/trusted_job.py"
    "delivery_job.py" = "${path.root}/glue_scripts/delivery_job.py"
  }
  
  bucket = var.s3_bucket_name
  key    = "glue-scripts/${each.key}"
  source = each.value
  etag   = filemd5(each.value)
}

# Glue Jobs
resource "aws_glue_job" "raw_job" {
  name     = "${var.environment}-raw-job"
  role_arn = aws_iam_role.glue_role.arn
  number_of_workers = 2
  worker_type = "G.1X"

  command {
    script_location = "s3://${var.s3_bucket_name}/glue-scripts/raw_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--enable-glue-datacatalog" = "true"
    "--S3_BUCKET"          = var.s3_bucket_name
    "--SRC_BUCKET"         = var.s3_bucket_name
    "--DST_BUCKET"         = var.buckets.raw
    "--GLUE_DB"            = var.glue_databases.raw
    "--additional-python-modules" = "pymysql"
  }

  max_retries = 0
  timeout     = 60
  glue_version = "4.0"

  tags = merge(var.tags, {
    Name = "${var.environment}-raw-job"
  })
}

resource "aws_glue_job" "trusted_job" {
  name     = "${var.environment}-trusted-job"
  role_arn = aws_iam_role.glue_role.arn
  number_of_workers = 2
  worker_type = "G.1X"

  command {
    script_location = "s3://${var.s3_bucket_name}/glue-scripts/trusted_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--S3_BUCKET"          = var.s3_bucket_name
    "--SRC_BUCKET"         = var.buckets.raw
    "--DST_BUCKET"         = var.buckets.trusted
    "--RAW_DB"            = var.glue_databases.raw
    "--TRUSTED_DB"            = var.glue_databases.trusted
    "--additional-python-modules" = "pymysql,pyarrow"
  }

  max_retries = 0
  timeout     = 60
  glue_version = "4.0"

  tags = merge(var.tags, {
    Name = "${var.environment}-trusted-job"
  })
}

resource "aws_glue_job" "delivery_job" {
  name     = "${var.environment}-delivery-job"
  role_arn = aws_iam_role.glue_role.arn
  number_of_workers = 2
  worker_type = "G.1X"

  command {
    script_location = "s3://${var.s3_bucket_name}/glue-scripts/delivery_job.py"
    python_version  = "3"
  }

  connections = [var.glue_connection_name]

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--S3_BUCKET"          = var.s3_bucket_name
    "--TRUSTED_DB"         = var.glue_databases.trusted
    "--additional-python-modules" = "pymysql,pyarrow"
  }

  max_retries = 0
  timeout     = 60
  glue_version = "4.0"

  tags = merge(var.tags, {
    Name = "${var.environment}-delivery-job"
  })
}
