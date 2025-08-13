# IAM Role for Step Functions
resource "aws_iam_role" "step_functions_role" {
  name = "${var.environment}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "step_functions_policy" {
  name = "${var.environment}-step-functions-policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:GetJob"
        ]
        Resource = [
          var.glue_job_raw_arn,
          var.glue_job_trusted_arn,
          var.glue_job_delivery_arn,
          "${replace(var.glue_job_raw_arn, ":job/", ":job/")}/*",
          "${replace(var.glue_job_trusted_arn, ":job/", ":job/")}/*",
          "${replace(var.glue_job_delivery_arn, ":job/", ":job/")}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${var.environment}-etl-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "ETL Pipeline with Raw, Trusted, and Delivery layers"
    StartAt = "RawLayer"
    States = {
      RawLayer = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "${var.environment}-raw-job"
        }
        Next = "TrustedLayer"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "FailState"
          }
        ]
      }
      TrustedLayer = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "${var.environment}-trusted-job"
        }
        Next = "DeliveryLayer"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "FailState"
          }
        ]
      }
      DeliveryLayer = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "${var.environment}-delivery-job"
        }
        Next = "SuccessState"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "FailState"
          }
        ]
      }
      SuccessState = {
        Type = "Succeed"
        Comment = "ETL Pipeline completed successfully"
      }
      FailState = {
        Type = "Fail"
        Comment = "ETL Pipeline failed"
      }
    }
  })

  tags = merge(var.tags, {
    Name = "${var.environment}-etl-pipeline"
  })
}
