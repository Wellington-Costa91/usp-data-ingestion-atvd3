# Temporary fix file - apply this to fix the permissions issue

# Update Step Functions IAM policy with correct permissions
resource "aws_iam_role_policy" "step_functions_glue_fix" {
  name = "${var.environment}-step-functions-glue-fix"
  role = module.step_functions.step_functions_role_name

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
          "glue:GetJob",
          "glue:ListJobs"
        ]
        Resource = "*"
      }
    ]
  })
}
