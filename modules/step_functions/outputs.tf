output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.etl_pipeline.name
}

output "step_functions_role_name" {
  description = "Name of the Step Functions IAM role"
  value       = aws_iam_role.step_functions_role.name
}
