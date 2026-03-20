# Regla de schedule
resource "aws_cloudwatch_event_rule" "etl_schedule" {
  name                = "${var.project_name}-schedule"
  description         = "Ejecuta el pipeline ETL diariamente"
  schedule_expression = var.schedule_expression

  tags = {
    Environment = var.environment
  }
}

# Target: la Lambda
resource "aws_cloudwatch_event_target" "etl_lambda_target" {
  rule      = aws_cloudwatch_event_rule.etl_schedule.name
  target_id = "ETLLambdaTarget"
  arn       = aws_lambda_function.etl.arn
}

# Permiso para que EventBridge invoque la Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.etl.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.etl_schedule.arn
}