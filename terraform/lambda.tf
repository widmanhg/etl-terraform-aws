# Empaquetar el código de Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/../lambda/etl.zip"
}

resource "aws_lambda_function" "etl" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-etl-function"
  role             = aws_iam_role.lambda_role.arn
  handler          = "etl.lambda_handler"
  runtime          = "python3.11"
  timeout          = var.lambda_timeout
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      RAW_BUCKET       = aws_s3_bucket.raw.bucket
      PROCESSED_BUCKET = aws_s3_bucket.processed.bucket
      ENVIRONMENT      = var.environment
    }
  }

  tags = {
    Name        = "ETL Lambda"
    Environment = var.environment
  }
}

# Logs de CloudWatch
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.etl.function_name}"
  retention_in_days = 14
}