terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "crypto-etl-tfstate-gerardo"
    key    = "crypto-etl/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

output "lambda_function_name" {
  value = aws_lambda_function.etl.function_name
}

output "athena_database" {
  value = aws_athena_database.crypto_db.name
}