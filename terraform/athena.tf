resource "aws_athena_database" "crypto_db" {
  name   = "crypto_etl_db"
  bucket = aws_s3_bucket.athena_results.bucket
}

resource "aws_athena_workgroup" "etl_workgroup" {
  name = "${var.project_name}-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
    }
  }
}

# Named query de ejemplo para consultar datos
resource "aws_athena_named_query" "crypto_prices" {
  name      = "get_crypto_prices"
  workgroup = aws_athena_workgroup.etl_workgroup.id
  database  = aws_athena_database.crypto_db.name
  query     = <<EOF
SELECT
  symbol,
  name,
  current_price,
  market_cap,
  price_change_percentage_24h,
  ingestion_date
FROM crypto_prices
ORDER BY market_cap DESC
LIMIT 100;
EOF
}