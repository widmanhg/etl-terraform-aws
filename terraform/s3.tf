resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${var.bucket_suffix}"
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed-${var.bucket_suffix}"
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-${var.bucket_suffix}"
}