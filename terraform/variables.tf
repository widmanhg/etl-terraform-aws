variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
  default     = "crypto-etl"
}

variable "environment" {
  description = "Entorno (dev, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Region de AWS"
  type        = string
  default     = "us-east-1"
}

variable "lambda_timeout" {
  description = "Timeout de Lambda en segundos"
  type        = number
  default     = 300
}

variable "schedule_expression" {
  description = "Frecuencia del pipeline"
  type        = string
  default     = "rate(1 day)"
}

variable "bucket_suffix" {
  description = "Sufijo unico para los buckets S3"
  type        = string
}