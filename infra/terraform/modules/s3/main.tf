variable "project_name" {
  type = string
}

variable "raw_bucket_name" {
  type = string
}

variable "curated_bucket_name" {
  type = string
}

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket" "curated" {
  bucket = var.curated_bucket_name
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "transition-raw"
    status = "Enabled"

    expiration {
      days = 365
    }
  }
}

output "bucket_names" {
  value = {
    raw     = aws_s3_bucket.raw.bucket
    curated = aws_s3_bucket.curated.bucket
  }
}

