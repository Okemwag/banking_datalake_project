terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "storage" {
  source              = "./modules/s3"
  project_name        = var.project_name
  raw_bucket_name     = "${var.project_name}-raw"
  curated_bucket_name = "${var.project_name}-curated"
}

output "storage_buckets" {
  value = module.storage.bucket_names
}

