variable "project_name" {
  type        = string
  description = "Base project name used for resource naming."
  default     = "banking-lakehouse"
}

variable "aws_region" {
  type        = string
  description = "AWS region for Terraform-managed resources."
  default     = "us-east-1"
}

