variable "aws_region" {
  description = "Region of the lakehouse built in lakehouse-aws"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Name used for this project's AWS resources"
  type        = string
  default     = "streaming-lakehouse"
}

variable "lake_bucket_prefix" {
  description = "The lakehouse-aws bucket is named <prefix>-<account id>"
  type        = string
  default     = "lakehouse"
}

variable "source_profile" {
  description = "The AWS CLI profile that holds your own credentials"
  type        = string
  default     = "default"
}