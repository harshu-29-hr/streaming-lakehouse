terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Every resource gets this tag, so costs can be traced back to the project later
  default_tags {
    tags = { project = var.project }
  }
}