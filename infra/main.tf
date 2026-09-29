data "aws_caller_identity" "current" {}

locals {
  account_id    = data.aws_caller_identity.current.account_id
  glue_database = replace(var.project, "-", "_") # Athena names can't contain hyphens
  warehouse     = "warehouse/${local.glue_database}"
}

# The bucket belongs to lakehouse-aws. This project only looks it up, so running
# `terraform destroy` here can never delete it.
data "aws_s3_bucket" "lake" {
  bucket = "${var.lake_bucket_prefix}-${local.account_id}"
}

# A Glue database of this project's own, stored in the shared lakehouse bucket
resource "aws_glue_catalog_database" "streaming" {
  name         = local.glue_database
  description  = "Streaming tables (bronze, silver, gold) from the streaming-lakehouse project"
  location_uri = "s3://${data.aws_s3_bucket.lake.bucket}/${local.warehouse}/"
}

# The role Spark runs as. Only your CLI user can use it, and it can reach this
# project's Glue database and S3 folder, nothing else.
resource "aws_iam_role" "spark" {
  name = "${var.project}-spark"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = data.aws_caller_identity.current.arn }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "spark" {
  name = "streaming-tables"
  role = aws_iam_role.spark.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "ListOnlyThisProjectsFolder"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = data.aws_s3_bucket.lake.arn
        Condition = { StringLike = { "s3:prefix" = [local.warehouse, "${local.warehouse}/*"] } }
      },
      {
        Sid      = "ReadWriteTableFiles"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "${data.aws_s3_bucket.lake.arn}/${local.warehouse}/*"
      },
      {
        Sid    = "ManageThisProjectsTables"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog",
          aws_glue_catalog_database.streaming.arn,
          "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${local.glue_database}/*",
        ]
      },
    ]
  })
}