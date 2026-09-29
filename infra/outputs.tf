output "bucket_name" {
  value = data.aws_s3_bucket.lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.streaming.name
}

output "warehouse" {
  value = "s3://${data.aws_s3_bucket.lake.bucket}/${local.warehouse}"
}

output "spark_role_arn" {
  value = aws_iam_role.spark.arn
}

output "aws_profile" {
  description = "Profile to add to ~/.aws/config so Spark runs as the role"
  value       = <<-EOT
    [profile ${var.project}]
    role_arn = ${aws_iam_role.spark.arn}
    source_profile = ${var.source_profile}
    region = ${var.aws_region}
  EOT
}