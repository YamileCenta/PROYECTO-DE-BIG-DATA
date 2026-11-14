resource "aws_athena_workgroup" "sis" {
  name = "${local.prefix}-sis"
  force_destroy = false
  configuration {
    enforce_workgroup_configuration = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query = 1073741824
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena.bucket}/results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
