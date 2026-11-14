resource "aws_s3_bucket" "data" {
  for_each = local.common_bucket_names
  bucket = each.value
  force_destroy = false
  tags = { DataDomain = each.key }
}
resource "aws_s3_bucket_public_access_block" "data" {
  for_each = aws_s3_bucket.data
  bucket = each.value.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = var.enable_kms ? "aws:kms" : "AES256"
      kms_master_key_id = var.enable_kms ? aws_kms_key.data[0].arn : null
    }
    bucket_key_enabled = var.enable_kms
  }
}
resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket = each.value.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket" "athena" {
  bucket = "${var.project_name}-athena-${var.bucket_suffix}"
  force_destroy = false
}
resource "aws_s3_bucket_public_access_block" "athena" {
  bucket = aws_s3_bucket.athena.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "athena" {
  bucket = aws_s3_bucket.athena.id
  rule { apply_server_side_encryption_by_default { sse_algorithm = "AES256" } }
}
