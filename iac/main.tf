data "aws_caller_identity" "current" {}
locals {
  prefix = var.project_name
  common_bucket_names = {
    sis       = "${var.project_name}-sis-${var.bucket_suffix}"
    mimic     = "${var.project_name}-mimic-${var.bucket_suffix}"
    artifacts = "${var.project_name}-artifacts-${var.bucket_suffix}"
  }
}
