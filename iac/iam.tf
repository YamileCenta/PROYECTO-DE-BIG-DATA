data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service", identifiers = ["elasticmapreduce.amazonaws.com"] }
  }
}
resource "aws_iam_role" "emr_service" {
  name = "${local.prefix}-emr-service"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}
resource "aws_iam_role_policy_attachment" "emr_service" {
  role = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service", identifiers = ["ec2.amazonaws.com"] }
  }
}
resource "aws_iam_role" "emr_ec2" {
  name = "${local.prefix}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}
resource "aws_iam_instance_profile" "emr" {
  name = "${local.prefix}-emr-profile"
  role = aws_iam_role.emr_ec2.name
}
data "aws_iam_policy_document" "emr_s3" {
  statement {
    sid = "ProjectBuckets"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.data : b.arn]
  }
  statement {
    sid = "ProjectObjects"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for b in aws_s3_bucket.data : "${b.arn}/*"]
  }
  dynamic "statement" {
    for_each = var.enable_kms ? [1] : []
    content {
      sid = "DataKey"
      actions = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
      resources = [aws_kms_key.data[0].arn]
    }
  }
}
resource "aws_iam_role_policy" "emr_s3" {
  name = "${local.prefix}-emr-s3"
  role = aws_iam_role.emr_ec2.id
  policy = data.aws_iam_policy_document.emr_s3.json
}
# No se crea un usuario IAM ni se distribuyen claves de acceso.
# Acceso humano a MIMIC: asignar identidades autorizadas con IAM Identity Center
# y permisos de lectura limitados al bucket MIMIC.
