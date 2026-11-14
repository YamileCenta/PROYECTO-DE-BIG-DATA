resource "aws_kms_key" "data" {
  count = var.enable_kms ? 1 : 0
  description = "Clave opcional para datos del proyecto"
  enable_key_rotation = true
  deletion_window_in_days = 7
}
resource "aws_kms_alias" "data" {
  count = var.enable_kms ? 1 : 0
  name = "alias/${local.prefix}-data"
  target_key_id = aws_kms_key.data[0].key_id
}
