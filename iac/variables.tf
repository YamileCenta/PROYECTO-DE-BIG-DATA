variable "aws_region" { type = string, default = "us-east-2" }
variable "project_name" { type = string, default = "sis-mimic-bigdata" }
variable "bucket_suffix" {
  description = "Sufijo globalmente unico para los buckets; usar letras minusculas, numeros y guiones."
  type = string
  validation { condition = can(regex("^[a-z0-9][a-z0-9-]{3,30}[a-z0-9]$", var.bucket_suffix)), error_message = "Usa entre 5 y 32 caracteres validos." }
}
variable "enable_emr" { type = bool, default = false }
variable "enable_quicksight" { type = bool, default = false }
variable "enable_kms" { type = bool, default = false }
variable "emr_instance_type" { type = string, default = "m5.xlarge" }
variable "emr_release_label" { type = string, default = "emr-7.10.0" }
variable "emr_idle_timeout_seconds" { type = number, default = 3600 }
variable "quicksight_principal_arn" { type = string, default = "" }
variable "quicksight_edition" { type = string, default = "STANDARD" }
variable "allowed_mimic_principal_arns" {
  description = "ARN de roles/usuarios autorizados para leer MIMIC (ademas del rol EMR al habilitarlo)."
  type = list(string)
  default = []
}
