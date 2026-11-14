# Bootstrap opcional: configurar el backend remoto solo después de crear
# un bucket de estado independiente, con versionado y cifrado.
# NO habilitar este bloque sin crear previamente el bucket.
# terraform {
#   backend "s3" {
#     bucket       = "REEMPLAZAR-BUCKET-ESTADO"
#     key          = "bigdata/iac/terraform.tfstate"
#     region       = "us-east-2"
#     encrypt      = true
#     use_lockfile = true
#   }
# }
