# Infraestructura AWS Big Data SIS + MIMIC-IV

Región: `us-east-2` (Ohio). Terraform: archivos planos en `iac/`.

## Alcance inicial

VPC privada con endpoint S3, 3 buckets de datos/artefactos, bucket adicional para resultados Athena, IAM EMR, catálogo Glue y workgroup Athena. Sin CloudWatch configurado, sin AWS Budgets, sin EC2/EMR permanente. KMS opcional; cifrado SSE-S3 por defecto.

**No es gratuito por definición.** S3, peticiones, Athena, NAT, EMR, QuickSight y transferencia pueden cobrar. No existe límite automático de gasto. La región y las cuotas deben verificarse antes de aplicar.

## Uso

1. Configurar credenciales temporales mediante AWS CLI/SSO; nunca poner claves en .tf.
2. `cp terraform.tfvars.example terraform.tfvars` y cambiar `bucket_suffix` por uno único (sin datos sensibles).
3. `terraform init`
4. `terraform fmt -recursive && terraform validate`
5. `terraform plan -out=base.tfplan` y **revisar** la lista de recursos.
6. `terraform apply base.tfplan` (solo tras revisión del equipo).

## EMR opcional (costoso)

`terraform plan -var="enable_emr=true"` y revisar antes de `terraform apply -var="enable_emr=true"`. Al habilitarlo se crea también **NAT Gateway + Elastic IP** para salida desde la subred privada. El clúster tiene terminación por inactividad de 3600 s; NAT puede seguir facturando hasta desactivar EMR y aplicar la destrucción. Para apagar: `terraform plan -var="enable_emr=false"` y luego `terraform apply -var="enable_emr=false"` (manteniendo los mismos valores de otras variables). No usar `terraform destroy` para apagar EMR, porque borraría también recursos base. Confirmar que no queden instancias/NAT/EIP y revisar costos en la consola.

**Nota:** En EMR privado, las reglas de security groups administrados, el grupo de acceso de servicio, la conectividad a AWS y los permisos EMR v2 requieren validación en la cuenta real. El archivo es una base para revisión, no una garantía de despliegue sin ajustes. Los nodos EMR pueden generar registros del servicio aunque no se configure un grupo CloudWatch.

## MIMIC-IV

La autorización debe cubrir **esta cuenta, región, personas y modalidad de procesamiento**. Mantener MIMIC separado; no cargar datos identificables a SIS ni publicar MIMIC en QuickSight. Los permisos humanos se asignarán con IAM Identity Center antes de cargar MIMIC; la lista `allowed_mimic_principal_arns` es reservada y no aplica automáticamente una política de acceso. No cargar MIMIC hasta completar y comprobar el aislamiento IAM, incluidos permisos de operadores y EMR. El rol EMR actual tiene acceso a los tres buckets y debe usarse únicamente por operadores autorizados.

## Componentes pendientes de datos reales

Las tablas Glue y los datasets/dashboards QuickSight requieren conocer esquemas y resultados finales. `enable_quicksight` queda desactivado y reservado; aún no crea recursos QuickSight. `backend.tf` contiene un ejemplo comentado: el backend remoto debe crearse por bootstrap separado antes de activarlo. Mientras tanto el estado es local y debe protegerse; nunca subirlo a Git.

## Precauciones

Los buckets tienen `force_destroy=false` y versionado en los tres buckets de datos. El borrado requerirá vaciarlos explícitamente; evitar destruir los datos. No activar KMS sin revisar costos, política de clave y permisos. El tipo y versión de EMR son candidatos sujetos a disponibilidad regional.
