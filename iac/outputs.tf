output "aws_region" { value = var.aws_region }
output "s3_bucket_names" { value = { for k, b in aws_s3_bucket.data : k => b.bucket } }
output "athena_results_bucket" { value = aws_s3_bucket.athena.bucket }
output "glue_database" { value = aws_glue_catalog_database.sis_gold.name }
output "athena_workgroup" { value = aws_athena_workgroup.sis.name }
output "vpc_id" { value = aws_vpc.main.id }
output "emr_cluster_id" { value = var.enable_emr ? aws_emr_cluster.spark[0].id : null }
