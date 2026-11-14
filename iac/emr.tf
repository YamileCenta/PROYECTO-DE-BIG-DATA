# EMR es OPT-IN. Incluye NAT Gateway mientras el cluster este habilitado.
# Requiere revisar cuotas EC2/EMR, release disponible y permisos de servicio.
resource "aws_security_group" "emr_service_access" {
  count = var.enable_emr ? 1 : 0
  name = "${local.prefix}-emr-service-access"
  description = "Service access EMR private subnet"
  vpc_id = aws_vpc.main.id
  ingress { from_port = 9443, to_port = 9443, protocol = "tcp", cidr_blocks = [aws_vpc.main.cidr_block] }
  egress { from_port = 0, to_port = 0, protocol = "-1", cidr_blocks = ["0.0.0.0/0"] }
  revoke_rules_on_delete = true
}
resource "aws_emr_cluster" "spark" {
  count = var.enable_emr ? 1 : 0
  name = "${local.prefix}-spark"
  release_label = var.emr_release_label
  applications = ["Spark"]
  service_role = aws_iam_role.emr_service.arn
  log_uri = "s3://${aws_s3_bucket.data["artifacts"].bucket}/logs/emr/"
  keep_job_flow_alive_when_no_steps = true
  termination_protection = false
  auto_termination_policy { idle_timeout = var.emr_idle_timeout_seconds }
  ec2_attributes {
    subnet_id = aws_subnet.private.id
    instance_profile = aws_iam_instance_profile.emr.arn
    service_access_security_group = aws_security_group.emr_service_access[0].id
  }
  master_instance_group { instance_type = var.emr_instance_type, instance_count = 1 }
  core_instance_group { instance_type = var.emr_instance_type, instance_count = 2 }
  depends_on = [aws_route.private_outbound, aws_iam_role_policy_attachment.emr_service, aws_iam_role_policy.emr_s3]
  tags = { Purpose = "temporary-spark" }
}
