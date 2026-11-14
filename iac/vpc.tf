resource "aws_vpc" "main" {
  cidr_block = "10.20.0.0/16"
  enable_dns_support = true
  enable_dns_hostnames = true
  tags = { Name = "${local.prefix}-vpc" }
}
resource "aws_subnet" "private" {
  vpc_id = aws_vpc.main.id
  cidr_block = "10.20.1.0/24"
  availability_zone = "${var.aws_region}a"
  map_public_ip_on_launch = false
  tags = { Name = "${local.prefix}-private" }
}
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  tags = { Name = "${local.prefix}-private-rt" }
}
resource "aws_route_table_association" "private" {
  subnet_id = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}
resource "aws_vpc_endpoint" "s3" {
  vpc_id = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = [aws_route_table.private.id]
  tags = { Name = "${local.prefix}-s3-endpoint" }
}
# La salida a internet para EMR solo se crea cuando enable_emr=true.
# EMR en subred privada necesita conectividad para dependencias y servicios AWS.
resource "aws_internet_gateway" "emr" {
  count = var.enable_emr ? 1 : 0
  vpc_id = aws_vpc.main.id
  tags = { Name = "${local.prefix}-emr-igw" }
}
resource "aws_subnet" "public" {
  count = var.enable_emr ? 1 : 0
  vpc_id = aws_vpc.main.id
  cidr_block = "10.20.2.0/24"
  availability_zone = "${var.aws_region}a"
  map_public_ip_on_launch = false
  tags = { Name = "${local.prefix}-nat-public" }
}
resource "aws_route_table" "public" {
  count = var.enable_emr ? 1 : 0
  vpc_id = aws_vpc.main.id
  route { cidr_block = "0.0.0.0/0", gateway_id = aws_internet_gateway.emr[0].id }
}
resource "aws_route_table_association" "public" {
  count = var.enable_emr ? 1 : 0
  subnet_id = aws_subnet.public[0].id
  route_table_id = aws_route_table.public[0].id
}
resource "aws_eip" "nat" {
  count = var.enable_emr ? 1 : 0
  domain = "vpc"
  depends_on = [aws_internet_gateway.emr]
}
resource "aws_nat_gateway" "emr" {
  count = var.enable_emr ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id = aws_subnet.public[0].id
  depends_on = [aws_route_table_association.public]
  tags = { Name = "${local.prefix}-emr-nat" }
}
resource "aws_route" "private_outbound" {
  count = var.enable_emr ? 1 : 0
  route_table_id = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id = aws_nat_gateway.emr[0].id
}
