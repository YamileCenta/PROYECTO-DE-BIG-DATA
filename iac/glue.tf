resource "aws_glue_catalog_database" "sis_gold" {
  name = "sis_gold"
  description = "Catalogo de tablas Gold SIS; sin crawlers ni jobs permanentes"
}
# Registrar tablas cuando existan los Parquet y su esquema validado.
