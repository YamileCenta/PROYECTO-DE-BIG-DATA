# ===========================================================
# ingest_sis.py
# Lee el dataset de afiliados del SIS con diagnóstico de
# Diabetes Mellitus desde HDFS (zona Raw, CSV) y lo convierte
# a Parquet.
#
# Uso: spark-submit scripts/ingesta/ingest_sis.py
# ===========================================================

import os
import sys
import findspark

os.environ["JAVA_HOME"]      = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"]     = "/opt/spark-3.5.1-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/home/bryanruiz/BigData_UPAO/bigdata_env/bin/python"

findspark.init()

from pyspark.sql import SparkSession

# -----------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------
HDFS_RAW_AFILIADOS = "hdfs://localhost:9000/raw/afiliados/Afiliados.csv"
HDFS_PARQUET_OUT   = "hdfs://localhost:9000/raw/parquet/afiliados_dm"

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("ingest_sis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 55)
print("  INGESTA SIS — Afiliados con Diabetes Mellitus")
print(f"  Fuente:  {HDFS_RAW_AFILIADOS}")
print(f"  Destino: {HDFS_PARQUET_OUT}")
print("=" * 55)

# -----------------------------------------------------------
# INGESTA
# -----------------------------------------------------------
try:
    print("\nLeyendo dataset de afiliados...")
    df_afiliados = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(HDFS_RAW_AFILIADOS)

    n_filas  = df_afiliados.count()
    n_cols   = len(df_afiliados.columns)
    print(f"  Filas leídas:   {n_filas:,}")
    print(f"  Columnas:       {n_cols}")
    print(f"  Columnas:       {df_afiliados.columns}")

    df_afiliados.createOrReplaceTempView("afiliados_dm")

    print(f"\nGuardando en Parquet: {HDFS_PARQUET_OUT} ...")
    df_afiliados.write.mode("overwrite").parquet(HDFS_PARQUET_OUT)

    print("\nVista previa (5 filas):")
    df_afiliados.show(5, truncate=False)

    print("\n" + "=" * 55)
    print("  Ingesta SIS completada exitosamente.")
    print("=" * 55)

except Exception as e:
    print(f"\nERROR durante la ingesta SIS: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
