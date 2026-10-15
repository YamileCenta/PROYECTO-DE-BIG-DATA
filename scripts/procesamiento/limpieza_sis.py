# ===========================================================
# clean_sis.py
# Limpieza, normalización e ingeniería de características
# sobre el dataset de afiliados SIS con Diabetes Mellitus.
# Lee desde Raw (CSV) y escribe en Processed (Parquet+Snappy).
#
# Uso: spark-submit scripts/procesamiento/clean_sis.py
# ===========================================================

import os
import sys
import findspark

os.environ["JAVA_HOME"]      = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"]     = "/opt/spark-3.5.1-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/home/bryanruiz/BigData_UPAO/bigdata_env/bin/python"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------
HDFS_RAW_SIS       = "hdfs://localhost:9000/raw/afiliados/Afiliados.csv"
HDFS_PROCESSED_SIS = "hdfs://localhost:9000/processed/sis/afiliados_limpio"

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("clean_sis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 55)
print("  LIMPIEZA SIS — Afiliados con Diabetes Mellitus")
print(f"  Fuente:  {HDFS_RAW_SIS}")
print(f"  Destino: {HDFS_PROCESSED_SIS}")
print("=" * 55)

try:
    # -----------------------------------------------------------
    # 1. CARGA
    # -----------------------------------------------------------
    print("\n[1/4] Cargando dataset SIS desde Raw...")
    df_sis = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(HDFS_RAW_SIS)

    print(f"  Filas originales: {df_sis.count():,}")

    # -----------------------------------------------------------
    # 2. NULOS, DUPLICADOS Y ATÍPICOS
    # -----------------------------------------------------------
    print("\n[2/4] Aplicando filtros de calidad...")

    # Nulos — filtro estricto en variable geográfica
    df_sis = df_sis.filter(F.col("DEPARTAMENTO").isNotNull())

    # Duplicados por clave primaria
    df_sis = df_sis.dropDuplicates(["CODIGO_ANONIMIZADO"])

    # Atípicos — rango biológico de edad
    df_sis = df_sis.filter(
        (F.col("EDAD") >= 0) & (F.col("EDAD") <= 120)
    )

    print(f"  Filas tras limpieza: {df_sis.count():,}")

    # -----------------------------------------------------------
    # 3. NORMALIZACIÓN DE FORMATOS
    # -----------------------------------------------------------
    print("\n[3/4] Normalizando columnas categóricas...")

    for c in ["DEPARTAMENTO", "SEXO", "TIPO_DIABETES"]:
        df_sis = df_sis.withColumn(c, F.upper(F.trim(F.col(c))))

    # -----------------------------------------------------------
    # 4. INGENIERÍA DE CARACTERÍSTICAS
    # -----------------------------------------------------------
    print("\n[4/4] Construyendo features derivadas...")

    # grupo_edad
    df_sis = df_sis.withColumn(
        "grupo_edad",
        F.when(F.col("EDAD").between(18, 44), "18-44")
         .when(F.col("EDAD").between(45, 64), "45-64")
         .otherwise("65+")
    )

    # sexo_homologado
    df_sis = df_sis.withColumn(
        "sexo_homologado",
        F.when(F.col("SEXO") == "MASCULINO", "M")
         .when(F.col("SEXO") == "FEMENINO", "F")
         .otherwise(F.col("SEXO"))
    )

    # -----------------------------------------------------------
    # 5. ESCRITURA EN PROCESSED
    # -----------------------------------------------------------
    print(f"\nEscribiendo en Parquet+Snappy: {HDFS_PROCESSED_SIS} ...")
    df_sis.coalesce(2).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(HDFS_PROCESSED_SIS)

    print("\n" + "=" * 55)
    print("  clean_sis.py completado exitosamente.")
    print(f"  Filas escritas: {df_sis.count():,}")
    print("=" * 55)

except Exception as e:
    print(f"\nERROR en clean_sis.py: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
