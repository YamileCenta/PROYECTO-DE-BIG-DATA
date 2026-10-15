# ===========================================================
# ingest_mimic.py
# Lee las tablas del módulo hosp de MIMIC-IV desde HDFS
# (zona Raw, formato CSV.gz) y las convierte a Parquet en Raw.
#
# Uso: spark-submit scripts/ingesta/ingest_mimic.py
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
HDFS_RAW_MIMIC   = "hdfs://localhost:9000/raw/mimic/"
HDFS_PARQUET_OUT = "hdfs://localhost:9000/raw/parquet/mimic/"

TABLAS = [
    "patients",
    "admissions",
    "diagnoses_icd",
    "d_icd_diagnoses",
    "procedures_icd",
    "d_icd_procedures",
    "drgcodes",
    "transfers",
    "labevents",
    "d_labitems",
]

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("ingest_mimic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 55)
print("  INGESTA MIMIC-IV — módulo hosp")
print(f"  Fuente:  {HDFS_RAW_MIMIC}")
print(f"  Destino: {HDFS_PARQUET_OUT}")
print("=" * 55)

# -----------------------------------------------------------
# INGESTA TABLA POR TABLA
# -----------------------------------------------------------
errores = []

for tabla in TABLAS:
    ruta_csv     = f"{HDFS_RAW_MIMIC}{tabla}.csv.gz"
    ruta_parquet = f"{HDFS_PARQUET_OUT}{tabla}"

    try:
        print(f"\nLeyendo: {tabla} ...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(ruta_csv)

        n_filas = df.count()
        print(f"  Filas leídas: {n_filas:,}")

        df.createOrReplaceTempView(tabla)

        df.write.mode("overwrite").parquet(ruta_parquet)
        print(f"  Guardado en Parquet: {ruta_parquet}")

    except Exception as e:
        msg = f"ERROR en tabla '{tabla}': {str(e)}"
        print(msg)
        errores.append(msg)

# -----------------------------------------------------------
# RESUMEN FINAL
# -----------------------------------------------------------
print("\n" + "=" * 55)
if errores:
    print(f"  Ingesta completada con {len(errores)} error(es):")
    for err in errores:
        print(f"    - {err}")
    sys.exit(1)
else:
    print(f"  Todas las tablas ({len(TABLAS)}) ingestadas exitosamente.")

print("=" * 55)
spark.stop()
