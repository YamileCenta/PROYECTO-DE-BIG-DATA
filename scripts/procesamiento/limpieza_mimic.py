# ===========================================================
# clean_mimic.py
# Limpieza y normalización de las tablas admissions y patients
# del módulo hosp de MIMIC-IV.
# Lee desde Raw (CSV.gz) y escribe en Processed (Parquet+Snappy).
#
# Uso: spark-submit scripts/procesamiento/clean_mimic.py
# ===========================================================

import os
import sys
import subprocess
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
HDFS_RAW_MIMIC            = "hdfs://localhost:9000/raw/mimic/"
HDFS_PROCESSED_ADMISSIONS = "hdfs://localhost:9000/processed/mimic/admissions_limpio"
HDFS_PROCESSED_PATIENTS   = "hdfs://localhost:9000/processed/mimic/patients_limpio"
HDFS_CHECKPOINTS          = "hdfs://localhost:9000/checkpoints"

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("clean_mimic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(HDFS_CHECKPOINTS)

print("=" * 55)
print("  LIMPIEZA MIMIC-IV — admissions y patients")
print(f"  Fuente:  {HDFS_RAW_MIMIC}")
print("=" * 55)

def limpiar_ruta(ruta):
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", ruta],
        capture_output=True
    )

try:
    # -----------------------------------------------------------
    # ADMISSIONS
    # -----------------------------------------------------------
    print("\n[1/2] Procesando admissions...")

    df_admissions = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{HDFS_RAW_MIMIC}admissions.csv.gz") \
        .dropDuplicates(["subject_id", "hadm_id"]) \
        .withColumn("admittime", F.to_timestamp("admittime")) \
        .withColumn("dischtime", F.to_timestamp("dischtime"))

    print(f"  Filas tras limpieza: {df_admissions.count():,}")

    # Checkpoint para cortar el plan de ejecución
    df_admissions = df_admissions.checkpoint()

    limpiar_ruta(HDFS_PROCESSED_ADMISSIONS)
    df_admissions.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(HDFS_PROCESSED_ADMISSIONS)

    print(f"  Escrito en: {HDFS_PROCESSED_ADMISSIONS}")

    # -----------------------------------------------------------
    # PATIENTS
    # -----------------------------------------------------------
    print("\n[2/2] Procesando patients...")

    df_patients = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{HDFS_RAW_MIMIC}patients.csv.gz") \
        .filter(
            (F.col("anchor_age") >= 0) & (F.col("anchor_age") <= 120)
        ) \
        .withColumn(
            "grupo_edad",
            F.when(F.col("anchor_age").between(18, 44), "18-44")
             .when(F.col("anchor_age").between(45, 64), "45-64")
             .otherwise("65+")
        ) \
        .withColumn("sexo_homologado", F.upper(F.col("gender")))

    print(f"  Filas tras limpieza: {df_patients.count():,}")

    df_patients = df_patients.checkpoint()

    limpiar_ruta(HDFS_PROCESSED_PATIENTS)
    df_patients.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(HDFS_PROCESSED_PATIENTS)

    print(f"  Escrito en: {HDFS_PROCESSED_PATIENTS}")

    # -----------------------------------------------------------
    # LIMPIAR CHECKPOINTS
    # -----------------------------------------------------------
    limpiar_ruta(HDFS_CHECKPOINTS)
    print("\nCheckpoints limpiados.")

    print("\n" + "=" * 55)
    print("  clean_mimic.py completado exitosamente.")
    print("=" * 55)

except Exception as e:
    print(f"\nERROR en clean_mimic.py: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
