# ===========================================================
# integrate_sis_mimic.py
# Construye los perfiles clínicos agregados de MIMIC-IV y
# realiza el JOIN por perfil con el padrón SIS para generar
# el dataset analítico enriquecido en la zona Consumption.
#
# Prerequisitos:
#   - clean_sis.py        completado (processed/sis/afiliados_limpio)
#   - clean_mimic.py      completado (processed/mimic/admissions_limpio
#                                     processed/mimic/patients_limpio)
#
# Uso: spark-submit scripts/integracion/integrate_sis_mimic.py
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
HDFS_PROCESSED_SIS        = "hdfs://localhost:9000/processed/sis/afiliados_limpio"
HDFS_PROCESSED_ADMISSIONS = "hdfs://localhost:9000/processed/mimic/admissions_limpio"
HDFS_PROCESSED_PATIENTS   = "hdfs://localhost:9000/processed/mimic/patients_limpio"

HDFS_CONSUMPTION_PERFIL   = "hdfs://localhost:9000/consumption/metricas_perfil_mimic"
HDFS_CONSUMPTION_RIESGO   = "hdfs://localhost:9000/consumption/riesgo_afiliados_sis"
HDFS_CHECKPOINTS          = "hdfs://localhost:9000/checkpoints"

# Soporte estadístico mínimo por perfil (n >= 30 pacientes MIMIC)
SOPORTE_MINIMO = 30

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("integrate_sis_mimic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(HDFS_CHECKPOINTS)

print("=" * 55)
print("  INTEGRACIÓN SIS–MIMIC")
print(f"  Soporte mínimo por perfil: n >= {SOPORTE_MINIMO}")
print("=" * 55)

def limpiar_ruta(ruta):
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", ruta],
        capture_output=True
    )

try:
    # -----------------------------------------------------------
    # 1. CARGA DESDE PROCESSED
    # -----------------------------------------------------------
    print("\n[1/3] Cargando datos limpios desde Processed...")

    df_sis = spark.read.parquet(HDFS_PROCESSED_SIS)
    df_patients = spark.read.parquet(HDFS_PROCESSED_PATIENTS)
    df_admissions = spark.read.parquet(HDFS_PROCESSED_ADMISSIONS)

    print(f"  SIS:        {df_sis.count():,} registros")
    print(f"  patients:   {df_patients.count():,} registros")
    print(f"  admissions: {df_admissions.count():,} registros")

    # -----------------------------------------------------------
    # 2. CONSTRUCCIÓN DE PERFILES CLÍNICOS MIMIC
    # -----------------------------------------------------------
    print("\n[2/3] Construyendo perfiles clínicos MIMIC-IV...")

    df_mimic_perfil = df_patients.join(df_admissions, "subject_id") \
        .groupBy("grupo_edad", "sexo_homologado") \
        .agg(
            F.count("hadm_id").alias("total_hospitalizaciones"),
            F.countDistinct("subject_id").alias("pacientes_unicos")
        ) \
        .withColumn(
            "tasa_reingreso_promedio",
            F.col("total_hospitalizaciones") / F.col("pacientes_unicos")
        ) \
        .filter(F.col("pacientes_unicos") >= SOPORTE_MINIMO)

    df_mimic_perfil = df_mimic_perfil.checkpoint()

    print(f"  Perfiles válidos (n>={SOPORTE_MINIMO}): {df_mimic_perfil.count()}")
    df_mimic_perfil.show(truncate=False)

    limpiar_ruta(HDFS_CONSUMPTION_PERFIL)
    df_mimic_perfil.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(HDFS_CONSUMPTION_PERFIL)

    print(f"  Escrito en: {HDFS_CONSUMPTION_PERFIL}")

    # -----------------------------------------------------------
    # 3. JOIN POR PERFIL: afiliado SIS ↔ perfil MIMIC
    # -----------------------------------------------------------
    print("\n[3/3] Ejecutando JOIN por perfil (grupo_edad + sexo_homologado)...")

    df_riesgo = df_sis.join(
        df_mimic_perfil,
        on=["grupo_edad", "sexo_homologado"],
        how="left"
    ).withColumn(
        "tasa_reingreso_promedio",
        F.coalesce(F.col("tasa_reingreso_promedio"), F.lit(1.0))
    )

    df_riesgo = df_riesgo.checkpoint()

    print(f"  Dataset enriquecido: {df_riesgo.count():,} registros")

    limpiar_ruta(HDFS_CONSUMPTION_RIESGO)
    df_riesgo.coalesce(4).write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("DEPARTAMENTO") \
        .parquet(HDFS_CONSUMPTION_RIESGO)

    print(f"  Escrito en: {HDFS_CONSUMPTION_RIESGO}")

    # -----------------------------------------------------------
    # LIMPIAR CHECKPOINTS
    # -----------------------------------------------------------
    limpiar_ruta(HDFS_CHECKPOINTS)
    print("\nCheckpoints limpiados.")

    print("\n" + "=" * 55)
    print("  integrate_sis_mimic.py completado exitosamente.")
    print("=" * 55)

except Exception as e:
    print(f"\nERROR en integrate_sis_mimic.py: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
