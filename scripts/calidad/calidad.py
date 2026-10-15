# ===========================================================
# quality_check.py
# Valida completitud, rangos, coherencia y soporte estadístico
# sobre los datasets procesados. Genera un reporte de calidad
# en consola y termina con exit code 1 si alguna regla falla.
#
# Prerequisitos:
#   - integrate_sis_mimic.py completado
#
# Uso: spark-submit scripts/calidad/quality_check.py
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
HDFS_RAW_SIS              = "hdfs://localhost:9000/raw/afiliados/Afiliados.csv"
HDFS_RAW_ADMISSIONS       = "hdfs://localhost:9000/raw/mimic/admissions.csv.gz"
HDFS_PROCESSED_SIS        = "hdfs://localhost:9000/processed/sis/afiliados_limpio"
HDFS_PROCESSED_ADMISSIONS = "hdfs://localhost:9000/processed/mimic/admissions_limpio"
HDFS_PROCESSED_PATIENTS   = "hdfs://localhost:9000/processed/mimic/patients_limpio"
HDFS_CONSUMPTION_PERFIL   = "hdfs://localhost:9000/consumption/metricas_perfil_mimic"
HDFS_CONSUMPTION_RIESGO   = "hdfs://localhost:9000/consumption/riesgo_afiliados_sis"

SOPORTE_MINIMO = 30

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("quality_check") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  CONTROL DE CALIDAD DE DATOS")
print("=" * 60)

# -----------------------------------------------------------
# HELPERS
# -----------------------------------------------------------
def completitud(df, columna):
    total    = df.count()
    no_nulos = df.filter(F.col(columna).isNotNull()).count()
    pct      = round((no_nulos / total) * 100, 2) if total > 0 else 0.0
    return total, no_nulos, pct

def hdfs_size_mb(ruta):
    result = subprocess.run(
        ["hdfs", "dfs", "-du", "-s", ruta],
        capture_output=True, text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        bytes_total = int(result.stdout.strip().split()[0])
        return round(bytes_total / (1024 * 1024), 2)
    return None

reglas_fallidas = []

def registrar(ok, mensaje):
    estado = "OK" if ok else "FALLO"
    print(f"  [{estado}] {mensaje}")
    if not ok:
        reglas_fallidas.append(mensaje)

try:
    # -----------------------------------------------------------
    # 1. COMPLETITUD — SIS
    # -----------------------------------------------------------
    print("\n── 1. COMPLETITUD SIS ──────────────────────────────────")
    df_sis = spark.read \
        .option("header", "true").option("inferSchema", "true") \
        .option("encoding", "UTF-8").csv(HDFS_RAW_SIS)

    total_sis, nn_codigo, pct_codigo = completitud(df_sis, "CODIGO_ANONIMIZADO")
    registrar(pct_codigo == 100.0,
        f"CODIGO_ANONIMIZADO: {pct_codigo}% ({nn_codigo:,}/{total_sis:,})")

    _, nn_depto, pct_depto = completitud(df_sis, "DEPARTAMENTO")
    registrar(pct_depto >= 99.0,
        f"DEPARTAMENTO: {pct_depto}% completitud")

    # -----------------------------------------------------------
    # 2. COMPLETITUD — MIMIC
    # -----------------------------------------------------------
    print("\n── 2. COMPLETITUD MIMIC-IV ─────────────────────────────")
    df_admissions_raw = spark.read \
        .option("header", "true").option("inferSchema", "true") \
        .csv(HDFS_RAW_ADMISSIONS)

    total_mimic = df_admissions_raw.count()
    nn_subject  = df_admissions_raw.filter(F.col("subject_id").isNotNull()).count()
    nn_hadm     = df_admissions_raw.filter(F.col("hadm_id").isNotNull()).count()

    pct_subject = round(nn_subject / total_mimic * 100, 2)
    pct_hadm    = round(nn_hadm    / total_mimic * 100, 2)

    registrar(pct_subject == 100.0,
        f"subject_id: {pct_subject}% completitud")
    registrar(pct_hadm == 100.0,
        f"hadm_id: {pct_hadm}% completitud")

    # -----------------------------------------------------------
    # 3. RANGOS Y COHERENCIA — SIS
    # -----------------------------------------------------------
    print("\n── 3. RANGOS Y COHERENCIA SIS ──────────────────────────")

    edad_fuera = df_sis.filter(
        (F.col("EDAD") < 0) | (F.col("EDAD") > 120)
    ).count()
    registrar(edad_fuera == 0,
        f"Registros EDAD fuera de [0,120]: {edad_fuera}")

    valor_negativo = df_sis.filter(F.col("VALOR_NETO") < 0).count()
    registrar(valor_negativo == 0,
        f"Registros VALOR_NETO < 0: {valor_negativo}")

    atenciones_negativas = df_sis.filter(F.col("CANT_ATENCIONES") < 0).count()
    registrar(atenciones_negativas == 0,
        f"Registros CANT_ATENCIONES < 0: {atenciones_negativas}")

    # -----------------------------------------------------------
    # 4. SOPORTE ESTADÍSTICO MIMIC
    # -----------------------------------------------------------
    print("\n── 4. SOPORTE ESTADÍSTICO MIMIC ────────────────────────")
    df_perfil = spark.read.parquet(HDFS_CONSUMPTION_PERFIL)
    n_perfiles = df_perfil.count()
    registrar(n_perfiles > 0,
        f"Perfiles MIMIC con n>={SOPORTE_MINIMO}: {n_perfiles}")

    # -----------------------------------------------------------
    # 5. CONTEOS POR ZONA
    # -----------------------------------------------------------
    print("\n── 5. CONTEOS POR ZONA ─────────────────────────────────")
    zonas = [
        ("Processed / SIS",        HDFS_PROCESSED_SIS),
        ("Processed / admissions",  HDFS_PROCESSED_ADMISSIONS),
        ("Processed / patients",    HDFS_PROCESSED_PATIENTS),
        ("Consumption / perfil",    HDFS_CONSUMPTION_PERFIL),
        ("Consumption / riesgo SIS",HDFS_CONSUMPTION_RIESGO),
    ]
    for nombre, ruta in zonas:
        df_z = spark.read.parquet(ruta)
        n    = df_z.count()
        registrar(n > 0, f"{nombre}: {n:,} filas")

    # -----------------------------------------------------------
    # 6. COMPARATIVA DE TAMAÑO RAW vs PROCESSED/CONSUMPTION
    # -----------------------------------------------------------
    print("\n── 6. COMPARATIVA DE TAMAÑO ────────────────────────────")
    comparativas = [
        ("Afiliados SIS",
         "hdfs://localhost:9000/raw/afiliados/",
         HDFS_CONSUMPTION_RIESGO),
        ("admissions",
         "hdfs://localhost:9000/raw/mimic/admissions.csv.gz",
         HDFS_PROCESSED_ADMISSIONS),
        ("patients",
         "hdfs://localhost:9000/raw/mimic/patients.csv.gz",
         HDFS_PROCESSED_PATIENTS),
    ]

    print(f"\n  {'Dataset':<22} {'Raw (MB)':>10} {'Procesado (MB)':>16} {'Reducción':>12}")
    print(f"  {'-'*62}")
    for nombre, ruta_raw, ruta_proc in comparativas:
        raw_mb  = hdfs_size_mb(ruta_raw)
        proc_mb = hdfs_size_mb(ruta_proc)
        if raw_mb and proc_mb and raw_mb > 0:
            reduccion = ((raw_mb - proc_mb) / raw_mb) * 100
            print(f"  {nombre:<22} {raw_mb:>10.2f} {proc_mb:>16.2f} {reduccion:>11.1f}%")
        else:
            print(f"  {nombre:<22} {'N/D':>10} {'N/D':>16} {'N/D':>12}")

    # -----------------------------------------------------------
    # RESUMEN FINAL
    # -----------------------------------------------------------
    print("\n" + "=" * 60)
    if reglas_fallidas:
        print(f"  RESULTADO: {len(reglas_fallidas)} REGLA(S) FALLIDA(S):")
        for r in reglas_fallidas:
            print(f"    - {r}")
        print("=" * 60)
        sys.exit(1)
    else:
        print("  RESULTADO: TODAS LAS REGLAS DE CALIDAD SE CUMPLEN.")
        print("=" * 60)

except Exception as e:
    print(f"\nERROR en quality_check.py: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
