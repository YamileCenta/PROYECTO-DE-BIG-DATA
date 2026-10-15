# ===========================================================
# benchmark_join.py
# Compara Standard Join vs Broadcast Join desactivando el
# auto-broadcast de Spark. Ejecuta 5 repeticiones de cada
# estrategia y reporta la mediana como métrica principal.
#
# Prerequisitos:
#   - integrate_sis_mimic.py completado
#
# Uso: spark-submit scripts/benchmark/benchmark_join.py
# ===========================================================

import os
import sys
import time
import statistics
import findspark

os.environ["JAVA_HOME"]      = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"]     = "/opt/spark-3.5.1-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/home/bryanruiz/BigData_UPAO/bigdata_env/bin/python"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, broadcast

# -----------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------
HDFS_CONSUMPTION_RIESGO = "hdfs://localhost:9000/consumption/riesgo_afiliados_sis"
HDFS_CONSUMPTION_PERFIL = "hdfs://localhost:9000/consumption/metricas_perfil_mimic"

REPETICIONES = 5

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("benchmark_join") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  BENCHMARK — Standard Join vs Broadcast Join")
print(f"  Repeticiones por estrategia: {REPETICIONES}")
print("=" * 60)

try:
    # -----------------------------------------------------------
    # 1. CARGA Y PREPARACIÓN
    # -----------------------------------------------------------
    print("\n[1/4] Cargando datasets desde Consumption...")

    df_afiliados_sis = spark.read.parquet(HDFS_CONSUMPTION_RIESGO)
    df_perfiles_mimic = spark.read.parquet(HDFS_CONSUMPTION_PERFIL)

    df_afiliados_edad = df_afiliados_sis.withColumn(
        "grupo_edad_calc",
        when(col("EDAD") < 45, "18-44")
        .when((col("EDAD") >= 45) & (col("EDAD") <= 64), "45-64")
        .otherwise("65+")
    )

    # Renombrar para evitar ambigüedad en el join
    df_perfiles_mimic_r = df_perfiles_mimic \
        .withColumnRenamed("grupo_edad", "grupo_edad_mimic")

    print(f"  Afiliados SIS:    {df_afiliados_edad.count():,} registros")
    print(f"  Perfiles MIMIC:   {df_perfiles_mimic_r.count()} registros")

    # -----------------------------------------------------------
    # 2. DESACTIVAR AUTO-BROADCAST
    # -----------------------------------------------------------
    print("\n[2/4] Desactivando auto-broadcast de Spark...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("  spark.sql.autoBroadcastJoinThreshold = -1")

    # -----------------------------------------------------------
    # 3. FUNCIÓN DE MEDICIÓN
    # -----------------------------------------------------------
    def medir_join(tipo, repeticiones):
        tiempos = []
        for i in range(repeticiones):
            inicio = time.perf_counter()

            if tipo == "normal":
                resultado = (
                    df_afiliados_edad.join(
                        df_perfiles_mimic_r,
                        df_afiliados_edad["grupo_edad_calc"]
                        == df_perfiles_mimic_r["grupo_edad_mimic"],
                        "inner"
                    )
                    .groupBy("DEPARTAMENTO", "grupo_edad_calc")
                    .count()
                )
            elif tipo == "broadcast":
                resultado = (
                    df_afiliados_edad.join(
                        broadcast(df_perfiles_mimic_r),
                        df_afiliados_edad["grupo_edad_calc"]
                        == df_perfiles_mimic_r["grupo_edad_mimic"],
                        "inner"
                    )
                    .groupBy("DEPARTAMENTO", "grupo_edad_calc")
                    .count()
                )

            resultado.count()
            fin = time.perf_counter()
            elapsed = fin - inicio
            tiempos.append(elapsed)
            print(f"    {tipo} — ejecución {i+1}: {elapsed:.4f} s")

        return tiempos

    # -----------------------------------------------------------
    # 4. EJECUCIÓN Y RESULTADOS
    # -----------------------------------------------------------
    print("\n[3/4] Ejecutando JOIN Normal (SortMergeJoin forzado)...")
    tiempos_normal = medir_join("normal", REPETICIONES)

    print("\n[4/4] Ejecutando JOIN Broadcast...")
    tiempos_broadcast = medir_join("broadcast", REPETICIONES)

    mediana_normal    = statistics.median(tiempos_normal)
    mediana_broadcast = statistics.median(tiempos_broadcast)
    mejora = ((mediana_normal - mediana_broadcast) / mediana_normal) * 100

    print("\n" + "=" * 60)
    print("  RESULTADOS")
    print("=" * 60)
    print(f"  {'Ejecución':<12} {'Normal (s)':>12} {'Broadcast (s)':>15}")
    print(f"  {'-'*42}")
    for i, (n, b) in enumerate(zip(tiempos_normal, tiempos_broadcast), 1):
        print(f"  {i:<12} {n:>12.4f} {b:>15.4f}")
    print(f"  {'-'*42}")
    print(f"  {'Mediana':<12} {mediana_normal:>12.4f} {mediana_broadcast:>15.4f}")
    print(f"\n  Mejora con Broadcast Join: {mejora:.2f}%")
    print("=" * 60)

    # -----------------------------------------------------------
    # PLANES DE EJECUCIÓN
    # -----------------------------------------------------------
    print("\n── PLAN — JOIN Normal:")
    df_afiliados_edad.join(
        df_perfiles_mimic_r,
        df_afiliados_edad["grupo_edad_calc"]
        == df_perfiles_mimic_r["grupo_edad_mimic"],
        "inner"
    ).explain("formatted")

    print("\n── PLAN — Broadcast JOIN:")
    df_afiliados_edad.join(
        broadcast(df_perfiles_mimic_r),
        df_afiliados_edad["grupo_edad_calc"]
        == df_perfiles_mimic_r["grupo_edad_mimic"],
        "inner"
    ).explain("formatted")

    # -----------------------------------------------------------
    # RESTAURAR CONFIGURACIÓN
    # -----------------------------------------------------------
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
    print("\nAuto-Broadcast restaurado al valor por defecto (10 MB).")

    print("\n" + "=" * 60)
    print("  benchmark_join.py completado exitosamente.")
    print("=" * 60)

except Exception as e:
    print(f"\nERROR en benchmark_join.py: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
