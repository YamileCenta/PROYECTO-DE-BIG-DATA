# ===========================================================
# benchmark_scalability.py
# Estima si hay espacio suficiente en HDFS para ejecutar el
# pipeline a 10x y 100x el volumen original, y reporta el
# resultado antes de intentar cualquier replicación.
#
# Prerequisitos:
#   - integrate_sis_mimic.py completado
#
# Uso: spark-submit scripts/benchmark/benchmark_scalability.py
# ===========================================================

import os
import sys
import re
import subprocess
import findspark

os.environ["JAVA_HOME"]      = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"]     = "/opt/spark-3.5.1-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/home/bryanruiz/BigData_UPAO/bigdata_env/bin/python"

findspark.init()

from pyspark.sql import SparkSession

# -----------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------
HDFS_RAW_SIS    = "hdfs://localhost:9000/raw/afiliados/Afiliados.csv"
HDFS_RAW_MIMIC  = "hdfs://localhost:9000/raw/mimic"

ESCALAS         = [10, 100]
RESERVA_BYTES   = 15 * 1024 ** 3   # 15 GiB de reserva mínima
FACTOR_ESPACIO  = 3.0               # Raw + temporales + Parquet final

# -----------------------------------------------------------
# INICIO DE SESIÓN SPARK
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("benchmark_scalability") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  BENCHMARK — Estimación de escalabilidad")
print(f"  Escalas:        {ESCALAS}")
print(f"  Reserva mínima: {round(RESERVA_BYTES / 1024**3, 2)} GiB")
print(f"  Factor espacio: {FACTOR_ESPACIO}x")
print("=" * 60)

# -----------------------------------------------------------
# HELPERS
# -----------------------------------------------------------
def bytes_hdfs(ruta):
    r = subprocess.run(
        ["hdfs", "dfs", "-du", "-s", ruta],
        capture_output=True, text=True, timeout=120
    )
    if r.returncode != 0:
        raise RuntimeError(f"Error al leer tamaño de {ruta}:\n{r.stderr}")
    partes = r.stdout.split()
    if not partes:
        raise RuntimeError(f"Sin salida para {ruta}")
    return int(partes[0])

def espacio_libre_hdfs():
    r = subprocess.run(
        ["hdfs", "dfsadmin", "-report"],
        capture_output=True, text=True, timeout=120
    )
    if r.returncode != 0:
        raise RuntimeError(f"Error al leer reporte HDFS:\n{r.stderr}")
    coincidencia = re.search(
        r"DFS Remaining:\s*([\d,]+)\s*(?:bytes)?",
        r.stdout, re.IGNORECASE
    )
    if not coincidencia:
        raise RuntimeError("No se pudo parsear DFS Remaining del reporte.")
    return int(coincidencia.group(1).replace(",", ""))

try:
    # -----------------------------------------------------------
    # 1. TAMAÑOS ACTUALES
    # -----------------------------------------------------------
    print("\n[1/2] Midiendo tamaño actual de los datasets...")

    tamano_sis   = bytes_hdfs(HDFS_RAW_SIS)
    tamano_mimic = bytes_hdfs(HDFS_RAW_MIMIC)
    tamano_total = tamano_sis + tamano_mimic
    libre        = espacio_libre_hdfs()

    print(f"  SIS (Raw):        {tamano_sis   / 1024**3:.3f} GiB")
    print(f"  MIMIC (Raw):      {tamano_mimic / 1024**3:.3f} GiB")
    print(f"  Total original:   {tamano_total / 1024**3:.3f} GiB")
    print(f"  Espacio libre:    {libre        / 1024**3:.3f} GiB")

    # -----------------------------------------------------------
    # 2. ESTIMACIÓN POR ESCALA
    # -----------------------------------------------------------
    print("\n[2/2] Estimando viabilidad por escala...")
    print(f"\n  {'Escala':<10} {'Estimado (GiB)':>16} {'Disponible (GiB)':>18} {'Viable':>8}")
    print(f"  {'-'*56}")

    resultados = []
    for escala in ESCALAS:
        estimado  = (tamano_total * escala * FACTOR_ESPACIO) + RESERVA_BYTES
        viable    = estimado <= libre
        resultados.append((escala, estimado, libre, viable))
        estado    = "SI" if viable else "NO"
        print(
            f"  {escala:<10} "
            f"{estimado / 1024**3:>16.2f} "
            f"{libre    / 1024**3:>18.2f} "
            f"{estado:>8}"
        )

    # -----------------------------------------------------------
    # RESUMEN
    # -----------------------------------------------------------
    print("\n" + "=" * 60)
    no_viables = [r for r in resultados if not r[3]]
    if no_viables:
        print("  RESULTADO: ESPACIO INSUFICIENTE para las siguientes escalas:")
        for escala, estimado, disponible, _ in no_viables:
            print(
                f"    - {escala}x: necesita {estimado/1024**3:.2f} GiB, "
                f"disponible {disponible/1024**3:.2f} GiB"
            )
        print("\n  Esto confirma que a partir de esa escala una sola")
        print("  máquina on-premise deja de ser suficiente.")
        print("=" * 60)
    else:
        print("  RESULTADO: Espacio suficiente para todas las escalas.")
        print("=" * 60)

except Exception as e:
    print(f"\nERROR en benchmark_scalability.py: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
