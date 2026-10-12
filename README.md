# PROYECTO-DE-BIG-DATA
Big Data y Analítica para la detección temprana de complicaciones críticas en pacientes diabéticos del SIS: comorbilidades, severidad y riesgo de reingreso.

## Descripción del proyecto

Proyecto desarrollado para el curso **Big Data y Analítica de Datos** de la Universidad Privada Antenor Orrego (UPAO).

La solución tiene como organización beneficiaria al **Seguro Integral de Salud (SIS)** y busca analizar información de afiliados con Diabetes Mellitus para apoyar la identificación temprana de perfiles con mayor riesgo de hospitalización o reingreso.

Se utilizan dos fuentes reales:

1. **Afiliados activos con diagnóstico de Diabetes Mellitus del SIS**, como fuente principal.
2. **MIMIC-IV v3.1**, como fuente clínica complementaria.

La integración entre ambas fuentes no se realiza por identidad de pacientes, debido a que pertenecen a poblaciones diferentes y no comparten identificadores. La unión se realiza mediante perfiles agregados construidos a partir de variables homologables.

---

## Objetivo

Desarrollar una solución de Big Data y analítica que permita estimar el riesgo de nueva hospitalización o reingreso en afiliados con diabetes mellitus del SIS, utilizando información demográfica, comorbilidades, antecedentes hospitalarios e indicadores complementarios derivados de MIMIC-IV.

---

## Arquitectura

La solución utiliza una arquitectura **On-premise** basada en:

- HDFS
- YARN
- Apache Spark / PySpark
- Spark SQL
- JupyterLab
- Apache Parquet
- Git / GitHub

El flujo general implementado es:

```text
Fuentes
   │
   ├── SIS
   │   └── afiliados.csv
   │
   └── MIMIC-IV v3.1
       └── hosp/*.csv.gz
          │
          ▼
      Ingesta Batch
          │
          ▼
     Raw / Bronze
          │
          ▼
   Procesamiento PySpark
          │
          ▼
   Processed / Silver
       Parquet
          │
          ▼
 Homologación de variables
          │
          ▼
 Perfiles agregados MIMIC
          │
          ▼
 LEFT JOIN SIS + MIMIC
          │
          ▼
 Consumption / Gold
          │
          ▼
 Analítica y modelado
```

---

# Pipeline de Big Data

## 1. Ingesta Batch — Raw / Bronze

Los archivos originales se cargan en HDFS sin modificar su estructura.

### Fuente SIS

```text
Afiliados activos con DM
→ afiliados.csv
```

### Fuente MIMIC-IV

Se utilizan tablas seleccionadas del módulo hospitalario `hosp`.

Entre las tablas consideradas se encuentran:

- `patients`
- `admissions`
- `diagnoses_icd`
- `d_icd_diagnoses`
- `labevents`
- `d_labitems`

El módulo `icu` no forma parte del alcance implementado.

La carga a HDFS se realiza mediante:

```bash
hdfs dfs -put archivo /ruta/hdfs/
```

Ejemplo:

```bash
hdfs dfs -mkdir -p /bigdata-diabetes/raw/sis/afiliados
hdfs dfs -mkdir -p /bigdata-diabetes/raw/mimic/hosp
```

La estructura base es:

```text
/bigdata-diabetes/
│
├── raw/
│   ├── sis/
│   │   └── afiliados/
│   │
│   └── mimic/
│       └── hosp/
│
├── silver/
│   ├── sis/
│   └── mimic/
│
└── gold/
```

---

## 2. Procesamiento y limpieza — Silver

Los datos almacenados en Raw son procesados mediante **PySpark**.

Las principales operaciones realizadas son:

- verificación del esquema;
- corrección de tipos;
- tratamiento de valores nulos;
- identificación de duplicados;
- validación de rangos;
- normalización de variables categóricas;
- transformación de fechas;
- construcción de variables derivadas;
- homologación de variables entre SIS y MIMIC-IV.

### Variables derivadas principales

Entre las características construidas se encuentran:

```text
grupo_edad
sexo_norm
tipo_dm_cod
valor_neto_atipico
valor_neto_log1p
```

Los grupos de edad utilizados son:

```text
<18
18-44
45-64
65+
```

La variable sexo se homologa entre ambas fuentes:

```text
MASCULINO → M
FEMENINO  → F
```

Los resultados procesados son almacenados en formato:

```text
Apache Parquet + compresión SNAPPY
```

en la zona:

```text
/bigdata-diabetes/silver/
```

---

## 3. Cálculo del reingreso en MIMIC-IV

El reingreso se calcula utilizando las hospitalizaciones de MIMIC-IV.

Se ordenan las admisiones de cada paciente mediante:

```text
subject_id
admittime
dischtime
```

Se considera reingreso cuando existe una nueva admisión del mismo paciente dentro de los **30 días posteriores al alta**.

La lógica general es:

```text
Hospitalización actual
        │
        ▼
     dischtime
        │
        ▼
 siguiente admisión
        │
        ▼
diferencia <= 30 días
        │
        ▼
   reingreso = 1
```

La tasa de reingreso se calcula únicamente sobre hospitalizaciones elegibles.

---

## 4. Integración SIS – MIMIC-IV

SIS y MIMIC-IV no comparten pacientes ni identificadores.

Por esta razón, no se realiza una unión mediante:

```text
CODIGO_ANONIMIZADO = subject_id
```

La integración se realiza mediante perfiles agregados.

Actualmente se utilizan:

```text
grupo_edad
sexo_norm
```

MIMIC-IV se agrupa para obtener métricas como:

```text
pacientes_unicos
ingresos
tasa_reingreso_30d
```

Se exige un mínimo de **30 pacientes MIMIC-IV por perfil** para considerar válida la métrica de referencia.

Posteriormente se realiza:

```text
SIS
LEFT JOIN
Métricas MIMIC
```

usando:

```text
grupo_edad + sexo_norm
```

Los afiliados SIS que no cuentan con un perfil equivalente en MIMIC-IV permanecen en el conjunto.

No se les asignan valores clínicos artificiales.

Se utiliza la variable:

```text
sin_ref_mimic
```

para identificar estos casos.

---

## 5. Zona Gold / Consumption

La zona Gold está destinada a conjuntos preparados para:

- análisis exploratorio;
- indicadores;
- modelado predictivo;
- visualización;
- consumo analítico.

Ruta:

```text
/bigdata-diabetes/gold/
```

Entre los productos analíticos se consideran:

```text
metricas_perfil_mimic
afiliados_enriquecidos
dataset_modelado
indicadores_riesgo
```

---

# Optimización del rendimiento

Se evaluó el uso de **Broadcast Join** para la integración entre el padrón SIS y la tabla agregada de perfiles MIMIC-IV.

La tabla MIMIC agregada es considerablemente menor que el conjunto principal SIS, por lo que puede distribuirse entre los ejecutores evitando un shuffle completo.

Resultados medidos:

| Estrategia | Mediana |
|---|---:|
| Standard Join — SortMergeJoin | 0.6188 s |
| Broadcast Join — BroadcastHashJoin | 0.4289 s |
| Mejora | 30.68 % |

La estrategia física fue comprobada mediante:

```python
df.explain("formatted")
```

---

# Prueba de escalabilidad

La prueba se realizó exclusivamente para evaluar rendimiento.

Los registros replicados **no se utilizan en el análisis ni en el modelo predictivo**.

### Pipeline principal

| Factor | Filas de entrada | Tiempo | RAM pico | Estado |
|---|---:|---:|---:|---|
| 1x | 7,814,429 | 35.32 s | 15.13 GB | OK |
| 10x | — | — | — | Omitido por capacidad |
| 100x | — | — | — | Omitido por capacidad |

El escenario 10x requería aproximadamente 4.3 GB adicionales de almacenamiento y el escenario 100x aproximadamente 43.4 GB, mientras que la VM disponía de alrededor de 4.1 GB libres.

Como prueba complementaria se evaluó una carga de mayor volumen.

| Factor | Tiempo | RAM pico | Spill memoria | Spill disco |
|---|---:|---:|---:|---:|
| 1x | 9.02 s | 14.79 GB | 0 GB | 0 GB |
| 2x | 14.65 s | 14.80 GB | 0 GB | 0 GB |
| 4x | 27.36 s | 15.13 GB | 0 GB | 0 GB |
| 8x | 83.09 s | 15.42 GB | 11.937 GB | 1.958 GB |

A partir de 8x se observó una degradación importante debido a operaciones de spill hacia disco.

---

# Línea base del negocio

La línea base se obtuvo a partir de los datos reales del SIS correspondientes al corte del **16/10/2025**.

| Indicador | Valor |
|---|---:|
| Afiliados diabéticos | 539,286 |
| Afiliados hospitalizados | 8,185 |
| Tasa de hospitalización | 1.52 % |
| Promedio de días hospitalizados | 12.73 días |
| Mediana de días hospitalizados | 7 días |
| Valor hospitalario promedio | S/ 2,785.10 |
| Mediana del valor hospitalario | S/ 1,238.66 |
| Hipertensión | 37.51 % |
| Obesidad / dislipidemia | 62.60 % |

La tasa de hospitalización de **1.52 %** constituye la línea base principal del proyecto.

---

# Requisitos del entorno

El proyecto fue desarrollado utilizando:

```text
Debian 12
Hadoop 3.3.6
HDFS
YARN
Apache Spark / PySpark 3.5.1
JupyterLab 4.6.3
Python 3
Apache Parquet
Git
```

---

# Ejecución del proyecto

## 1. Iniciar Hadoop

Verificar procesos:

```bash
jps
```

Deben estar activos, según la configuración utilizada:

```text
NameNode
DataNode
ResourceManager
NodeManager
```

---

## 2. Verificar HDFS

```bash
hdfs dfs -ls /
```

Luego:

```bash
hdfs dfs -ls -R /bigdata-diabetes
```

---

## 3. Iniciar JupyterLab

```bash
jupyter lab
```

---

## 4. Ejecutar notebooks

Orden recomendado:

```text
01_ingesta.ipynb
02_limpieza_sis.ipynb
03_limpieza_mimic.ipynb
04_integracion.ipynb
05_optimizacion.ipynb
06_escalabilidad.ipynb
07_eda.ipynb
08_modelo_base.ipynb
```

> Los nombres pueden variar de acuerdo con la estructura final del repositorio.

---

# Estructura del repositorio

```text
bigdata-diabetes-sis/
│
├── README.md
│
├── notebooks/
│   ├── 01_ingesta.ipynb
│   ├── 02_limpieza_sis.ipynb
│   ├── 03_limpieza_mimic.ipynb
│   ├── 04_integracion.ipynb
│   ├── 05_optimizacion.ipynb
│   └── 06_escalabilidad.ipynb
│
├── scripts/
│   ├── ingesta/
│   ├── procesamiento/
│   ├── calidad/
│   └── benchmark/
│
├── config/
│
├── docs/
│   ├── arquitectura/
│   └── evidencias/
│
├── sample_data/
│
└── .gitignore
```

---

# Datos

## SIS

Fuente:

**Seguro Integral de Salud – Plataforma Nacional de Datos Abiertos**

Dataset:

```text
Afiliados activos en el Seguro Integral de Salud
con diagnóstico de Diabetes Mellitus
```

El conjunto completo no necesariamente debe mantenerse dentro del repositorio.

---

## MIMIC-IV

MIMIC-IV v3.1 es distribuido mediante PhysioNet y requiere acceso credencializado.

Por sus condiciones de uso:

**Los archivos de MIMIC-IV no se incluyen en este repositorio.**

El usuario debe obtenerlos directamente desde PhysioNet y cumplir con las condiciones de acceso correspondientes.

---



# Limitaciones

- MIMIC-IV corresponde a una población hospitalaria de Estados Unidos y sus patrones no son directamente generalizables a los afiliados del SIS.
- La integración SIS–MIMIC se realiza mediante perfiles agregados y no mediante identidad individual.
- El entorno utilizado corresponde a máquinas virtuales académicas y no a infraestructura oficial del SIS.
- El módulo ICU de MIMIC-IV no forma parte del alcance implementado.
- Los resultados tienen fines académicos y de apoyo analítico; no constituyen diagnóstico ni recomendación médica.

---

# Equipo

| Integrante | Unidad 1 | Unidad 2 |
|---|---|---|
| Mariños Gonzalez, Luis Bryan | Coordinación | Ingeniería de datos |
| Rabanal Ocampo, Estalin | Ingeniería de datos | Calidad y documentación |
| Ruiz Tulumba, Bryan | Análisis y modelado | Coordinación |
| Centa Fernandez, Mayli | Calidad y documentación | Análisis y modelado |

---
