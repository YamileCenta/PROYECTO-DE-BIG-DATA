# PROYECTO-DE-BIG-DATA
Big Data y Analítica para la detección temprana de complicaciones críticas en pacientes diabéticos del SIS: comorbilidades, severidad y riesgo de reingreso.

## Descripción del proyecto

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

<img width="1600" height="549" alt="WhatsApp Image 2026-10-11 at 11 06 25 PM" src="https://github.com/user-attachments/assets/17478578-55bc-4aa1-bbc9-1592873879a7" />

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

La integración se encuentra implementada en:
```text
scripts/integracion/
```

Posteriormente se realiza:
```text
SIS
LEFT JOIN
Métricas agregadas MIMIC
```

utilizando:
```text
grupo_edad + sexo_norm
```

Los afiliados SIS que no cuentan con un perfil equivalente en MIMIC-IV permanecen en el conjunto.
No se asignan valores clínicos artificiales.
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
# Modelo base

Se implementó una Regresión Logística mediante Spark ML como línea base
técnica para el problema de clasificación.

Resultados obtenidos:

| Métrica | Resultado |
|---|---:|
| Accuracy | 79.86 % |
| AUC-ROC | 0.5724 |
| AUC-PR | 0.2453 |

El modelo base presenta una capacidad discriminativa limitada y dificultades para identificar correctamente la clase positiva con el umbral utilizado.
Estos resultados se utilizarán como línea base técnica para evaluar posteriormente mejoras mediante:
- ajuste del umbral de clasificación;
- tratamiento del desbalance de clases;
- incorporación de nuevas características;
- evaluación de modelos alternativos.

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

Los registros replicados **no se utilizan en el análisis exploratorio ni en el modelo predictivo**.

## Pipeline principal

| Factor | Filas de entrada | Espacio estimado requerido | Tiempo | RAM pico | Estado |
|---|---:|---:|---:|---:|---|
| 1x | 7,814,429 | 2.79 GB | 35.32 s | 15.13 GB | OK |
| 10x | — | 98.74 GB | — | — | Omitido por capacidad |
| 100x | — | 852.41 GB | — | — | Omitido por capacidad |

La máquina virtual disponía de aproximadamente **48.42 GB libres**.

Por ello, los escenarios 10x y 100x no fueron materializados para evitar la saturación del almacenamiento.

Como prueba complementaria se evaluó el comportamiento incremental sobre `labevents`.

| Factor | Tiempo | RAM pico | Spill memoria | Spill disco |
|---|---:|---:|---:|---:|
| 1x | 9.02 s | 14.79 GB | 0 GB | 0 GB |
| 2x | 14.65 s | 14.80 GB | 0 GB | 0 GB |
| 4x | 27.36 s | 15.13 GB | 0 GB | 0 GB |
| 8x | 83.09 s | 15.42 GB | 11.937 GB | 1.958 GB |

El escenario 8x fue el primero en presentar spill significativo y uso máximo de CPU, evidenciando una degradación del rendimiento.

Los scripts relacionados con estas pruebas se encuentran en:

```text
scripts/benchmark/
```

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
Git / GitHub
```

---

# Ejecución del proyecto

## 1. Iniciar Hadoop

Verificar los procesos:

```bash
jps
```

Según la configuración utilizada deben encontrarse activos:

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

## 3. Ejecutar el pipeline mediante scripts

La implementación reproducible del pipeline se encuentra organizada en la carpeta `scripts/`.

La secuencia recomendada de ejecución es la siguiente:

### 3.1. Ingesta de datos

Carga de la fuente principal SIS:

```bash
spark-submit scripts/ingesta/ingesta_sis.py
```

Carga de las tablas seleccionadas de MIMIC-IV:

```bash
spark-submit scripts/ingesta/ingesta_mimic.py
```

### 3.2. Procesamiento y limpieza

Limpieza y transformación de MIMIC-IV:

```bash
spark-submit scripts/procesamiento/limpieza_sis.py
```

Limpieza y transformación de MIMIC-IV:
```bash
spark-submit scripts/procesamiento/limpieza_mimic.py
```

### 3.3. Control de calidad

Aplicación de las reglas de calidad definidas para los datos procesados:
```bash
spark-submit scripts/calidad/calidad.py
```

### 3.4. Integración SIS – MIMIC-IV
Construcción de perfiles agregados e integración analítica mediante LEFT JOIN:
```bash
spark-submit scripts/integracion/integracion_sis_mimic.py
```

### 3.5. Optimización del JOIN

Evaluación del Standard Join frente al Broadcast Join:

```bash
spark-submit scripts/benchmark/join.py
```

3.6. Prueba de escalabilidad

Ejecución de las pruebas de rendimiento y escalabilidad:

```bash
spark-submit scripts/benchmark/escalabilidad.py
```

El orden general del pipeline es:
Ingesta SIS + MIMIC
        │
        ▼
Procesamiento y limpieza
        │
        ▼
Control de calidad
        │
        ▼
Integración SIS–MIMIC
        │
        ▼
Optimización del JOIN
        │
        ▼
Prueba de escalabilidad
---

## 4. Ejecutar notebooks

Los notebooks complementan la ejecución del pipeline y se utilizan principalmente para:

- inspección de datos;
- validación de resultados;
- análisis exploratorio;
- visualizaciones;
- modelado.

Para iniciar JupyterLab:

```bash
jupyter lab
```

Orden recomendado:

```text
notebooks/
├── 01_ingesta.ipynb
├── 02_limpieza_sis.ipynb
├── 03_limpieza_mimic.ipynb
├── 04_integracion.ipynb
├── 05_calidad.ipynb
├── 06_optimizacion.ipynb
├── 07_escalabilidad.ipynb
├── 08_eda.ipynb
└── 09_modelo_base.ipynb
```

Los **scripts** contienen las etapas reproducibles del pipeline, mientras que los **notebooks** permiten inspeccionar, validar, analizar y documentar los resultados obtenidos.

---

# Estructura del repositorio

```text
PROYECTO-DE-BIG-DATA/
│
├── README.md
├── .gitignore
│
├── notebooks/
│   ├── 01_ingesta.ipynb
│   ├── 02_limpieza_sis.ipynb
│   ├── 03_limpieza_mimic.ipynb
│   ├── 04_integracion.ipynb
│   ├── 05_calidad.ipynb
│   ├── 06_optimizacion.ipynb
│   ├── 07_escalabilidad.ipynb
│   ├── 08_eda.ipynb
│   └── 09_modelo_base.ipynb
│
├── scripts/
│   ├── benchmark/
│   ├── calidad/
│   ├── ingesta/
│   ├── integracion/
│   └── procesamiento/
│
├── docs/
│   ├── arquitectura/
│   │   └── arquitectura_final.png
│   │
│   └── evidencias/
│       ├── evidencia_hdfs/
│       ├── evidencia_optimizacion/
│       ├── evidencia_escalabilidad/
│       └── evidencia_modelo/
│
└── sample_data/
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

El conjunto completo puede obtenerse directamente desde la fuente institucional.

---

## MIMIC-IV

MIMIC-IV v3.1 es distribuido mediante **PhysioNet** y requiere acceso credencializado.

Por sus condiciones de uso:

**Los archivos de MIMIC-IV no se incluyen en este repositorio.**

El usuario debe obtenerlos directamente desde PhysioNet y cumplir con las condiciones de acceso correspondientes.

---

# Documentación y evidencias

## Arquitectura

La carpeta:

```text
docs/arquitectura/
```

contiene el diagrama correspondiente a la arquitectura final implementada en el proyecto.

---

## Evidencias

La carpeta:

```text
docs/evidencias/
```

contiene evidencias de las principales etapas ejecutadas, entre ellas:

- estructura de las zonas HDFS;
- escritura de datos en Silver y Gold;
- ejecución del pipeline;
- comparación Standard Join vs Broadcast Join;
- prueba de escalabilidad;
- resultados del análisis exploratorio;
- resultados del modelo base.

Estas evidencias complementan los scripts y notebooks del repositorio.

---

# Limitaciones

- MIMIC-IV corresponde a una población hospitalaria de Estados Unidos y sus patrones no son directamente generalizables a los afiliados del SIS.
- La integración SIS–MIMIC se realiza mediante perfiles agregados y no mediante identidad individual.
- El entorno utilizado corresponde a máquinas virtuales académicas y no a infraestructura oficial del SIS.
- El módulo ICU de MIMIC-IV no forma parte del alcance implementado.
- Los escenarios 10x y 100x del pipeline completo no fueron materializados debido a restricciones de almacenamiento de la máquina virtual.
- Los resultados tienen fines académicos y de apoyo analítico; no constituyen diagnóstico ni recomendación médica.

---

# Equipo de trabajo

**Grupo 8**

| Integrante | Unidad 1 | Unidad 2 |
|---|---|---|
| Mariños Gonzalez, Luis Bryan | Coordinación | Ingeniería de datos |
| Rabanal Ocampo, Estalin | Ingeniería de datos | Calidad y documentación |
| Ruiz Tulumba, Bryan | Análisis y modelado | Coordinación |
| Centa Fernandez, Mayli | Calidad y documentación | Análisis y modelado |

---
