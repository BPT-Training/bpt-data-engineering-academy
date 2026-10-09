# Databricks notebook source
# MAGIC %md
# MAGIC # M2 · Python for Data Processing
# MAGIC
# MAGIC **Objetivo:** pasar de funciones sueltas a procesos reutilizables, y entender cuándo saltar de pandas a Spark.
# MAGIC
# MAGIC | Bloque | Tema |
# MAGIC |---|---|
# MAGIC | 1 | Funciones como bloques: composición de pasos |
# MAGIC | 2 | Comprensiones: list, dict, set |
# MAGIC | 3 | Generadores: procesar sin cargar todo en memoria |
# MAGIC | 4 | pandas sobre una muestra de `samples.tpch` |
# MAGIC | 5 | El mismo cálculo en PySpark: la transición |
# MAGIC
# MAGIC > Guía: [01-python.md §3 y §5](../../guia-buenas-practicas/01-python.md)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Funciones como bloques: composición de pasos
# MAGIC
# MAGIC En Python las funciones son objetos: se pueden guardar en una lista y aplicar en orden.
# MAGIC Así, un "pipeline" es una lista de pasos pequeños, fáciles de probar y de reordenar.

# COMMAND ----------

from collections.abc import Callable

Paso = Callable[[dict], dict]

vuelos = [
    {"vuelo_id": "VL0001", "origen": " bog", "destino": "MDE", "retraso_min": 5},
    {"vuelo_id": "VL0002", "origen": "mde", "destino": "clo", "retraso_min": 42},
    {"vuelo_id": "VL0003", "origen": "CLO", "destino": "CTG", "retraso_min": 120},
]


def normalizar_aeropuertos(vuelo: dict) -> dict:
    return {**vuelo, "origen": vuelo["origen"].strip().upper(), "destino": vuelo["destino"].strip().upper()}


def marcar_puntualidad(vuelo: dict) -> dict:
    return {**vuelo, "a_tiempo": vuelo["retraso_min"] <= 15}


def ejecutar_pipeline(registro: dict, pasos: list[Paso]) -> dict:
    for paso in pasos:
        registro = paso(registro)
    return registro


PASOS = [normalizar_aeropuertos, marcar_puntualidad]
for v in vuelos:
    print(ejecutar_pipeline(v, PASOS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Comprensiones
# MAGIC
# MAGIC Úsalas para transformaciones **de una línea**. Si necesitas varios `if` o anidar, un `for` explícito se lee mejor.

# COMMAND ----------

limpios = [ejecutar_pipeline(v, PASOS) for v in vuelos]

ids_retrasados = [v["vuelo_id"] for v in limpios if not v["a_tiempo"]]          # list
retraso_por_vuelo = {v["vuelo_id"]: v["retraso_min"] for v in limpios}        # dict
aeropuertos = {v["origen"] for v in limpios} | {v["destino"] for v in limpios} # set

print(ids_retrasados)
print(retraso_por_vuelo)
print(sorted(aeropuertos))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generadores
# MAGIC
# MAGIC Un generador produce valores **bajo demanda** (`yield`) en lugar de construir la lista completa.
# MAGIC Sirve para leer archivos grandes línea a línea o procesar por lotes.

# COMMAND ----------

import sys

lista = [i * 2 for i in range(1_000_000)]
generador = (i * 2 for i in range(1_000_000))

print(f"Lista:     {sys.getsizeof(lista):>12,} bytes")
print(f"Generador: {sys.getsizeof(generador):>12,} bytes")
print("Suma igual:", sum(lista) == sum(generador))

# COMMAND ----------

def leer_registros(n: int):
    """Simula leer un archivo enorme registro a registro."""
    for i in range(n):
        yield {"id": i, "valor": i % 7}


def solo_pares(registros):
    for r in registros:
        if r["valor"] % 2 == 0:
            yield r


# Nada se ejecuta hasta que alguien consume el generador
flujo = solo_pares(leer_registros(10))
print(next(flujo))
print(list(flujo))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. pandas sobre una muestra
# MAGIC
# MAGIC pandas corre en **un solo nodo** (el driver). Por eso siempre trabajamos sobre una muestra acotada.
# MAGIC
# MAGIC ⚠️ **Regla BPT:** nunca uses `toPandas()` sin `limit`, un filtro fuerte o una agregación antes.

# COMMAND ----------

df_muestra = spark.table("samples.tpch.orders").limit(1000)
pdf_ordenes = df_muestra.toPandas()

print(pdf_ordenes.shape)
pdf_ordenes.head()

# COMMAND ----------

pdf_ordenes["o_totalprice"] = pdf_ordenes["o_totalprice"].astype(float)   # viene como Decimal

resumen_pandas = (
    pdf_ordenes
    .groupby("o_orderpriority", as_index=False)
    .agg(venta_total=("o_totalprice", "sum"), n_ordenes=("o_orderkey", "count"))
    .sort_values("o_orderpriority")
)
resumen_pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. El mismo cálculo en PySpark
# MAGIC
# MAGIC La sintaxis se parece mucho, pero hay dos diferencias clave:
# MAGIC
# MAGIC | | pandas | PySpark |
# MAGIC |---|---|---|
# MAGIC | Ejecución | Inmediata (*eager*) | Diferida (*lazy*): nada corre hasta una acción (`display`, `count`, `write`) |
# MAGIC | Escala | Memoria de un nodo | Distribuido en el cluster |
# MAGIC
# MAGIC Aquí corremos sobre **la tabla completa**, no sobre la muestra.

# COMMAND ----------

from pyspark.sql import functions as F

resumen_spark = (
    spark.table("samples.tpch.orders")
    .groupBy("o_orderpriority")
    .agg(
        F.sum("o_totalprice").alias("venta_total"),
        F.count("o_orderkey").alias("n_ordenes"),
    )
    .orderBy("o_orderpriority")
)
display(resumen_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### pandas API on Spark: el puente
# MAGIC
# MAGIC Si ya tienes código pandas, `pyspark.pandas` permite ejecutarlo distribuido casi sin cambios. Para código **nuevo**, BPT recomienda escribir PySpark directamente.

# COMMAND ----------

import pyspark.pandas as ps

psdf = ps.read_table("samples.tpch.orders")
psdf.groupby("o_orderpriority")["o_totalprice"].sum().sort_index()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente: [02_python_data_processing_lab](./02_python_data_processing_lab)
