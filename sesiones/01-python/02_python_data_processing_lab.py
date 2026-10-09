# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M2 · De funciones a pipeline, y de pandas a Spark
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `aplicar_pasos` + `agregar_ingreso` | Composición de funciones puras |
# MAGIC | 2 | `en_lotes` | Generadores |
# MAGIC | 3 | `venta_por_estado` en pandas | Agregaciones con pandas |
# MAGIC | 4 | `venta_por_estado_spark` | El mismo cálculo en PySpark |
# MAGIC
# MAGIC Solución: [soluciones/02_python_data_processing_lab_solucion](./soluciones/02_python_data_processing_lab_solucion)

# COMMAND ----------

from collections.abc import Callable, Iterable, Iterator
from types import GeneratorType

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Paso = Callable[[dict], dict]

RESERVAS = [
    {"reserva_id": "RS001", "origen": " bog", "tarifa": 320000.0, "pasajeros": 1},
    {"reserva_id": "RS002", "origen": "mde", "tarifa": 280000.0, "pasajeros": 2},
    {"reserva_id": "RS003", "origen": "CLO ", "tarifa": 410000.0, "pasajeros": 3},
]


def normalizar_origen(reserva: dict) -> dict:
    return {**reserva, "origen": reserva["origen"].strip().upper()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · Composición de pasos
# MAGIC
# MAGIC 1. `agregar_ingreso(reserva)`: devuelve una **copia** con la llave `ingreso = tarifa * pasajeros`.
# MAGIC 2. `aplicar_pasos(registros, pasos)`: aplica en orden cada paso a cada registro y devuelve una lista **nueva**.

# COMMAND ----------

def agregar_ingreso(reserva: dict) -> dict:
    # TODO
    ...


def aplicar_pasos(registros: list[dict], pasos: list[Paso]) -> list[dict]:
    # TODO
    ...

# COMMAND ----------

resultado = aplicar_pasos(RESERVAS, [normalizar_origen, agregar_ingreso])

assert [r["origen"] for r in resultado] == ["BOG", "MDE", "CLO"]
assert [r["ingreso"] for r in resultado] == [320000.0, 560000.0, 1230000.0]
assert "ingreso" not in RESERVAS[0], "¡Se modificó el registro original!"
assert aplicar_pasos(RESERVAS, []) == RESERVAS, "Sin pasos, los registros quedan iguales"
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · Generador `en_lotes`
# MAGIC
# MAGIC Recibe cualquier iterable y produce listas de tamaño `tamano`; el último lote puede ser más pequeño.
# MAGIC Si `tamano < 1`, lanza `ValueError`.

# COMMAND ----------

def en_lotes(registros: Iterable, tamano: int) -> Iterator[list]:
    # TODO
    ...

# COMMAND ----------

assert isinstance(en_lotes([1, 2, 3], 2), GeneratorType), "Debe ser un generador (usa yield)"
assert list(en_lotes(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]
assert list(en_lotes([], 2)) == []
try:
    list(en_lotes([1], 0))
    raise AssertionError("Debió lanzar ValueError con tamano=0")
except ValueError:
    pass
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Agregación en pandas
# MAGIC
# MAGIC `venta_por_estado(pdf)` devuelve un DataFrame con las columnas `o_orderstatus`, `venta_total` (suma de `o_totalprice`) y `n_ordenes`.
# MAGIC Debe estar ordenado por `venta_total` descendente y tener el índice reiniciado.

# COMMAND ----------

PDF_PRUEBA = pd.DataFrame(
    {
        "o_orderkey": [1, 2, 3, 4, 5],
        "o_orderstatus": ["F", "O", "F", "P", "O"],
        "o_totalprice": [100.0, 50.0, 25.0, 10.0, 70.0],
    }
)


def venta_por_estado(pdf: pd.DataFrame) -> pd.DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado_pd = venta_por_estado(PDF_PRUEBA)
assert list(resultado_pd.columns) == ["o_orderstatus", "venta_total", "n_ordenes"]
assert list(resultado_pd.itertuples(index=False, name=None)) == [("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1)]
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4 · El mismo cálculo en PySpark
# MAGIC
# MAGIC `venta_por_estado_spark(df)`: mismas columnas y mismo orden, usando `groupBy` + `agg` y `F.sum` / `F.count` con `.alias()`.

# COMMAND ----------

def venta_por_estado_spark(df: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado_spark = venta_por_estado_spark(spark.createDataFrame(PDF_PRUEBA))
assert resultado_spark.columns == ["o_orderstatus", "venta_total", "n_ordenes"]
assert [tuple(r) for r in resultado_spark.collect()] == [("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1)]
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus · Mismo resultado sobre datos reales
# MAGIC
# MAGIC Aplicamos las dos versiones a la **misma** muestra de `samples.tpch.orders` y comparamos los resultados.

# COMMAND ----------

pdf_muestra = spark.table("samples.tpch.orders").limit(1000).toPandas()
pdf_muestra["o_totalprice"] = pdf_muestra["o_totalprice"].astype(float)

en_pandas = venta_por_estado(pdf_muestra)
en_spark = venta_por_estado_spark(spark.createDataFrame(pdf_muestra)).toPandas()

assert list(en_pandas["o_orderstatus"]) == list(en_spark["o_orderstatus"])
assert list(en_pandas["n_ordenes"]) == list(en_spark["n_ordenes"])
print("✅ pandas y Spark coinciden")
display(en_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Y ahora, sin muestra: la tabla completa en Spark

# COMMAND ----------

display(venta_por_estado_spark(spark.table("samples.tpch.orders")))
