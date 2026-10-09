# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M2

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
# MAGIC ## Ejercicio 1

# COMMAND ----------

def agregar_ingreso(reserva: dict) -> dict:
    """Copia de la reserva con ingreso = tarifa × pasajeros."""
    return {**reserva, "ingreso": reserva["tarifa"] * reserva["pasajeros"]}


def aplicar_pasos(registros: list[dict], pasos: list[Paso]) -> list[dict]:
    """Aplica cada paso en orden a cada registro. Los pasos deben ser puros."""
    resultado = []
    for registro in registros:
        for paso in pasos:
            registro = paso(registro)
        resultado.append(registro)
    return resultado

# COMMAND ----------

resultado = aplicar_pasos(RESERVAS, [normalizar_origen, agregar_ingreso])

assert [r["origen"] for r in resultado] == ["BOG", "MDE", "CLO"]
assert [r["ingreso"] for r in resultado] == [320000.0, 560000.0, 1230000.0]
assert "ingreso" not in RESERVAS[0], "¡Se modificó el registro original!"
assert aplicar_pasos(RESERVAS, []) == RESERVAS, "Sin pasos, los registros quedan iguales"
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2

# COMMAND ----------

def en_lotes(registros: Iterable, tamano: int) -> Iterator[list]:
    """Produce listas de `tamano` elementos; el último lote puede ser menor."""
    if tamano < 1:
        raise ValueError(f"tamano debe ser >= 1, se recibió {tamano}")
    lote = []
    for registro in registros:
        lote.append(registro)
        if len(lote) == tamano:
            yield lote
            lote = []
    if lote:
        yield lote

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
# MAGIC ## Ejercicio 3

# COMMAND ----------

PDF_PRUEBA = pd.DataFrame(
    {
        "o_orderkey": [1, 2, 3, 4, 5],
        "o_orderstatus": ["F", "O", "F", "P", "O"],
        "o_totalprice": [100.0, 50.0, 25.0, 10.0, 70.0],
    }
)


def venta_por_estado(pdf: pd.DataFrame) -> pd.DataFrame:
    """Venta total y número de órdenes por estado, de mayor a menor venta."""
    return (
        pdf.groupby("o_orderstatus", as_index=False)
        .agg(venta_total=("o_totalprice", "sum"), n_ordenes=("o_orderkey", "count"))
        .sort_values("venta_total", ascending=False)
        .reset_index(drop=True)
    )

# COMMAND ----------

resultado_pd = venta_por_estado(PDF_PRUEBA)
assert list(resultado_pd.columns) == ["o_orderstatus", "venta_total", "n_ordenes"]
assert list(resultado_pd.itertuples(index=False, name=None)) == [("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1)]
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4

# COMMAND ----------

def venta_por_estado_spark(df: DataFrame) -> DataFrame:
    """Versión PySpark de venta_por_estado."""
    return (
        df.groupBy("o_orderstatus")
        .agg(
            F.sum("o_totalprice").alias("venta_total"),
            F.count("o_orderkey").alias("n_ordenes"),
        )
        .orderBy(F.col("venta_total").desc())
    )

# COMMAND ----------

resultado_spark = venta_por_estado_spark(spark.createDataFrame(PDF_PRUEBA))
assert resultado_spark.columns == ["o_orderstatus", "venta_total", "n_ordenes"]
assert [tuple(r) for r in resultado_spark.collect()] == [("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1)]
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

pdf_muestra = spark.table("samples.tpch.orders").limit(1000).toPandas()
pdf_muestra["o_totalprice"] = pdf_muestra["o_totalprice"].astype(float)

en_pandas = venta_por_estado(pdf_muestra)
en_spark = venta_por_estado_spark(spark.createDataFrame(pdf_muestra)).toPandas()

assert list(en_pandas["o_orderstatus"]) == list(en_spark["o_orderstatus"])
assert list(en_pandas["n_ordenes"]) == list(en_spark["n_ordenes"])
print("✅ pandas y Spark coinciden")
display(en_spark)
