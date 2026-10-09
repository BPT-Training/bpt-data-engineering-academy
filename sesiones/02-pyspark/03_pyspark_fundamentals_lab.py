# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M3 · PySpark Fundamentals
# MAGIC
# MAGIC Cada ejercicio es una función `DataFrame → DataFrame` (regla BPT: la lógica en funciones probables).
# MAGIC Primero se prueba con DataFrames pequeños de resultado conocido, y luego se aplica a `samples.tpch`.
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `venta_por_nacion` | Joins + agregación |
# MAGIC | 2 | `clientes_sin_ordenes` | `left_anti` |
# MAGIC | 3 | `ordenes_urgentes_por_anio` | `filter` + `groupBy` |
# MAGIC | 4 | `agregar_nacion` | Broadcast join |
# MAGIC | 5 | `agregar_nacion_seguro` | Corregir un join que duplica filas |
# MAGIC
# MAGIC Solución: [soluciones/03_pyspark_fundamentals_lab_solucion](./soluciones/03_pyspark_fundamentals_lab_solucion)

# COMMAND ----------

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CLIENTES = spark.createDataFrame(
    [(1, "Ana", 1, "BUILDING"), (2, "Luis", 1, "AUTOMOBILE"), (3, "Sofía", 2, "BUILDING"), (4, "Pedro", 3, "MACHINERY")],
    "c_custkey long, c_name string, c_nationkey long, c_mktsegment string",
)
ORDENES = spark.createDataFrame(
    [
        (10, 1, "F", 100.0, date(2025, 1, 5), "1-URGENT"),
        (11, 1, "O", 50.0, date(2025, 2, 10), "3-MEDIUM"),
        (12, 2, "F", 200.0, date(2025, 1, 20), "1-URGENT"),
        (13, 3, "F", 75.0, date(2026, 3, 1), "2-HIGH"),
        (14, 3, "P", 25.0, date(2026, 3, 15), "1-URGENT"),
    ],
    "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string",
)
NACIONES = spark.createDataFrame([(1, "COLOMBIA"), (2, "PERU"), (3, "MEXICO")], "n_nationkey long, n_name string")
NACIONES_SUCIO = spark.createDataFrame(
    [(1, "COLOMBIA"), (1, "COLOMBIA"), (2, "PERU"), (3, "MEXICO")], "n_nationkey long, n_name string"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · `venta_por_nacion`
# MAGIC
# MAGIC Une órdenes → clientes → naciones (joins `inner`) y devuelve `n_name`, `venta_total` (suma de `o_totalprice`) y `n_ordenes`, ordenado por `venta_total` descendente.

# COMMAND ----------

def venta_por_nacion(ordenes: DataFrame, clientes: DataFrame, naciones: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado = venta_por_nacion(ORDENES, CLIENTES, NACIONES)
assert resultado.columns == ["n_name", "venta_total", "n_ordenes"]
assert [tuple(r) for r in resultado.collect()] == [("COLOMBIA", 350.0, 3), ("PERU", 100.0, 2)]
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

display(
    venta_por_nacion(
        spark.table("samples.tpch.orders"), spark.table("samples.tpch.customer"), spark.table("samples.tpch.nation")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · `clientes_sin_ordenes`
# MAGIC
# MAGIC Devuelve los clientes (todas sus columnas) que no tienen ninguna orden. Usa `left_anti`.

# COMMAND ----------

def clientes_sin_ordenes(clientes: DataFrame, ordenes: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado = clientes_sin_ordenes(CLIENTES, ORDENES)
assert resultado.columns == CLIENTES.columns, "Solo deben quedar las columnas de clientes"
assert [r.c_custkey for r in resultado.collect()] == [4]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · `ordenes_urgentes_por_anio`
# MAGIC
# MAGIC Cuenta las órdenes con prioridad `1-URGENT` por año. Columnas: `anio`, `n_urgentes`. Orden: `anio` ascendente.

# COMMAND ----------

def ordenes_urgentes_por_anio(ordenes: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado = ordenes_urgentes_por_anio(ORDENES)
assert resultado.columns == ["anio", "n_urgentes"]
assert [tuple(r) for r in resultado.collect()] == [(2025, 2), (2026, 1)]
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

display(ordenes_urgentes_por_anio(spark.table("samples.tpch.orders")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4 · `agregar_nacion` con broadcast
# MAGIC
# MAGIC Agrega a cada cliente el nombre de su nación. Usa un join `left` con `F.broadcast` sobre naciones.
# MAGIC Columnas: `c_custkey`, `c_name`, `n_name`.

# COMMAND ----------

def agregar_nacion(clientes: DataFrame, naciones: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado = agregar_nacion(CLIENTES, NACIONES)
assert resultado.columns == ["c_custkey", "c_name", "n_name"]
assert {r.c_custkey: r.n_name for r in resultado.collect()} == {1: "COLOMBIA", 2: "COLOMBIA", 3: "PERU", 4: "MEXICO"}
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC Revisa el plan: debe aparecer `BroadcastHashJoin`.

# COMMAND ----------

agregar_nacion(spark.table("samples.tpch.customer"), spark.table("samples.tpch.nation")).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 5 · El join que duplica filas
# MAGIC
# MAGIC `NACIONES_SUCIO` tiene la nación 1 repetida. Con tu `agregar_nacion`, los clientes de Colombia salen dos veces:

# COMMAND ----------

print("Clientes:", CLIENTES.count(), "→ después del join:", agregar_nacion(CLIENTES, NACIONES_SUCIO).count())

# COMMAND ----------

# MAGIC %md
# MAGIC Escribe `agregar_nacion_seguro`, que:
# MAGIC 1. Deduplique la dimensión por `n_nationkey` antes del join.
# MAGIC 2. **Valide** que el número de filas no cambió; si cambió, lanza `ValueError`.

# COMMAND ----------

def agregar_nacion_seguro(clientes: DataFrame, naciones: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado = agregar_nacion_seguro(CLIENTES, NACIONES_SUCIO)
assert resultado.count() == CLIENTES.count() == 4
assert {r.c_custkey: r.n_name for r in resultado.collect()} == {1: "COLOMBIA", 2: "COLOMBIA", 3: "PERU", 4: "MEXICO"}
print("✅ Ejercicio 5 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que te llevas
# MAGIC
# MAGIC - Funciones `DataFrame → DataFrame` que se prueban con datos pequeños y se aplican a datos reales sin cambios.
# MAGIC - `left_anti` para encontrar faltantes y `broadcast` para dimensiones pequeñas.
# MAGIC - **Siempre** validar la cardinalidad de los joins.
