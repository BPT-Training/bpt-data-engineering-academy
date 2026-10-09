# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M3

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

def venta_por_nacion(ordenes: DataFrame, clientes: DataFrame, naciones: DataFrame) -> DataFrame:
    """Venta total y número de órdenes por nación del cliente."""
    return (
        ordenes
        .join(clientes, F.col("o_custkey") == F.col("c_custkey"), how="inner")
        .join(naciones, F.col("c_nationkey") == F.col("n_nationkey"), how="inner")
        .groupBy("n_name")
        .agg(
            F.sum("o_totalprice").alias("venta_total"),
            F.count("o_orderkey").alias("n_ordenes"),
        )
        .orderBy(F.col("venta_total").desc())
    )


resultado = venta_por_nacion(ORDENES, CLIENTES, NACIONES)
assert resultado.columns == ["n_name", "venta_total", "n_ordenes"]
assert [tuple(r) for r in resultado.collect()] == [("COLOMBIA", 350.0, 3), ("PERU", 100.0, 2)]
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

def clientes_sin_ordenes(clientes: DataFrame, ordenes: DataFrame) -> DataFrame:
    """Clientes que no tienen ninguna orden."""
    return clientes.join(ordenes, F.col("c_custkey") == F.col("o_custkey"), how="left_anti")


resultado = clientes_sin_ordenes(CLIENTES, ORDENES)
assert resultado.columns == CLIENTES.columns, "Solo deben quedar las columnas de clientes"
assert [r.c_custkey for r in resultado.collect()] == [4]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

def ordenes_urgentes_por_anio(ordenes: DataFrame) -> DataFrame:
    """Número de órdenes 1-URGENT por año."""
    return (
        ordenes
        .filter(F.col("o_orderpriority") == "1-URGENT")
        .groupBy(F.year("o_orderdate").alias("anio"))
        .agg(F.count("*").alias("n_urgentes"))
        .orderBy("anio")
    )


resultado = ordenes_urgentes_por_anio(ORDENES)
assert resultado.columns == ["anio", "n_urgentes"]
assert [tuple(r) for r in resultado.collect()] == [(2025, 2), (2026, 1)]
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

def agregar_nacion(clientes: DataFrame, naciones: DataFrame) -> DataFrame:
    """Nombre de la nación de cada cliente. Naciones es pequeña → broadcast."""
    return clientes.join(
        F.broadcast(naciones), F.col("c_nationkey") == F.col("n_nationkey"), how="left"
    ).select("c_custkey", "c_name", "n_name")


resultado = agregar_nacion(CLIENTES, NACIONES)
assert resultado.columns == ["c_custkey", "c_name", "n_name"]
assert {r.c_custkey: r.n_name for r in resultado.collect()} == {1: "COLOMBIA", 2: "COLOMBIA", 3: "PERU", 4: "MEXICO"}
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

def agregar_nacion_seguro(clientes: DataFrame, naciones: DataFrame) -> DataFrame:
    """Como agregar_nacion, pero deduplica la dimensión y valida que no se dupliquen clientes."""
    naciones_unicas = naciones.dropDuplicates(["n_nationkey"])
    resultado = agregar_nacion(clientes, naciones_unicas)

    n_antes, n_despues = clientes.count(), resultado.count()
    if n_antes != n_despues:
        raise ValueError(f"El join cambió el número de filas: {n_antes} → {n_despues}")
    return resultado


resultado = agregar_nacion_seguro(CLIENTES, NACIONES_SUCIO)
assert resultado.count() == CLIENTES.count() == 4
assert {r.c_custkey: r.n_name for r in resultado.collect()} == {1: "COLOMBIA", 2: "COLOMBIA", 3: "PERU", 4: "MEXICO"}
print("✅ Ejercicio 5 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC **Nota sobre `dropDuplicates`:** aquí es seguro porque las filas repetidas son idénticas. Si las filas duplicadas tuvieran valores **distintos** (por ejemplo, dos nombres para la misma llave), habría que elegir con un criterio explícito usando una window. Lo vemos en M4.
