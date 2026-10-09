# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M3 · PySpark Fundamentals
# MAGIC
# MAGIC **Instrucciones:** cada ejercicio es una función `DataFrame → DataFrame`. Primero se verifica con DataFrames pequeños de resultado conocido y después se aplica sin cambios a `samples.tpch`.
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `get_sales_by_nation` | Joins encadenados + agregación |
# MAGIC | 2 | `get_customers_without_orders` | `left_anti` |
# MAGIC | 3 | `count_urgent_orders_by_year` | `filter` + `groupBy` |
# MAGIC | 4 | `add_nation_name` | Join con broadcast |
# MAGIC | 5 | `add_nation_name_safe` | Corregir un join que duplica filas |
# MAGIC
# MAGIC Solución: [soluciones/03_pyspark_fundamentals_lab_solucion](./soluciones/03_pyspark_fundamentals_lab_solucion)

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: F401 - para los ejercicios

# COMMAND ----------

# DBTITLE 1,Datos de prueba
DF_TEST_CUSTOMERS = spark.createDataFrame(
    [
        (1, "Ana", 1, "BUILDING"),
        (2, "Luis", 1, "AUTOMOBILE"),
        (3, "Sofía", 2, "BUILDING"),
        (4, "Pedro", 3, "MACHINERY"),
    ],
    "c_custkey long, c_name string, c_nationkey long, c_mktsegment string",
)
DF_TEST_ORDERS = spark.createDataFrame(
    [
        (10, 1, "F", 100.0, date(2025, 1, 5), "1-URGENT"),
        (11, 1, "O", 50.0, date(2025, 2, 10), "3-MEDIUM"),
        (12, 2, "F", 200.0, date(2025, 1, 20), "1-URGENT"),
        (13, 3, "F", 75.0, date(2026, 3, 1), "2-HIGH"),
        (14, 3, "P", 25.0, date(2026, 3, 15), "1-URGENT"),
    ],
    "o_orderkey long, o_custkey long, o_orderstatus string, "
    "o_totalprice double, o_orderdate date, o_orderpriority string",
)
DF_TEST_NATIONS = spark.createDataFrame(
    [(1, "COLOMBIA"), (2, "PERU"), (3, "MEXICO")],
    "n_nationkey long, n_name string",
)
DF_TEST_NATIONS_DUP = spark.createDataFrame(
    [(1, "COLOMBIA"), (1, "COLOMBIA"), (2, "PERU"), (3, "MEXICO")],
    "n_nationkey long, n_name string",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · `get_sales_by_nation`
# MAGIC
# MAGIC Unir órdenes → clientes → naciones (joins `inner`) y devolver `n_name`, `total_sales` (suma de `o_totalprice`) y `order_count`, ordenado por `total_sales` de mayor a menor.

# COMMAND ----------

def get_sales_by_nation(
    df_orders: DataFrame, df_customers: DataFrame, df_nations: DataFrame
) -> DataFrame:
    """
    Calcula la venta total y el número de órdenes por nación.

    Args:
        df_orders: Órdenes (o_custkey, o_orderkey, o_totalprice).
        df_customers: Clientes (c_custkey, c_nationkey).
        df_nations: Naciones (n_nationkey, n_name).

    Returns:
        DataFrame con n_name, total_sales y order_count.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 1
df_result = get_sales_by_nation(
    DF_TEST_ORDERS, DF_TEST_CUSTOMERS, DF_TEST_NATIONS
)
assert df_result.columns == ["n_name", "total_sales", "order_count"]
assert [tuple(row) for row in df_result.collect()] == [
    ("COLOMBIA", 350.0, 3),
    ("PERU", 100.0, 2),
]
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

display(
    get_sales_by_nation(
        spark.table("samples.tpch.orders"),
        spark.table("samples.tpch.customer"),
        spark.table("samples.tpch.nation"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · `get_customers_without_orders`
# MAGIC
# MAGIC Devolver los clientes (con todas sus columnas) que no tienen ninguna orden. Usar `left_anti`.

# COMMAND ----------

def get_customers_without_orders(
    df_customers: DataFrame, df_orders: DataFrame
) -> DataFrame:
    """
    Obtiene los clientes que no tienen órdenes.

    Args:
        df_customers: Clientes (c_custkey).
        df_orders: Órdenes (o_custkey).

    Returns:
        Clientes sin órdenes, con las columnas de df_customers.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 2
df_result = get_customers_without_orders(DF_TEST_CUSTOMERS, DF_TEST_ORDERS)
assert df_result.columns == DF_TEST_CUSTOMERS.columns
assert [row.c_custkey for row in df_result.collect()] == [4]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · `count_urgent_orders_by_year`
# MAGIC
# MAGIC Contar las órdenes con prioridad `1-URGENT` por año. Columnas: `order_year`, `urgent_count`. Orden: `order_year` ascendente.

# COMMAND ----------

URGENT_PRIORITY = "1-URGENT"


def count_urgent_orders_by_year(df_orders: DataFrame) -> DataFrame:
    """
    Cuenta las órdenes urgentes por año.

    Args:
        df_orders: Órdenes (o_orderdate, o_orderpriority).

    Returns:
        DataFrame con order_year y urgent_count.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 3
df_result = count_urgent_orders_by_year(DF_TEST_ORDERS)
assert df_result.columns == ["order_year", "urgent_count"]
assert [tuple(row) for row in df_result.collect()] == [(2025, 2), (2026, 1)]
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

display(count_urgent_orders_by_year(spark.table("samples.tpch.orders")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4 · `add_nation_name`
# MAGIC
# MAGIC Agregar a cada cliente el nombre de su nación con un join `left` y `F.broadcast` sobre naciones. Columnas: `c_custkey`, `c_name`, `n_name`.

# COMMAND ----------

def add_nation_name(
    df_customers: DataFrame, df_nations: DataFrame
) -> DataFrame:
    """
    Agrega el nombre de la nación a cada cliente.

    Args:
        df_customers: Clientes (c_custkey, c_name, c_nationkey).
        df_nations: Naciones (n_nationkey, n_name), tabla pequeña.

    Returns:
        DataFrame con c_custkey, c_name y n_name.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 4
EXPECTED_NATIONS = {1: "COLOMBIA", 2: "COLOMBIA", 3: "PERU", 4: "MEXICO"}
df_result = add_nation_name(DF_TEST_CUSTOMERS, DF_TEST_NATIONS)
assert df_result.columns == ["c_custkey", "c_name", "n_name"]
assert {
    row.c_custkey: row.n_name for row in df_result.collect()
} == EXPECTED_NATIONS
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 5 · El join que duplica filas
# MAGIC
# MAGIC `DF_TEST_NATIONS_DUP` tiene la nación 1 repetida. Con `add_nation_name`, los clientes de Colombia salen dos veces:

# COMMAND ----------

rows_after = add_nation_name(DF_TEST_CUSTOMERS, DF_TEST_NATIONS_DUP).count()
rows_before = DF_TEST_CUSTOMERS.count()
print(f"Clientes: {rows_before} · después del join: {rows_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC Escribir `add_nation_name_safe`, que:
# MAGIC 1. Deduplique la dimensión por `n_nationkey` antes del join.
# MAGIC 2. Valide que el número de filas no cambió; si cambió, lance `ValueError` con un mensaje que diga cuántas filas había y cuántas quedaron.

# COMMAND ----------

def add_nation_name_safe(
    df_customers: DataFrame, df_nations: DataFrame
) -> DataFrame:
    """
    Agrega el nombre de la nación sin duplicar clientes.

    Args:
        df_customers: Clientes (c_custkey, c_name, c_nationkey).
        df_nations: Naciones (n_nationkey, n_name).

    Returns:
        DataFrame con c_custkey, c_name y n_name.

    Raises:
        ValueError: Si el join cambia el número de clientes.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 5
df_result = add_nation_name_safe(DF_TEST_CUSTOMERS, DF_TEST_NATIONS_DUP)
assert df_result.count() == DF_TEST_CUSTOMERS.count() == 4
assert {
    row.c_custkey: row.n_name for row in df_result.collect()
} == EXPECTED_NATIONS
print("✅ Ejercicio 5 correcto")
