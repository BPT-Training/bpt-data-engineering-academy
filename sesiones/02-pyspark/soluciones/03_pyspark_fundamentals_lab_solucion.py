# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M3

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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
EXPECTED_NATIONS = {1: "COLOMBIA", 2: "COLOMBIA", 3: "PERU", 4: "MEXICO"}

# COMMAND ----------

# DBTITLE 1,Ejercicio 1


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
    return (
        df_orders
        .join(
            df_customers,
            F.col("o_custkey") == F.col("c_custkey"),
            how="inner",
        )
        .join(
            df_nations,
            F.col("c_nationkey") == F.col("n_nationkey"),
            how="inner",
        )
        .groupBy("n_name")
        .agg(
            F.sum("o_totalprice").alias("total_sales"),
            F.count("o_orderkey").alias("order_count"),
        )
        .orderBy(F.col("total_sales").desc())
    )


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

# DBTITLE 1,Ejercicio 2


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
    return df_customers.join(
        df_orders, F.col("c_custkey") == F.col("o_custkey"), how="left_anti"
    )


df_result = get_customers_without_orders(DF_TEST_CUSTOMERS, DF_TEST_ORDERS)
assert df_result.columns == DF_TEST_CUSTOMERS.columns
assert [row.c_custkey for row in df_result.collect()] == [4]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 3
URGENT_PRIORITY = "1-URGENT"


def count_urgent_orders_by_year(df_orders: DataFrame) -> DataFrame:
    """
    Cuenta las órdenes urgentes por año.

    Args:
        df_orders: Órdenes (o_orderdate, o_orderpriority).

    Returns:
        DataFrame con order_year y urgent_count.
    """
    return (
        df_orders
        .filter(F.col("o_orderpriority") == URGENT_PRIORITY)
        .groupBy(F.year("o_orderdate").alias("order_year"))
        .agg(F.count("*").alias("urgent_count"))
        .orderBy("order_year")
    )


df_result = count_urgent_orders_by_year(DF_TEST_ORDERS)
assert df_result.columns == ["order_year", "urgent_count"]
assert [tuple(row) for row in df_result.collect()] == [(2025, 2), (2026, 1)]
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 4


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
    return df_customers.join(
        F.broadcast(df_nations),
        F.col("c_nationkey") == F.col("n_nationkey"),
        how="left",
    ).select("c_custkey", "c_name", "n_name")


df_result = add_nation_name(DF_TEST_CUSTOMERS, DF_TEST_NATIONS)
assert df_result.columns == ["c_custkey", "c_name", "n_name"]
assert {
    row.c_custkey: row.n_name for row in df_result.collect()
} == EXPECTED_NATIONS
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 5


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
    df_nations_dedup = df_nations.dropDuplicates(["n_nationkey"])
    df_result = add_nation_name(df_customers, df_nations_dedup)

    rows_before, rows_after = df_customers.count(), df_result.count()
    if rows_before != rows_after:
        raise ValueError(
            f"El join cambió el número de clientes: "
            f"{rows_before} -> {rows_after}"
        )
    return df_result


df_result = add_nation_name_safe(DF_TEST_CUSTOMERS, DF_TEST_NATIONS_DUP)
assert df_result.count() == DF_TEST_CUSTOMERS.count() == 4
assert {
    row.c_custkey: row.n_name for row in df_result.collect()
} == EXPECTED_NATIONS
print("✅ Ejercicio 5 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC **Sobre `dropDuplicates`:** aquí es seguro porque las filas repetidas son idénticas. Si tuvieran valores **distintos** para la misma llave, habría que elegir con un criterio explícito usando una ventana (M4).
