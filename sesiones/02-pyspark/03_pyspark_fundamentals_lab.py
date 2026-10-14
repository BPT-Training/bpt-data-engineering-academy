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
# MAGIC Solución: notebook `03_pyspark_fundamentals_lab_solucion` (subcarpeta `soluciones` de esta misma carpeta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antes de empezar: trabajar sobre una copia
# MAGIC
# MAGIC Las respuestas se escriben en una **copia personal** de este notebook. Así no se pierden cuando se actualiza el repositorio con **Pull**.
# MAGIC
# MAGIC 1. En la barra superior de este notebook, abrir el menú **File** y seleccionar **Clone**.
# MAGIC 2. En **Name**, agregar `_mi_copia` al final del nombre: `03_pyspark_fundamentals_lab_mi_copia`.
# MAGIC 3. En **Destination**, verificar que esté seleccionada la **misma carpeta** de este notebook (`sesiones/02-pyspark` dentro de la carpeta del repositorio). Si aparece otra, navegar hasta esa carpeta y seleccionarla.
# MAGIC 4. Seleccionar **Clone**. La copia se abre automáticamente.
# MAGIC 5. Confirmar que el nombre en la parte superior termina en `_mi_copia` y resolver los ejercicios ahí.
# MAGIC
# MAGIC **Por qué en la misma carpeta:** la copia usa la configuración del curso con rutas relativas a esta carpeta. Si se guarda en otra ubicación (por ejemplo, la carpeta personal), deja de encontrarla.

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: F401 - para los ejercicios

# COMMAND ----------

# DBTITLE 1,Funciones de verificación (no modificar)


def check_pending(result, exercise, function_name):
    """Falla con un mensaje claro si la función todavía no está resuelta."""
    if result is None:
        raise AssertionError(
            f"{exercise} pendiente: completar la función {function_name} "
            "donde dice # TODO y ejecutar de nuevo su celda."
        )


def check_equal(actual, expected, description):
    """Compara un resultado con el esperado y explica la diferencia."""
    if actual != expected:
        raise AssertionError(
            f"{description}: se obtuvo {actual!r}; se esperaba {expected!r}."
        )

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
check_pending(df_result, "Ejercicio 1", "get_sales_by_nation")
check_equal(
    df_result.columns,
    ["n_name", "total_sales", "order_count"],
    "Columnas del resultado",
)
check_equal(
    [tuple(row) for row in df_result.collect()],
    [("COLOMBIA", 350.0, 3), ("PERU", 100.0, 2)],
    "Filas del resultado (en orden)",
)
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
check_pending(df_result, "Ejercicio 2", "get_customers_without_orders")
check_equal(
    df_result.columns,
    DF_TEST_CUSTOMERS.columns,
    "Columnas del resultado (solo las de clientes)",
)
check_equal(
    [row.c_custkey for row in df_result.collect()],
    [4],
    "Clientes sin órdenes",
)
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
check_pending(df_result, "Ejercicio 3", "count_urgent_orders_by_year")
check_equal(
    df_result.columns, ["order_year", "urgent_count"], "Columnas del resultado"
)
check_equal(
    [tuple(row) for row in df_result.collect()],
    [(2025, 2), (2026, 1)],
    "Filas del resultado (en orden)",
)
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
check_pending(df_result, "Ejercicio 4", "add_nation_name")
check_equal(
    df_result.columns,
    ["c_custkey", "c_name", "n_name"],
    "Columnas del resultado",
)
check_equal(
    {row.c_custkey: row.n_name for row in df_result.collect()},
    EXPECTED_NATIONS,
    "Nación de cada cliente",
)
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
check_pending(df_result, "Ejercicio 5", "add_nation_name_safe")
check_equal(df_result.count(), 4, "Número de clientes después del join")
check_equal(
    {row.c_custkey: row.n_name for row in df_result.collect()},
    EXPECTED_NATIONS,
    "Nación de cada cliente",
)
print("✅ Ejercicio 5 correcto")
