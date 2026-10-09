# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M4 · PySpark Advanced Patterns
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `get_latest_valid_record` (**entregable**) | Ventana + condición de validez |
# MAGIC | 2 | `add_cumulative_sales` | Ventana con marco explícito |
# MAGIC | 3 | `drop_duplicates_with_tiebreak` | Deduplicación con prioridad de fuente |
# MAGIC
# MAGIC Solución: [soluciones/04_pyspark_advanced_lab_solucion](./soluciones/04_pyspark_advanced_lab_solucion)

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W  # noqa: F401 - ejercicios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · Último registro válido por entidad (entregable)
# MAGIC
# MAGIC Es el patrón más común en ingeniería de datos: de un historial de cambios, quedarse con el estado vigente.
# MAGIC
# MAGIC `get_latest_valid_record(df, key, order_column, is_valid)`:
# MAGIC - `key`: columna que identifica la entidad.
# MAGIC - `order_column`: columna temporal; gana el valor más reciente.
# MAGIC - `is_valid`: condición (`Column`) que debe cumplir el registro.
# MAGIC
# MAGIC Devuelve **una fila por entidad**: la más reciente entre las válidas, con las mismas columnas de entrada. Las entidades sin registros válidos no aparecen.

# COMMAND ----------

DF_TEST_BOOKING_HISTORY = spark.createDataFrame(
    [
        ("R1", "CREADA", 100.0, datetime(2026, 10, 1, 10, 0)),
        ("R1", "PAGADA", 100.0, datetime(2026, 10, 1, 11, 0)),
        ("R1", "ERROR", None, datetime(2026, 10, 1, 12, 0)),
        ("R2", "CREADA", 200.0, datetime(2026, 10, 2, 9, 0)),
        ("R2", "PAGADA", -5.0, datetime(2026, 10, 2, 10, 0)),
        ("R3", "ERROR", 50.0, datetime(2026, 10, 3, 8, 0)),
    ],
    "booking_id string, status string, fare double, updated_at timestamp",
)
IS_VALID_BOOKING = (F.col("status") != "ERROR") & (F.col("fare") >= 0)


def get_latest_valid_record(
    df: DataFrame, key: str, order_column: str, is_valid: Column
) -> DataFrame:
    """
    Obtiene el registro más reciente por entidad entre los válidos.

    Args:
        df: Historial de registros.
        key: Columna que identifica la entidad.
        order_column: Columna temporal; gana el valor más reciente.
        is_valid: Condición que debe cumplir el registro.

    Returns:
        Un registro por entidad, con las columnas de df.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 1
df_result = get_latest_valid_record(
    DF_TEST_BOOKING_HISTORY, "booking_id", "updated_at", IS_VALID_BOOKING
)
assert df_result.columns == DF_TEST_BOOKING_HISTORY.columns
assert {row.booking_id: row.status for row in df_result.collect()} == {
    "R1": "PAGADA",
    "R2": "CREADA",
}
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicado a datos reales
# MAGIC
# MAGIC Última orden **finalizada** (`o_orderstatus = 'F'`) de cada cliente. La misma función, sin cambios:

# COMMAND ----------

df_latest_order = get_latest_valid_record(
    spark.table("samples.tpch.orders"),
    "o_custkey",
    "o_orderdate",
    F.col("o_orderstatus") == "F",
)
display(df_latest_order.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · `add_cumulative_sales`
# MAGIC
# MAGIC Agregar la columna `cumulative_sales`: suma de `sales` del cliente desde su primer mes hasta el mes actual. Usar el marco `rowsBetween(W.unboundedPreceding, W.currentRow)`.

# COMMAND ----------

DF_TEST_MONTHLY_SALES = spark.createDataFrame(
    [
        ("C1", date(2026, 1, 1), 100.0),
        ("C1", date(2026, 2, 1), 50.0),
        ("C1", date(2026, 3, 1), 25.0),
        ("C2", date(2026, 1, 1), 10.0),
        ("C2", date(2026, 3, 1), 30.0),
    ],
    "customer_id string, sales_month date, sales double",
)


def add_cumulative_sales(df_sales: DataFrame) -> DataFrame:
    """
    Agrega la venta acumulada de cada cliente mes a mes.

    Args:
        df_sales: Ventas con customer_id, sales_month y sales.

    Returns:
        DataFrame con la columna adicional cumulative_sales.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 2
df_result = add_cumulative_sales(DF_TEST_MONTHLY_SALES).orderBy(
    "customer_id", "sales_month"
)
assert df_result.columns == [
    "customer_id", "sales_month", "sales", "cumulative_sales",
]
assert [row.cumulative_sales for row in df_result.collect()] == [
    100.0, 150.0, 175.0, 10.0, 40.0,
]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Deduplicar con desempate por fuente
# MAGIC
# MAGIC Los estados de vuelo llegan de dos fuentes: `system` (automática) y `manual` (digitada por un operador). Dejar **un registro por `flight_id`**:
# MAGIC 1. Gana el `updated_at` más reciente.
# MAGIC 2. Si hay empate, gana la fuente `system`.

# COMMAND ----------

DF_TEST_FLIGHT_STATUS = spark.createDataFrame(
    [
        ("V1", "DEMORADO", datetime(2026, 10, 14, 6, 0), "manual"),
        ("V1", "OPERADO", datetime(2026, 10, 14, 6, 0), "system"),
        ("V1", "PROGRAMADO", datetime(2026, 10, 14, 5, 0), "system"),
        ("V2", "PROGRAMADO", datetime(2026, 10, 14, 7, 0), "system"),
        ("V2", "CANCELADO", datetime(2026, 10, 14, 8, 0), "manual"),
    ],
    "flight_id string, status string, updated_at timestamp, source string",
)


def drop_duplicates_with_tiebreak(df_status: DataFrame) -> DataFrame:
    """
    Deja un estado por vuelo: el más reciente y, en empate, el del sistema.

    Args:
        df_status: Estados con flight_id, updated_at y source.

    Returns:
        Un registro por flight_id, con las mismas columnas.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 3
df_result = drop_duplicates_with_tiebreak(DF_TEST_FLIGHT_STATUS)
assert df_result.columns == DF_TEST_FLIGHT_STATUS.columns
assert {row.flight_id: row.status for row in df_result.collect()} == {
    "V1": "OPERADO",
    "V2": "CANCELADO",
}
print("✅ Ejercicio 3 correcto")
