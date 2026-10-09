# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M4

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W

# COMMAND ----------

# DBTITLE 1,Ejercicio 1
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

    Si is_valid da null (por ejemplo, tarifa nula), filter descarta la
    fila.

    Args:
        df: Historial de registros.
        key: Columna que identifica la entidad.
        order_column: Columna temporal; gana el valor más reciente.
        is_valid: Condición que debe cumplir el registro.

    Returns:
        Un registro por entidad, con las columnas de df.
    """
    latest_first = W.partitionBy(key).orderBy(F.col(order_column).desc())
    return (
        df.filter(is_valid)
        .withColumn("_tmp_row_number", F.row_number().over(latest_first))
        .filter(F.col("_tmp_row_number") == 1)
        .drop("_tmp_row_number")
    )


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
# MAGIC **Para discutir:** con `o_orderdate` hay empates (dos órdenes del mismo cliente el mismo día). Para que el resultado sea determinístico se agrega un segundo criterio de orden, por ejemplo `o_orderkey` descendente.

# COMMAND ----------

# DBTITLE 1,Ejercicio 2
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
    window = (
        W.partitionBy("customer_id")
        .orderBy("sales_month")
        .rowsBetween(W.unboundedPreceding, W.currentRow)
    )
    return df_sales.withColumn(
        "cumulative_sales", F.sum("sales").over(window)
    )


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

# DBTITLE 1,Ejercicio 3
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
SOURCE_PRIORITY = F.when(F.col("source") == "system", 0).otherwise(1)


def drop_duplicates_with_tiebreak(df_status: DataFrame) -> DataFrame:
    """
    Deja un estado por vuelo: el más reciente y, en empate, el del sistema.

    Args:
        df_status: Estados con flight_id, updated_at y source.

    Returns:
        Un registro por flight_id, con las mismas columnas.
    """
    window = W.partitionBy("flight_id").orderBy(
        F.col("updated_at").desc(), SOURCE_PRIORITY.asc()
    )
    return (
        df_status.withColumn("_tmp_row_number", F.row_number().over(window))
        .filter(F.col("_tmp_row_number") == 1)
        .drop("_tmp_row_number")
    )


df_result = drop_duplicates_with_tiebreak(DF_TEST_FLIGHT_STATUS)
assert df_result.columns == DF_TEST_FLIGHT_STATUS.columns
assert {row.flight_id: row.status for row in df_result.collect()} == {
    "V1": "OPERADO",
    "V2": "CANCELADO",
}
print("✅ Ejercicio 3 correcto")
