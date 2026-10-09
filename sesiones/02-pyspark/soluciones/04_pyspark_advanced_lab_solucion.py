# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M4

# COMMAND ----------

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

HISTORIAL_RESERVAS = spark.createDataFrame(
    [
        ("R1", "CREADA", 100.0, datetime(2026, 10, 1, 10, 0)),
        ("R1", "PAGADA", 100.0, datetime(2026, 10, 1, 11, 0)),
        ("R1", "ERROR", None, datetime(2026, 10, 1, 12, 0)),
        ("R2", "CREADA", 200.0, datetime(2026, 10, 2, 9, 0)),
        ("R2", "PAGADA", -5.0, datetime(2026, 10, 2, 10, 0)),
        ("R3", "ERROR", 50.0, datetime(2026, 10, 3, 8, 0)),
    ],
    "reserva_id string, estado string, tarifa double, actualizado_ts timestamp",
)
ES_VALIDA = (F.col("estado") != "ERROR") & (F.col("tarifa") >= 0)


def ultimo_registro_valido(df: DataFrame, llave: str, orden: str, es_valido: Column) -> DataFrame:
    """Registro más reciente por `llave` entre los que cumplen `es_valido`.

    Nota: si `es_valido` da null (por ejemplo, tarifa nula), `filter` descarta la fila.
    """
    ventana = Window.partitionBy(llave).orderBy(F.col(orden).desc())
    return (
        df.filter(es_valido)
        .withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


resultado = ultimo_registro_valido(HISTORIAL_RESERVAS, "reserva_id", "actualizado_ts", ES_VALIDA)
assert resultado.columns == HISTORIAL_RESERVAS.columns
assert {r.reserva_id: r.estado for r in resultado.collect()} == {"R1": "PAGADA", "R2": "CREADA"}
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC **Para discutir:** con `o_orderdate` hay empates (un cliente puede tener dos órdenes el mismo día). ¿Cómo harías el resultado determinístico?
# MAGIC Una respuesta: agregar un segundo criterio de orden, por ejemplo `o_orderkey` descendente.

# COMMAND ----------

VENTAS_MENSUALES = spark.createDataFrame(
    [
        ("C1", date(2026, 1, 1), 100.0),
        ("C1", date(2026, 2, 1), 50.0),
        ("C1", date(2026, 3, 1), 25.0),
        ("C2", date(2026, 1, 1), 10.0),
        ("C2", date(2026, 3, 1), 30.0),
    ],
    "cliente string, mes date, venta double",
)


def acumulado_por_cliente(df: DataFrame) -> DataFrame:
    """Agrega la venta acumulada del cliente mes a mes."""
    ventana = (
        Window.partitionBy("cliente")
        .orderBy("mes")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return df.withColumn("venta_acumulada", F.sum("venta").over(ventana))


resultado = acumulado_por_cliente(VENTAS_MENSUALES).orderBy("cliente", "mes")
assert resultado.columns == ["cliente", "mes", "venta", "venta_acumulada"]
assert [r.venta_acumulada for r in resultado.collect()] == [100.0, 150.0, 175.0, 10.0, 40.0]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

@F.udf("string")
def clasificar_prioridad_udf(prioridad: str) -> str:
    if prioridad in ("1-URGENT", "2-HIGH"):
        return "alta"
    if prioridad == "3-MEDIUM":
        return "media"
    return "baja"


def clasificar_prioridad(col: str) -> Column:
    """Versión built-in: alta / media / baja. Los nulos caen en 'baja', igual que la UDF."""
    return (
        F.when(F.col(col).isin("1-URGENT", "2-HIGH"), "alta")
        .when(F.col(col) == "3-MEDIUM", "media")
        .otherwise("baja")
    )


PRIORIDADES = spark.createDataFrame(
    [("1-URGENT",), ("2-HIGH",), ("3-MEDIUM",), ("4-NOT SPECIFIED",), ("5-LOW",), (None,)], "p string"
)
comparacion = PRIORIDADES.select(
    clasificar_prioridad_udf("p").alias("con_udf"), clasificar_prioridad("p").alias("con_builtin")
).collect()

assert all(r.con_udf == r.con_builtin for r in comparacion), comparacion
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué funciona con `None`?** `F.col(col).isin(...)` con null da null, y `when` trata null como falso, así que cae en `otherwise("baja")`.

# COMMAND ----------

ESTADOS_VUELO = spark.createDataFrame(
    [
        ("V1", "DEMORADO", datetime(2026, 10, 14, 6, 0), "manual"),
        ("V1", "OPERADO", datetime(2026, 10, 14, 6, 0), "sistema"),
        ("V1", "PROGRAMADO", datetime(2026, 10, 14, 5, 0), "sistema"),
        ("V2", "PROGRAMADO", datetime(2026, 10, 14, 7, 0), "sistema"),
        ("V2", "CANCELADO", datetime(2026, 10, 14, 8, 0), "manual"),
    ],
    "vuelo_id string, estado string, actualizado_ts timestamp, fuente string",
)

PRIORIDAD_FUENTE = F.when(F.col("fuente") == "sistema", 0).otherwise(1)


def deduplicar_con_desempate(df: DataFrame) -> DataFrame:
    """Un registro por vuelo: el más reciente y, si hay empate, el de la fuente 'sistema'."""
    ventana = Window.partitionBy("vuelo_id").orderBy(F.col("actualizado_ts").desc(), PRIORIDAD_FUENTE.asc())
    return df.withColumn("_rn", F.row_number().over(ventana)).filter(F.col("_rn") == 1).drop("_rn")


resultado = deduplicar_con_desempate(ESTADOS_VUELO)
assert resultado.columns == ESTADOS_VUELO.columns
assert {r.vuelo_id: r.estado for r in resultado.collect()} == {"V1": "OPERADO", "V2": "CANCELADO"}
print("✅ Ejercicio 4 correcto")
