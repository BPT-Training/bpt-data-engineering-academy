# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M4 · PySpark Advanced Patterns
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `ultimo_registro_valido` (**entregable**) | Window + filtro de validez |
# MAGIC | 2 | `acumulado_por_cliente` | Window con frame explícito |
# MAGIC | 3 | `clasificar_prioridad` | Reemplazar una UDF por built-ins |
# MAGIC | 4 | `deduplicar_con_desempate` | Dedup determinística con prioridad de fuente |
# MAGIC
# MAGIC Solución: [soluciones/04_pyspark_advanced_lab_solucion](./soluciones/04_pyspark_advanced_lab_solucion)

# COMMAND ----------

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · Entregable · Último registro válido por entidad
# MAGIC
# MAGIC Es **el patrón más común en ingeniería de datos**: de un historial de cambios, quedarse con el estado vigente.
# MAGIC
# MAGIC `ultimo_registro_valido(df, llave, orden, es_valido)`:
# MAGIC - `llave`: columna que identifica la entidad.
# MAGIC - `orden`: columna temporal; gana el valor más reciente.
# MAGIC - `es_valido`: condición (`Column`) que debe cumplir el registro.
# MAGIC
# MAGIC Devuelve **una fila por entidad** (la más reciente entre las válidas) con las mismas columnas de entrada.
# MAGIC Las entidades sin ningún registro válido no aparecen.

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
    # TODO
    ...

# COMMAND ----------

resultado = ultimo_registro_valido(HISTORIAL_RESERVAS, "reserva_id", "actualizado_ts", ES_VALIDA)
assert resultado.columns == HISTORIAL_RESERVAS.columns
assert {r.reserva_id: r.estado for r in resultado.collect()} == {"R1": "PAGADA", "R2": "CREADA"}
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicado a datos reales
# MAGIC
# MAGIC Última orden **finalizada** (`o_orderstatus = 'F'`) de cada cliente en `samples.tpch.orders`. Es la misma función, sin cambios:

# COMMAND ----------

df_ultima_orden = ultimo_registro_valido(
    spark.table("samples.tpch.orders"), "o_custkey", "o_orderdate", F.col("o_orderstatus") == "F"
)
display(df_ultima_orden.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · Acumulado por cliente
# MAGIC
# MAGIC Agrega la columna `venta_acumulada`: suma de `venta` del cliente desde su primer mes hasta el mes actual.
# MAGIC Usa un frame explícito `rowsBetween(Window.unboundedPreceding, Window.currentRow)`.

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
    # TODO
    ...

# COMMAND ----------

resultado = acumulado_por_cliente(VENTAS_MENSUALES).orderBy("cliente", "mes")
assert resultado.columns == ["cliente", "mes", "venta", "venta_acumulada"]
assert [r.venta_acumulada for r in resultado.collect()] == [100.0, 150.0, 175.0, 10.0, 40.0]
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Reemplazar una UDF
# MAGIC
# MAGIC Esta UDF funciona, pero es lenta y opaca para el optimizador. Escribe `clasificar_prioridad(col)` con `F.when` que devuelva **exactamente lo mismo**.

# COMMAND ----------

@F.udf("string")
def clasificar_prioridad_udf(prioridad: str) -> str:
    if prioridad in ("1-URGENT", "2-HIGH"):
        return "alta"
    if prioridad == "3-MEDIUM":
        return "media"
    return "baja"


def clasificar_prioridad(col: str) -> Column:
    # TODO
    ...

# COMMAND ----------

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
# MAGIC ## Ejercicio 4 · Deduplicar con desempate por fuente
# MAGIC
# MAGIC Los estados de vuelo llegan de dos fuentes: `sistema` (automática) y `manual` (digitada por un operador).
# MAGIC Deja **un registro por `vuelo_id`**:
# MAGIC 1. Gana el `actualizado_ts` más reciente.
# MAGIC 2. Si hay empate en el timestamp, gana la fuente `sistema`.

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


def deduplicar_con_desempate(df: DataFrame) -> DataFrame:
    # TODO
    ...

# COMMAND ----------

resultado = deduplicar_con_desempate(ESTADOS_VUELO)
assert resultado.columns == ESTADOS_VUELO.columns
assert {r.vuelo_id: r.estado for r in resultado.collect()} == {"V1": "OPERADO", "V2": "CANCELADO"}
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregable de la sesión
# MAGIC
# MAGIC `ultimo_registro_valido`: una query PySpark que obtiene el último registro válido por entidad, probada con datos controlados y aplicada a `samples.tpch`.
