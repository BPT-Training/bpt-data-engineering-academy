# Databricks notebook source
# MAGIC %md
# MAGIC # M4 · PySpark Advanced Patterns
# MAGIC
# MAGIC **Objetivo:** dominar los patrones que más aparecen en pipelines reales.
# MAGIC
# MAGIC | Bloque | Tema |
# MAGIC |---|---|
# MAGIC | 1 | Window functions: ranking, lag/lead, acumulados |
# MAGIC | 2 | Deduplicación determinística |
# MAGIC | 3 | Nulos: los errores silenciosos |
# MAGIC | 4 | Fechas y zonas horarias |
# MAGIC | 5 | Built-ins vs. UDF vs. pandas UDF |
# MAGIC | 6 | Rendimiento básico |
# MAGIC
# MAGIC > Guía: [02-pyspark.md §3, §5, §6](../../guia-buenas-practicas/02-pyspark.md)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

df_ordenes = spark.table("samples.tpch.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Window functions
# MAGIC
# MAGIC Una window calcula sobre un **grupo de filas relacionadas** sin colapsarlas, a diferencia de `groupBy`, que devuelve una fila por grupo.
# MAGIC
# MAGIC ```python
# MAGIC Window.partitionBy("cliente").orderBy("fecha")
# MAGIC #              ↑ grupos              ↑ orden dentro del grupo
# MAGIC ```

# COMMAND ----------

por_cliente_fecha = Window.partitionBy("o_custkey").orderBy("o_orderdate", "o_orderkey")

df_historial = (
    df_ordenes
    .filter(F.col("o_custkey").isin(1, 2, 4))
    .select("o_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
    .withColumns(
        {
            "n_orden_cliente": F.row_number().over(por_cliente_fecha),
            "precio_orden_anterior": F.lag("o_totalprice").over(por_cliente_fecha),
            "dias_desde_anterior": F.datediff("o_orderdate", F.lag("o_orderdate").over(por_cliente_fecha)),
            "gasto_acumulado": F.sum("o_totalprice").over(
                por_cliente_fecha.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        }
    )
    .orderBy("o_custkey", "n_orden_cliente")
)
display(df_historial)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `row_number` vs. `rank` vs. `dense_rank`
# MAGIC
# MAGIC | Valores | row_number | rank | dense_rank |
# MAGIC |---|---|---|---|
# MAGIC | 100 | 1 | 1 | 1 |
# MAGIC | 100 | 2 | 1 | 1 |
# MAGIC | 90 | 3 | 3 | 2 |
# MAGIC
# MAGIC Top 3 clientes por gasto en cada segmento de mercado:

# COMMAND ----------

df_gasto = (
    df_ordenes.join(spark.table("samples.tpch.customer"), F.col("o_custkey") == F.col("c_custkey"), "inner")
    .groupBy("c_mktsegment", "c_custkey", "c_name")
    .agg(F.sum("o_totalprice").alias("gasto_total"))
)
por_segmento = Window.partitionBy("c_mktsegment").orderBy(F.col("gasto_total").desc())

display(
    df_gasto.withColumn("ranking", F.dense_rank().over(por_segmento))
    .filter(F.col("ranking") <= 3)
    .orderBy("c_mktsegment", "ranking")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplicación determinística
# MAGIC
# MAGIC `dropDuplicates(["id"])` se queda con **cualquier** fila. Si las filas repetidas tienen datos distintos, dos ejecuciones pueden dar resultados distintos.
# MAGIC
# MAGIC La forma BPT es una window con orden explícito **y desempate**.

# COMMAND ----------

from datetime import datetime

df_eventos = spark.createDataFrame(
    [
        ("VL0001", "PROGRAMADO", datetime(2026, 10, 14, 5, 0), "a.csv"),
        ("VL0001", "DEMORADO", datetime(2026, 10, 14, 6, 0), "b.csv"),
        ("VL0001", "OPERADO", datetime(2026, 10, 14, 6, 0), "c.csv"),   # empate en timestamp
        ("VL0002", "CANCELADO", datetime(2026, 10, 14, 7, 0), "a.csv"),
    ],
    "vuelo_id string, estado string, actualizado_ts timestamp, archivo string",
)

ultimo_estado = Window.partitionBy("vuelo_id").orderBy(F.col("actualizado_ts").desc(), F.col("archivo").desc())

display(
    df_eventos.withColumn("_rn", F.row_number().over(ultimo_estado)).filter("_rn = 1").drop("_rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Esta lógica ya está empaquetada en `bpt_utils.limpieza.deduplicar`, así que no se reescribe en cada notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Nulos: los errores silenciosos

# COMMAND ----------

df_nulos = spark.createDataFrame(
    [("A", 10), ("B", None), (None, 5), (None, None)], "codigo string, valor int"
)

display(
    df_nulos.agg(
        F.count("*").alias("filas"),
        F.count("valor").alias("count_valor_ignora_nulos"),
        F.avg("valor").alias("promedio_sin_nulos"),
        F.avg(F.coalesce("valor", F.lit(0))).alias("promedio_nulos_como_0"),
    )
)

# COMMAND ----------

# `==` con null da null (no True): la fila (None, None) NO aparece
print("Con == :", df_nulos.filter(F.col("codigo") == F.lit(None)).count())
# eqNullSafe (<=> en SQL) sí compara nulos
print("Con eqNullSafe:", df_nulos.filter(F.col("codigo").eqNullSafe(F.lit(None))).count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Regla BPT:** para cada columna clave, decide y **documenta** qué pasa con los nulos: se descartan, se imputan (`coalesce`, `na.fill`) o van a quarantine (M6).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fechas y zonas horarias

# COMMAND ----------

df_fechas = spark.createDataFrame(
    [("2026-10-14 08:30",), ("14/10/2026 22:15",), ("sin dato",)], "salida_texto string"
)

display(
    df_fechas.withColumns(
        {
            # try_to_timestamp devuelve null en vez de fallar: prueba varios formatos con coalesce
            "salida_ts": F.coalesce(
                F.try_to_timestamp("salida_texto", F.lit("yyyy-MM-dd HH:mm")),
                F.try_to_timestamp("salida_texto", F.lit("dd/MM/yyyy HH:mm")),
            ),
        }
    ).withColumns(
        {
            "fecha": F.to_date("salida_ts"),
            "hora": F.hour("salida_ts"),
            "semana": F.date_trunc("week", "salida_ts"),
            "salida_utc": F.to_utc_timestamp("salida_ts", "America/Bogota"),
        }
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Regla BPT:** guarda los timestamps en UTC y convierte a hora local solo en la capa de consumo (Gold o reportes).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Built-ins vs. UDF vs. pandas UDF
# MAGIC
# MAGIC | Opción | Dónde corre | Rendimiento | Cuándo |
# MAGIC |---|---|---|---|
# MAGIC | Built-in (`F.*`) | JVM, optimizado | Alto | Siempre que exista |
# MAGIC | pandas UDF | Python, por lotes (Arrow) | Medio | Lógica que no existe en `F.*` |
# MAGIC | UDF Python | Python, fila a fila | Bajo | Último recurso |

# COMMAND ----------

import time

import pandas as pd


@F.udf("string")
def prioridad_udf(prioridad: str) -> str:
    return prioridad.split("-")[1].lower() if prioridad else None


@F.pandas_udf("string")
def prioridad_pandas_udf(prioridad: pd.Series) -> pd.Series:
    return prioridad.str.split("-").str[1].str.lower()


def prioridad_builtin(col: str):
    return F.lower(F.split(F.col(col), "-").getItem(1))


def medir(nombre: str, columna) -> None:
    inicio = time.time()
    df_ordenes.select(columna.alias("p")).agg(F.count_distinct("p")).collect()
    print(f"{nombre:<14} {time.time() - inicio:6.2f} s")


medir("UDF Python", prioridad_udf("o_orderpriority"))
medir("pandas UDF", prioridad_pandas_udf("o_orderpriority"))
medir("Built-in", prioridad_builtin("o_orderpriority"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Rendimiento básico
# MAGIC
# MAGIC | ❌ Evitar | ✅ Preferir |
# MAGIC |---|---|
# MAGIC | `collect()` / `toPandas()` de tablas grandes | Agregar primero, o `limit` |
# MAGIC | `count()` en cada celda "para ver cómo va" | Una sola acción al final |
# MAGIC | Filtrar después del join | Filtrar y proyectar **antes** del join |
# MAGIC | Muchos `withColumn` encadenados | Un `withColumns({...})` |
# MAGIC | `cache()` en serverless | No está soportado: materializa en una tabla si lo necesitas |

# COMMAND ----------

df_lineas = spark.table("samples.tpch.lineitem")

consulta = (
    df_lineas
    .filter(F.col("l_shipdate") >= "1998-01-01")                     # filtro temprano → PushedFilters
    .select("l_orderkey", "l_extendedprice", "l_discount")            # solo las columnas necesarias
    .join(df_ordenes.select("o_orderkey", "o_orderpriority"), F.col("l_orderkey") == F.col("o_orderkey"), "inner")
    .groupBy("o_orderpriority")
    .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("venta_neta"))
)
consulta.explain(mode="formatted")

# COMMAND ----------

display(consulta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente: [04_pyspark_advanced_lab](./04_pyspark_advanced_lab)
