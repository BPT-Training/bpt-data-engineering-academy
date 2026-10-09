# Databricks notebook source
# MAGIC %md
# MAGIC # M3 · PySpark Fundamentals
# MAGIC
# MAGIC **Objetivo:** entender cómo trabaja Spark y dominar las operaciones base de un DataFrame.
# MAGIC
# MAGIC | Bloque | Tema |
# MAGIC |---|---|
# MAGIC | 1 | Arquitectura: driver, executors y lazy evaluation |
# MAGIC | 2 | DataFrames y schemas |
# MAGIC | 3 | `select`, `filter`, `withColumns`, casts |
# MAGIC | 4 | Agregaciones con `groupBy` |
# MAGIC | 5 | Joins: tipos, broadcast y validación de cardinalidad |
# MAGIC | 6 | Leer el plan con `explain` |
# MAGIC
# MAGIC **Datos:** `samples.tpch`, un modelo de ventas con clientes, órdenes, líneas y naciones.
# MAGIC
# MAGIC > Guía: [02-pyspark.md](../../guia-buenas-practicas/02-pyspark.md)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Arquitectura en 3 minutos
# MAGIC
# MAGIC ```
# MAGIC            ┌──────────────┐
# MAGIC  tu código →│    Driver    │  construye el plan y lo optimiza
# MAGIC            └──────┬───────┘
# MAGIC        ┌──────────┼──────────┐
# MAGIC   ┌────▼───┐ ┌────▼───┐ ┌────▼───┐
# MAGIC   │Executor│ │Executor│ │Executor│  cada uno procesa particiones de datos en paralelo
# MAGIC   └────────┘ └────────┘ └────────┘
# MAGIC ```
# MAGIC
# MAGIC - **Transformaciones** (`select`, `filter`, `join`, `groupBy`) son *lazy*: solo agregan pasos al plan.
# MAGIC - **Acciones** (`count`, `display`, `collect`, `write`) disparan la ejecución.
# MAGIC - **Narrow vs. wide:** `filter` trabaja dentro de cada partición; `groupBy` y `join` necesitan mover datos entre executors (**shuffle**), que es lo costoso.
# MAGIC
# MAGIC En serverless no administras el cluster, pero estos conceptos siguen definiendo el rendimiento.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

df_ordenes = spark.table("samples.tpch.orders")
df_clientes = spark.table("samples.tpch.customer")
df_naciones = spark.table("samples.tpch.nation")

# Esto NO ejecuta nada todavía: solo define el plan
df_filtrado = df_ordenes.filter(F.col("o_orderstatus") == "F")
print(type(df_filtrado))

# COMMAND ----------

# Esto SÍ ejecuta: es una acción
df_filtrado.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DataFrames y schemas

# COMMAND ----------

df_ordenes.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Para tablas Delta el schema ya viene definido. Para **archivos** (CSV, JSON), BPT exige declararlo explícitamente en producción:

# COMMAND ----------

SCHEMA_VUELOS = T.StructType(
    [
        T.StructField("vuelo_id", T.StringType(), nullable=False),
        T.StructField("origen", T.StringType()),
        T.StructField("destino", T.StringType()),
        T.StructField("retraso_min", T.IntegerType()),
    ]
)

df_vuelos = spark.createDataFrame(
    [("VL0001", "BOG", "MDE", 5), ("VL0002", "MDE", "CLO", 42), ("VL0003", "CLO", "CTG", None)],
    schema=SCHEMA_VUELOS,
)
df_vuelos.printSchema()

# Con un archivo sería: spark.read.csv(ruta, header=True, schema=SCHEMA_VUELOS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `select`, `filter`, `withColumns`, casts
# MAGIC
# MAGIC - Una transformación por línea, dentro de paréntesis.
# MAGIC - `F.col("x")` para referenciar columnas.
# MAGIC - `withColumns` para agregar varias columnas de una vez.

# COMMAND ----------

df_ordenes_enriquecidas = (
    df_ordenes
    .filter(F.col("o_orderdate") >= "1998-01-01")
    .select("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority")
    .withColumns(
        {
            "anio": F.year("o_orderdate"),
            "mes": F.month("o_orderdate"),
            "es_urgente": F.col("o_orderpriority") == "1-URGENT",
            "total_redondeado": F.col("o_totalprice").cast("decimal(12,0)"),
        }
    )
)
display(df_ordenes_enriquecidas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregaciones
# MAGIC
# MAGIC Siempre con `.alias()`: sin alias la columna se llama `sum(o_totalprice)`.

# COMMAND ----------

df_resumen_mensual = (
    df_ordenes_enriquecidas
    .groupBy("anio", "mes")
    .agg(
        F.count("*").alias("n_ordenes"),
        F.sum("o_totalprice").alias("venta_total"),
        F.round(F.avg("o_totalprice"), 2).alias("ticket_promedio"),
        F.sum(F.col("es_urgente").cast("int")).alias("n_urgentes"),
    )
    .orderBy("anio", "mes")
)
display(df_resumen_mensual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Joins
# MAGIC
# MAGIC | Tipo | Devuelve |
# MAGIC |---|---|
# MAGIC | `inner` | Solo filas con pareja en ambos lados |
# MAGIC | `left` | Todas las de la izquierda (nulos si no hay pareja) |
# MAGIC | `left_anti` | Filas de la izquierda **sin** pareja: ideal para encontrar faltantes |
# MAGIC | `left_semi` | Filas de la izquierda **con** pareja, sin traer columnas de la derecha |
# MAGIC
# MAGIC **Regla BPT:** tipo de join siempre explícito (`how=`).

# COMMAND ----------

df_clientes_nacion = df_clientes.join(
    F.broadcast(df_naciones),                                         # naciones es pequeña: 25 filas
    F.col("c_nationkey") == F.col("n_nationkey"),
    how="left",
).select("c_custkey", "c_name", "c_mktsegment", F.col("n_name").alias("nacion"))

display(df_clientes_nacion.limit(10))

# COMMAND ----------

# Clientes que nunca han hecho una orden
df_sin_ordenes = df_clientes.join(
    df_ordenes, F.col("c_custkey") == F.col("o_custkey"), how="left_anti"
)
print("Clientes sin órdenes:", df_sin_ordenes.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Validar cardinalidad
# MAGIC
# MAGIC Si la dimensión tiene llaves duplicadas, el join **multiplica** filas sin dar error. Siempre compara conteos en joins N:1.

# COMMAND ----------

df_naciones_duplicadas = df_naciones.unionByName(df_naciones.filter(F.col("n_name") == "BRAZIL"))

n_antes = df_clientes.count()
n_despues = df_clientes.join(
    df_naciones_duplicadas, F.col("c_nationkey") == F.col("n_nationkey"), how="left"
).count()
print(f"Antes: {n_antes:,}  Después: {n_despues:,}  → {n_despues - n_antes:,} filas duplicadas por el join")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Leer el plan
# MAGIC
# MAGIC Busca en el plan:
# MAGIC - `BroadcastHashJoin`: el broadcast funcionó.
# MAGIC - `SortMergeJoin`: hubo shuffle de ambos lados.
# MAGIC - `PushedFilters` / `PartitionFilters`: el filtro se empujó a la lectura.
# MAGIC - `Exchange`: cada uno es un shuffle.

# COMMAND ----------

df_clientes_nacion.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente: [03_pyspark_fundamentals_lab](./03_pyspark_fundamentals_lab)
