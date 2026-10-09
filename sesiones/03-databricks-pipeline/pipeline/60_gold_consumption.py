# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Consumo de Gold con SQL y Python
# MAGIC
# MAGIC **Propósito:** consultar el modelo Gold directamente, más allá de los indicadores de un reporte. Es el uso final del pipeline: los usuarios de negocio consultan con SQL y los usuarios avanzados procesan con Python.
# MAGIC
# MAGIC | Bloque | Tema |
# MAGIC |---|---|
# MAGIC | 1 | Explorar el modelo: tablas, columnas y granularidad |
# MAGIC | 2 | Consultas SQL sobre hechos y dimensiones |
# MAGIC | 3 | Consultas parametrizadas desde Python |
# MAGIC | 4 | De Gold a pandas: agregar primero, traer después |
# MAGIC
# MAGIC **Dependencias:** `40_curated_gold_flights_nt`. Las mismas consultas SQL se pueden ejecutar en el **SQL Editor** con el SQL Warehouse.
# MAGIC
# MAGIC > Guía: [04-medallion.md §5](../../../guia-buenas-practicas/04-medallion.md) y [06-calidad-y-seguridad.md §3.3](../../../guia-buenas-practicas/06-calidad-y-seguridad.md)

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports y configuración
from pyspark.sql import functions as F

FACT_TABLE = get_table_name(SCHEMA_GOLD, "fct_flight")
DIM_TABLE = get_table_name(SCHEMA_GOLD, "dim_airport")
KPI_TABLE = get_table_name(SCHEMA_GOLD, "agg_route_punctuality")

# Vistas temporales para escribir SQL con nombres cortos en este notebook
spark.table(FACT_TABLE).createOrReplaceTempView("fct_flight")
spark.table(DIM_TABLE).createOrReplaceTempView("dim_airport")
spark.table(KPI_TABLE).createOrReplaceTempView("agg_route_punctuality")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 1 · Explorar el modelo
# MAGIC
# MAGIC Antes de consultar, leer la **granularidad** de cada tabla en su comentario: evita sumar lo que no se debe sumar.

# COMMAND ----------

# DBTITLE 1,Comentario y columnas de cada tabla
for table_name in [FACT_TABLE, DIM_TABLE, KPI_TABLE]:
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    print(f"{table_name}\n  {detail.description}")

display(spark.sql(f"DESCRIBE TABLE {FACT_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 2 · Consultas SQL

# COMMAND ----------

# DBTITLE 1,Rutas con menor puntualidad
# MAGIC %sql
# MAGIC SELECT origin,
# MAGIC        destination,
# MAGIC        operated_flights,
# MAGIC        on_time_pct,
# MAGIC        avg_delay_min
# MAGIC FROM agg_route_punctuality
# MAGIC WHERE operated_flights >= 2
# MAGIC ORDER BY on_time_pct, operated_flights DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Hechos con dimensión: ingreso por ciudad de origen
# MAGIC %sql
# MAGIC SELECT d.city                  AS origin_city,
# MAGIC        COUNT(*)                AS flights,
# MAGIC        SUM(f.booking_count)    AS bookings,
# MAGIC        SUM(f.total_revenue)    AS revenue
# MAGIC FROM fct_flight AS f
# MAGIC JOIN dim_airport AS d
# MAGIC   ON f.origin = d.iata_code
# MAGIC GROUP BY d.city
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# DBTITLE 1,Puntualidad por tipo de ruta y semana
# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('WEEK', flight_date)                      AS week_start,
# MAGIC        route_type,
# MAGIC        COUNT(*)                                             AS operated,
# MAGIC        ROUND(AVG(CASE WHEN is_on_time THEN 100 ELSE 0 END), 1) AS on_time_pct
# MAGIC FROM fct_flight
# MAGIC WHERE status = 'OPERADO'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 3 · Consultas parametrizadas desde Python
# MAGIC
# MAGIC Cuando un valor viene de un parámetro o de un usuario, **nunca se concatena** en el texto SQL: se valida y se pasa como parámetro con nombre.

# COMMAND ----------

# DBTITLE 1,Antipatrón: concatenar el parámetro en el SQL
nb_origin = "BOG"
query = "SELECT * FROM fct_flight WHERE origin = '" + nb_origin + "'"
# Si nb_origin viniera de un usuario, podría alterar la consulta

# COMMAND ----------

# DBTITLE 1,Patrón: validar y usar parámetros con nombre
dbutils.widgets.text("nb_origin", "BOG", "Aeropuerto de origen")
nb_origin = dbutils.widgets.get("nb_origin").strip().upper()

valid_origins = {
    row.iata_code for row in spark.table(DIM_TABLE).collect()
}
if nb_origin not in valid_origins:
    raise ValueError(f"Aeropuerto no válido: {nb_origin}")

df_origin_flights = spark.sql(
    """
    SELECT flight_date, destination, status, delay_minutes, total_revenue
    FROM fct_flight
    WHERE origin = :origin
    ORDER BY flight_date
    """,
    args={"origin": nb_origin},
)
display(df_origin_flights)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 4 · De Gold a pandas
# MAGIC
# MAGIC Para análisis o gráficos puntuales se puede pasar a pandas, pero **siempre después de agregar** en Spark: así viaja al driver un resultado pequeño, no la tabla completa.

# COMMAND ----------

# DBTITLE 1,Patrón: agregar en Spark y traer el resultado
pdf_daily_kpi = (
    spark.table(FACT_TABLE)
    .filter(F.col("status") == "OPERADO")
    .groupBy("flight_date")
    .agg(
        F.count("*").alias("operated"),
        F.avg(F.col("is_on_time").cast("int")).alias("on_time_share"),
    )
    .orderBy("flight_date")
    .toPandas()
)
pdf_daily_kpi["on_time_pct"] = (pdf_daily_kpi["on_time_share"] * 100).round(1)
pdf_daily_kpi.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC En un proyecto real, las tablas Gold tienen millones de filas: `spark.table(...).toPandas()` sin agregar trae todo a una sola máquina y la sesión puede fallar ([02-pyspark.md §7](../../../guia-buenas-practicas/02-pyspark.md)).
