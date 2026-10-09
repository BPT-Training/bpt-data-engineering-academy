# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Challenge M7

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción A
# MAGIC
# MAGIC Regla a agregar en `FLIGHT_RULES` de `30_conformance_curated_flights_nt`:
# MAGIC
# MAGIC ```python
# MAGIC {
# MAGIC     "name": "actual_departure_consistent",
# MAGIC     "expression": "actual_departure_ts IS NULL "
# MAGIC     "OR actual_departure_ts >= "
# MAGIC     "scheduled_departure_ts - INTERVAL 1 HOUR",
# MAGIC },
# MAGIC ```
# MAGIC
# MAGIC **Puntos clave para la rúbrica:**
# MAGIC - `actual_departure_ts IS NULL OR …`: sin esa parte, los vuelos cancelados (sin salida real) irían a quarantine, porque una comparación con null da null y `_udf_apply_expectations` la trata como incumplida.
# MAGIC - Con `should_delete_missing=True` en el MERGE, los vuelos salen de Silver al reprocesar. Un MERGE que solo hace upsert los dejaría ahí.
# MAGIC - Hay que ejecutar de nuevo Gold: los cambios se propagan capa por capa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B

# COMMAND ----------

# DBTITLE 1,Imports y configuración
from pyspark.sql import functions as F

from bpt_utils.delta_writer import overwrite_table

FACT_TABLE = get_table_name(SCHEMA_GOLD, "fct_flight")
KPI_TABLE = get_table_name(SCHEMA_GOLD, "agg_route_weekly_revenue")

# COMMAND ----------

# DBTITLE 1,agg_route_weekly_revenue
df_route_weekly_revenue = (
    spark.table(FACT_TABLE)
    .withColumn("week_start", F.to_date(F.date_trunc("week", "flight_date")))
    .groupBy("week_start", "origin", "destination")
    .agg(
        F.count("*").alias("flight_count"),
        F.sum("booking_count").alias("booking_count"),
        F.sum("total_revenue").alias("total_revenue"),
    )
)
overwrite_table(
    spark,
    df_route_weekly_revenue,
    KPI_TABLE,
    comment="Ingreso semanal por ruta. "
    "Un registro por semana (lunes) y origen-destino.",
)
display(
    spark.table(KPI_TABLE).orderBy(
        "week_start", F.col("total_revenue").desc()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Puntos clave para la rúbrica:**
# MAGIC - Se construye desde `fct_flight`, no desde Silver: **los KPIs se derivan del modelo Gold**, así el ingreso cuadra con el resto de consultas.
# MAGIC - La granularidad está declarada en el comentario de la tabla.
# MAGIC - `overwrite` completo: el KPI es idempotente.
