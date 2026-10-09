# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Challenge M7

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción A
# MAGIC
# MAGIC Línea a agregar en `REGLAS_VUELOS` de `06_2_silver`:
# MAGIC
# MAGIC ```python
# MAGIC {"name": "salida_real_coherente",
# MAGIC  "expression": "salida_real IS NULL OR salida_real >= salida_programada - INTERVAL 1 HOUR"},
# MAGIC ```
# MAGIC
# MAGIC **Puntos clave para la rúbrica:**
# MAGIC - `salida_real IS NULL OR …`: sin esa parte, los vuelos cancelados (sin salida real) caerían en quarantine, porque la comparación con null da null y `_udf_apply_expectations` lo trata como incumplido.
# MAGIC - Gracias a `should_delete_missing=True` en el MERGE, los vuelos salen de Silver al reprocesar. Con un MERGE que solo hace upsert se quedarían ahí.
# MAGIC - Hay que reejecutar Gold: los datos se propagan por capas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B

# COMMAND ----------

from pyspark.sql import functions as F

from bpt_utils.delta_writer import overwrite_table

TABLA_KPI = get_table_name(SCHEMA_GOLD, "agg_ingreso_semanal_ruta")

df_kpi = (
    spark.table(get_table_name(SCHEMA_GOLD, "fct_vuelo"))
    .withColumn("semana", F.to_date(F.date_trunc("week", "fecha")))
    .groupBy("semana", "origen", "destino")
    .agg(
        F.count("*").alias("vuelos"),
        F.sum("n_reservas").alias("n_reservas"),
        F.sum("ingreso_total").alias("ingreso_total"),
    )
)
overwrite_table(
    spark, df_kpi, TABLA_KPI,
    comment="Ingreso semanal por ruta. Un registro por semana (lunes) y origen-destino.",
)
display(spark.table(TABLA_KPI).orderBy("semana", F.col("ingreso_total").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC **Puntos clave para la rúbrica:**
# MAGIC - Se construye desde `fct_vuelo`, no desde Silver: **los KPIs se derivan del modelo Gold** y así el ingreso cuadra con el resto de reportes.
# MAGIC - Granularidad explícita en el comentario.
# MAGIC - `overwrite` completo: el KPI es idempotente.
