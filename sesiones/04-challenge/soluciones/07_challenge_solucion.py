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
# MAGIC {"nombre": "salida_real_coherente",
# MAGIC  "expresion": "salida_real IS NULL OR salida_real >= salida_programada - INTERVAL 1 HOUR"},
# MAGIC ```
# MAGIC
# MAGIC **Puntos clave para la rúbrica:**
# MAGIC - `salida_real IS NULL OR …`: sin esa parte, los vuelos cancelados (sin salida real) caerían en quarantine, porque la comparación con null da null y `aplicar_expectativas` lo trata como incumplido.
# MAGIC - Gracias a `borrar_faltantes=True` en el MERGE, los vuelos salen de Silver al reprocesar. Con un MERGE que solo hace upsert se quedarían ahí.
# MAGIC - Hay que reejecutar Gold: los datos se propagan por capas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B

# COMMAND ----------

from pyspark.sql import functions as F

from bpt_utils.escritura import sobrescribir

TABLA_KPI = tabla(ESQUEMA_GOLD, "agg_ingreso_semanal_ruta")

df_kpi = (
    spark.table(tabla(ESQUEMA_GOLD, "fct_vuelo"))
    .withColumn("semana", F.to_date(F.date_trunc("week", "fecha")))
    .groupBy("semana", "origen", "destino")
    .agg(
        F.count("*").alias("vuelos"),
        F.sum("n_reservas").alias("n_reservas"),
        F.sum("ingreso_total").alias("ingreso_total"),
    )
)
sobrescribir(
    spark, df_kpi, TABLA_KPI,
    comentario="Ingreso semanal por ruta. Un registro por semana (lunes) y origen-destino.",
)
display(spark.table(TABLA_KPI).orderBy("semana", F.col("ingreso_total").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC **Puntos clave para la rúbrica:**
# MAGIC - Se construye desde `fct_vuelo`, no desde Silver: **los KPIs se derivan del modelo Gold** y así el ingreso cuadra con el resto de reportes.
# MAGIC - Granularidad explícita en el comentario.
# MAGIC - `overwrite` completo: el KPI es idempotente.
