# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Paso 4: Auditoría y observabilidad
# MAGIC
# MAGIC Un pipeline en producción debe poder responder, sin abrir el código:
# MAGIC 1. ¿Corrió? ¿Cuándo? ¿Falló?
# MAGIC 2. ¿Cuántas filas leyó, escribió y rechazó cada paso?
# MAGIC 3. ¿Por qué se rechazaron registros?
# MAGIC 4. ¿Qué cambió en cada tabla?
# MAGIC
# MAGIC > Guía: [04-medallion.md §5](../../guia-buenas-practicas/04-medallion.md)

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

from pyspark.sql import functions as F

from bpt_utils.calidad import resumen_calidad

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1–2. Log de ejecuciones

# COMMAND ----------

display(
    spark.table(TABLA_LOG)
    .withColumn("duracion_s", F.col("fin").cast("long") - F.col("inicio").cast("long"))
    .orderBy(F.col("inicio").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conciliación Bronze → Silver
# MAGIC
# MAGIC Por cada entidad: **filas leídas = en Silver + en quarantine + duplicados eliminados**. Si no cuadra, algo se perdió en silencio.

# COMMAND ----------

for entidad, llave in [("aeropuertos", "codigo_iata"), ("vuelos", "vuelo_id"), ("reservas", "reserva_id")]:
    n_bronze = spark.table(tabla(ESQUEMA_BRONZE, entidad)).count()
    n_silver = spark.table(tabla(ESQUEMA_SILVER, entidad)).count()
    n_quarantine = spark.table(tabla(ESQUEMA_SILVER, f"{entidad}_quarantine")).count()
    n_duplicados = n_bronze - n_silver - n_quarantine
    print(f"{entidad:<12} bronze {n_bronze:>5} = silver {n_silver:>5} + quarantine {n_quarantine:>3} + duplicados {n_duplicados:>3}")
    assert n_duplicados >= 0, f"{entidad}: hay más filas en Silver + quarantine que en Bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ¿Por qué se rechazaron registros?

# COMMAND ----------

for entidad in ["aeropuertos", "vuelos", "reservas"]:
    print(f"— {entidad}")
    display(resumen_calidad(spark.table(tabla(ESQUEMA_SILVER, f"{entidad}_quarantine"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ¿Qué cambió en cada tabla?

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {tabla(ESQUEMA_SILVER, 'vuelos')}")
    .select("version", "timestamp", "operation", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba de idempotencia
# MAGIC
# MAGIC 1. Anota los conteos de abajo.
# MAGIC 2. Vuelve a ejecutar `06_1_bronze`, `06_2_silver` y `06_3_gold`.
# MAGIC 3. Ejecuta esta celda otra vez: **los conteos deben ser idénticos**.

# COMMAND ----------

conteos = {
    f"{esquema}.{t}": spark.table(tabla(esquema, t)).count()
    for esquema, t in [
        (ESQUEMA_BRONZE, "vuelos"), (ESQUEMA_SILVER, "vuelos"), (ESQUEMA_SILVER, "reservas"),
        (ESQUEMA_GOLD, "fct_vuelo"), (ESQUEMA_GOLD, "agg_puntualidad_ruta"),
    ]
}
for nombre, n in conteos.items():
    print(f"{nombre:<40} {n:>6}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregable M6
# MAGIC
# MAGIC Pipeline funcional **Bronze → Silver → Gold** con:
# MAGIC - ✅ Ingesta incremental e idempotente.
# MAGIC - ✅ Reglas de calidad declarativas y quarantine.
# MAGIC - ✅ Modelo dimensional y KPI en Gold.
# MAGIC - ✅ Log de auditoría y conciliación de conteos.
