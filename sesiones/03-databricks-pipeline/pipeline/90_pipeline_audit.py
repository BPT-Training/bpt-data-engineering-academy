# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Auditoría y observabilidad
# MAGIC
# MAGIC **Propósito:** responder sin abrir el código:
# MAGIC 1. ¿Corrió? ¿Cuándo? ¿Falló?
# MAGIC 2. ¿Cuántas filas leyó, escribió y rechazó cada paso?
# MAGIC 3. ¿Por qué se rechazaron registros?
# MAGIC 4. ¿Qué cambió en cada tabla?
# MAGIC
# MAGIC **Dependencias:** pipeline ejecutado (`20` → `30` → `40`).
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/04-medallion.md` §7 (en la raíz del repositorio)

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports y configuración
from pyspark.sql import functions as F

from bpt_utils.udf_quality import _udf_summarize_quality

ENTITY_KEYS = {
    "airports": "iata_code",
    "flights": "flight_id",
    "bookings": "booking_id",
}

# COMMAND ----------

# DBTITLE 1,1–2. Log de ejecuciones
display(
    spark.table(LOG_TABLE)
    .withColumn(
        "duration_s",
        F.col("finished_at").cast("long") - F.col("started_at").cast("long"),
    )
    .orderBy(F.col("started_at").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conciliación Bronze → Silver
# MAGIC
# MAGIC Por entidad: **filas en Bronze = Silver + quarantine + duplicados eliminados**. Si no cuadra, algo se perdió en silencio.

# COMMAND ----------

# DBTITLE 1,Conciliación de conteos
for entity in ENTITY_KEYS:
    bronze_rows = spark.table(get_table_name(SCHEMA_BRONZE, entity)).count()
    silver_rows = spark.table(get_table_name(SCHEMA_SILVER, entity)).count()
    quarantine_rows = spark.table(
        get_table_name(SCHEMA_SILVER, f"{entity}_quarantine")
    ).count()
    duplicate_rows = bronze_rows - silver_rows - quarantine_rows
    print(
        f"{entity:<9} bronze {bronze_rows:>5} = silver {silver_rows:>5} "
        f"+ quarantine {quarantine_rows:>3} + duplicados {duplicate_rows:>3}"
    )
    assert duplicate_rows >= 0, (
        f"{entity}: Silver + quarantine supera a Bronze"
    )

# COMMAND ----------

# DBTITLE 1,3. Motivos de rechazo
for entity in ENTITY_KEYS:
    print(f"— {entity}")
    display(
        _udf_summarize_quality(
            spark.table(get_table_name(SCHEMA_SILVER, f"{entity}_quarantine"))
        )
    )

# COMMAND ----------

# DBTITLE 1,4. Historial de cambios de Silver flights
display(
    spark.sql(
        f"DESCRIBE HISTORY {get_table_name(SCHEMA_SILVER, 'flights')}"
    ).select("version", "timestamp", "operation", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba de idempotencia
# MAGIC
# MAGIC 1. Anotar los conteos de la celda siguiente.
# MAGIC 2. Ejecutar de nuevo el pipeline (`99_flights_orch` con `wf_reset = false`).
# MAGIC 3. Ejecutar otra vez la celda: **los conteos deben ser idénticos**.

# COMMAND ----------

# DBTITLE 1,Conteos de control
control_tables = [
    (SCHEMA_BRONZE, "flights"),
    (SCHEMA_SILVER, "flights"),
    (SCHEMA_SILVER, "bookings"),
    (SCHEMA_GOLD, "fct_flight"),
    (SCHEMA_GOLD, "agg_route_punctuality"),
]
for schema_name, table in control_tables:
    row_count = spark.table(get_table_name(schema_name, table)).count()
    print(f"{schema_name}.{table:<24} {row_count:>6}")
