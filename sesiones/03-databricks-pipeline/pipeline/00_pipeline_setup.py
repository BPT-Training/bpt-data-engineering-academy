# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Preparación del pipeline
# MAGIC
# MAGIC **Propósito:** crear la tabla de log y, opcionalmente, reiniciar el pipeline (borrar tablas y checkpoints).
# MAGIC
# MAGIC **Parámetros:**
# MAGIC
# MAGIC | Parámetro | Valores | Uso |
# MAGIC |---|---|---|
# MAGIC | `nb_reset` | `false` / `true` | `true` borra todas las tablas y checkpoints del pipeline |
# MAGIC
# MAGIC **Dependencias:** `00_config`; archivos de práctica en el Volume (los copia `00_smoke_test`).
# MAGIC
# MAGIC ## El pipeline completo
# MAGIC
# MAGIC ```
# MAGIC  Volume raw/          BRONZE (20_)         SILVER (30_)                 GOLD (40_)
# MAGIC  ├─ airports/*.csv ─► airports      ─►     airports (+quarantine)  ─►   dim_airport
# MAGIC  ├─ flights/*.csv  ─► flights       ─►     flights  (+quarantine)  ─►   fct_flight
# MAGIC  └─ bookings/*.csv ─► bookings      ─►     bookings (+quarantine)  ─►   agg_route_punctuality
# MAGIC                            └──────────── execution_log (auditoría) ──────────┘
# MAGIC ```
# MAGIC
# MAGIC | Notebook | Qué hace |
# MAGIC |---|---|
# MAGIC | `00_pipeline_setup` | Tabla de log y reinicio opcional |
# MAGIC | `20_landing_conformance_flights_nt` | Bronze: ingesta incremental con Auto Loader |
# MAGIC | `30_conformance_curated_flights_nt` | Silver: limpieza, conformación, calidad y quarantine |
# MAGIC | `40_curated_gold_flights_nt` | Gold: dimensión, hechos y KPI de puntualidad |
# MAGIC | `60_gold_consumption` | Consultas sobre Gold con SQL y Python |
# MAGIC | `90_pipeline_audit` | Log, conciliación de conteos e idempotencia |
# MAGIC | `99_flights_orch` | Orquestación: ejecuta 20 → 30 → 40 en orden |
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/04-medallion.md` (en la raíz del repositorio)

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from bpt_utils.audit_helper import create_log_table

# COMMAND ----------

# DBTITLE 1,Parámetros
dbutils.widgets.dropdown("nb_reset", "false", ["false", "true"], "Reiniciar")
should_reset = dbutils.widgets.get("nb_reset") == "true"
pipeline_params = config["pipeline_parameters"]

# COMMAND ----------

# DBTITLE 1,Reinicio opcional
if should_reset:
    for schema_name in [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]:
        tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema_name}")
        for table_row in tables.collect():
            table_name = get_table_name(schema_name, table_row.tableName)
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {LOG_TABLE}")
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("Pipeline reiniciado")

# COMMAND ----------

# DBTITLE 1,Tabla de log
create_log_table(spark, LOG_TABLE)
print("✅ Tabla de log lista:", LOG_TABLE)

# COMMAND ----------

# DBTITLE 1,Verificar los archivos de la fuente
for entity in pipeline_params["entities"]:
    source_files = dbutils.fs.ls(f"{VOLUME_PATH}/{entity}")
    assert source_files, (
        f"No hay archivos en {VOLUME_PATH}/{entity}. "
        "Ejecutar 00_smoke_test para copiarlos."
    )
    print(f"✅ {entity}: {[file.name for file in source_files]}")
