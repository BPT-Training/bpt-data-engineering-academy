# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Paso 0: Preparar el pipeline
# MAGIC
# MAGIC **Dominio de ejemplo:** operación de vuelos, con aeropuertos, vuelos y reservas. Son datos **sintéticos**, generados con `scripts/generar_datos.py`.
# MAGIC
# MAGIC ```
# MAGIC  Volume raw/                BRONZE              SILVER                         GOLD
# MAGIC  ├─ aeropuertos/*.csv ──►  aeropuertos  ──►  aeropuertos (+quarantine) ──►  dim_aeropuerto
# MAGIC  ├─ vuelos/*.csv      ──►  vuelos       ──►  vuelos      (+quarantine) ──►  fct_vuelo
# MAGIC  └─ reservas/*.csv    ──►  reservas     ──►  reservas    (+quarantine) ──►  agg_puntualidad_ruta
# MAGIC                                    │                │                         │
# MAGIC                                    └──────── log_ejecuciones (auditoría) ─────┘
# MAGIC ```
# MAGIC
# MAGIC | Notebook | Qué hace |
# MAGIC |---|---|
# MAGIC | `06_0_setup_pipeline` | Tabla de log; opción de reiniciar todo |
# MAGIC | `06_1_bronze` | Ingesta incremental con Auto Loader |
# MAGIC | `06_2_silver` | Limpieza, tipado, dedup, calidad y quarantine |
# MAGIC | `06_3_gold` | Dimensión, hechos y KPI de puntualidad |
# MAGIC | `06_4_auditoria` | Log, calidad e idempotencia |
# MAGIC
# MAGIC > Guía: [04-medallion.md](../../guia-buenas-practicas/04-medallion.md)

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

dbutils.widgets.dropdown("nb_reset", "no", ["no", "si"], "¿Borrar tablas y checkpoints?")
REINICIAR = dbutils.widgets.get("nb_reset") == "si"

# COMMAND ----------

from bpt_utils.audit_helper import create_log_table

if REINICIAR:
    for esquema in [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]:
        for fila in spark.sql(f"SHOW TABLES IN {CATALOG}.{esquema}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{esquema}.{fila.tableName}")
    spark.sql(f"DROP TABLE IF EXISTS {LOG_TABLE}")
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("Pipeline reiniciado")

create_log_table(spark, LOG_TABLE)
print("✅ Tabla de log lista:", LOG_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar que los CSV están en el Volume
# MAGIC
# MAGIC Se copian en la sesión 00. Si faltan, vuelve a ejecutar [00_smoke_test](../00-setup/00_smoke_test).

# COMMAND ----------

for entidad in ["aeropuertos", "vuelos", "reservas"]:
    archivos = dbutils.fs.ls(f"{VOLUME_PATH}/{entidad}")
    assert archivos, f"No hay archivos en {VOLUME_PATH}/{entidad}"
    print(f"✅ {entidad}: {[a.name for a in archivos]}")
