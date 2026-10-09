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

dbutils.widgets.dropdown("reiniciar", "no", ["no", "si"], "¿Borrar tablas y checkpoints?")
REINICIAR = dbutils.widgets.get("reiniciar") == "si"

# COMMAND ----------

from bpt_utils.auditoria import crear_tabla_log

if REINICIAR:
    for esquema in [ESQUEMA_BRONZE, ESQUEMA_SILVER, ESQUEMA_GOLD]:
        for fila in spark.sql(f"SHOW TABLES IN {CATALOGO}.{esquema}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{esquema}.{fila.tableName}")
    spark.sql(f"DROP TABLE IF EXISTS {TABLA_LOG}")
    dbutils.fs.rm(RUTA_CHECKPOINTS, recurse=True)
    print("Pipeline reiniciado")

crear_tabla_log(spark, TABLA_LOG)
print("✅ Tabla de log lista:", TABLA_LOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar que los CSV están en el Volume
# MAGIC
# MAGIC Se copian en la sesión 00. Si faltan, vuelve a ejecutar [00_smoke_test](../00-setup/00_smoke_test).

# COMMAND ----------

for entidad in ["aeropuertos", "vuelos", "reservas"]:
    archivos = dbutils.fs.ls(f"{RUTA_VOLUME}/{entidad}")
    assert archivos, f"No hay archivos en {RUTA_VOLUME}/{entidad}"
    print(f"✅ {entidad}: {[a.name for a in archivos]}")
