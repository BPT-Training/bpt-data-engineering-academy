# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Paso 1: Bronze
# MAGIC
# MAGIC **Contrato Bronze:** el dato tal como llegó, más metadatos de ingesta. **No se limpia nada.**
# MAGIC
# MAGIC Los únicos cambios permitidos:
# MAGIC - **Nombres de columna a `snake_case`:** Delta no acepta espacios en los nombres y la fuente trae `"Vuelo Id"`, `"Código IATA"`…
# MAGIC - **Columnas técnicas:** `_ingest_ts` y `_source_file`.
# MAGIC
# MAGIC Todos los valores quedan como **texto**, igual que en el archivo.
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) registra qué archivos ya cargó en el *checkpoint*: reejecutar este notebook **no duplica** datos.

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

from datetime import datetime

from bpt_utils.audit_helper import _udf_add_ingestion_columns, log_execution
from bpt_utils.udf_cleaning import _udf_normalize_column_names

ENTIDADES = ["aeropuertos", "vuelos", "reservas"]

# COMMAND ----------

def contar_filas(nombre_tabla: str) -> int:
    return spark.table(nombre_tabla).count() if spark.catalog.tableExists(nombre_tabla) else 0


def ingestar_bronze(entidad: str) -> None:
    """Carga incremental de los CSV de una entidad a su tabla Bronze, con log de ejecución."""
    tabla_destino = get_table_name(SCHEMA_BRONZE, entidad)
    checkpoint = f"{CHECKPOINT_PATH}/bronze_{entidad}"
    inicio = datetime.now()
    try:
        filas_antes = contar_filas(tabla_destino)

        df_stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/{entidad}")
        )
        # Primero las columnas técnicas (leen _metadata de la fuente), luego los nombres
        df_bronze = _udf_normalize_column_names(_udf_add_ingestion_columns(df_stream))

        consulta = (
            df_bronze.writeStream
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)          # procesa lo pendiente y se detiene
            .toTable(tabla_destino)
        )
        consulta.awaitTermination()

        filas_nuevas = contar_filas(tabla_destino) - filas_antes
        log_execution(
            spark, LOG_TABLE, pipeline="vuelos", layer="bronze", target_table=tabla_destino,
            started_at=inicio, status="OK", rows_written=filas_nuevas,
        )
        print(f"✅ {entidad}: {filas_nuevas} filas nuevas")
    except Exception as error:
        log_execution(
            spark, LOG_TABLE, pipeline="vuelos", layer="bronze", target_table=tabla_destino,
            started_at=inicio, status="ERROR", message=str(error)[:1000],
        )
        raise

# COMMAND ----------

for entidad in ENTIDADES:
    ingestar_bronze(entidad)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contenido de Bronze (sin limpiar, por diseño)

# COMMAND ----------

display(spark.table(get_table_name(SCHEMA_BRONZE, "vuelos")).limit(20))

# COMMAND ----------

spark.table(get_table_name(SCHEMA_BRONZE, "vuelos")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Prueba de idempotencia:** vuelve a ejecutar la celda de ingesta. Debe reportar **0 filas nuevas**.
# MAGIC
# MAGIC **Nota:** `_rescued_data` guarda lo que no encaja con el schema inferido. Si algún día aparece con datos, la fuente cambió.
