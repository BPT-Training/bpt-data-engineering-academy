# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Bronze: ingesta desde Landing
# MAGIC
# MAGIC **Propósito:** cargar los CSV del Volume a tablas Delta Bronze, de forma incremental.
# MAGIC
# MAGIC **Contrato de Bronze:** el dato tal como llegó, más metadatos de ingesta. **No se limpia nada.** Los únicos cambios permitidos:
# MAGIC - Nombres de columna en `snake_case`: Delta no acepta espacios y la fuente trae `"Vuelo Id"`, `"Código IATA"`.
# MAGIC - Columnas técnicas `_ingest_ts` y `_source_file`.
# MAGIC
# MAGIC **Por qué Auto Loader:** registra en el checkpoint qué archivos ya cargó; reejecutar no duplica datos.
# MAGIC
# MAGIC **Dependencias:** `00_pipeline_setup`. **Entrada:** `/Volumes/.../raw/<entidad>/`. **Salida:** `<esquema>_bronze.<entidad>`.

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime

from bpt_utils.audit_helper import _udf_add_ingestion_columns, log_execution
from bpt_utils.udf_cleaning import _udf_normalize_column_names

# COMMAND ----------

# DBTITLE 1,Configuración
pipeline_params = config["pipeline_parameters"]
PIPELINE_NAME = pipeline_params["name"]
ENTITIES = pipeline_params["entities"]

# COMMAND ----------

# DBTITLE 1,Reglas de negocio: ingesta de una entidad


def count_rows(table_name: str) -> int:
    """
    Cuenta las filas de una tabla, o 0 si todavía no existe.

    Args:
        table_name: Nombre completo de la tabla.

    Returns:
        Número de filas.
    """
    if not spark.catalog.tableExists(table_name):
        return 0
    return spark.table(table_name).count()


def ingest_to_bronze(entity: str) -> None:
    """
    Carga los archivos nuevos de una entidad a su tabla Bronze.

    Args:
        entity: Nombre de la entidad y de su carpeta en el Volume.

    Raises:
        RuntimeError: Si la ingesta falla; el error queda en el log.
    """
    target_table = get_table_name(SCHEMA_BRONZE, entity)
    checkpoint = f"{CHECKPOINT_PATH}/bronze_{entity}"
    started_at = datetime.now()
    try:
        rows_before = count_rows(target_table)
        df_stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/{entity}")
        )
        # Primero las columnas técnicas (leen _metadata de la fuente)
        df_raw = _udf_normalize_column_names(
            _udf_add_ingestion_columns(df_stream)
        )
        query = (
            df_raw.writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)  # procesa lo pendiente y termina
            .toTable(target_table)
        )
        query.awaitTermination()

        rows_written = count_rows(target_table) - rows_before
        log_execution(
            spark, LOG_TABLE, pipeline=PIPELINE_NAME, layer="bronze",
            target_table=target_table, started_at=started_at,
            status="OK", rows_written=rows_written,
        )
        print(f"✅ {entity}: {rows_written} filas nuevas")
    except Exception as error:
        log_execution(
            spark, LOG_TABLE, pipeline=PIPELINE_NAME, layer="bronze",
            target_table=target_table, started_at=started_at,
            status="ERROR", message=str(error)[:1000],
        )
        raise RuntimeError(
            f"Falló la ingesta Bronze de {entity}. "
            f"Revisar los archivos en {VOLUME_PATH}/{entity}: {error}"
        ) from error

# COMMAND ----------

# DBTITLE 1,Persistencia
for entity in ENTITIES:
    ingest_to_bronze(entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contenido de Bronze (sin limpiar, por diseño)

# COMMAND ----------

display(spark.table(get_table_name(SCHEMA_BRONZE, "flights")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Prueba de idempotencia:** volver a ejecutar la celda de persistencia. Debe reportar **0 filas nuevas**.
# MAGIC
# MAGIC `_rescued_data` guarda lo que no encaja con el schema inferido. Si aparece con datos, la fuente cambió.
