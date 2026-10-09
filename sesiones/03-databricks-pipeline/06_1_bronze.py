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

from bpt_utils.auditoria import agregar_columnas_ingesta, registrar_ejecucion
from bpt_utils.limpieza import normalizar_columnas

ENTIDADES = ["aeropuertos", "vuelos", "reservas"]

# COMMAND ----------

def contar_filas(nombre_tabla: str) -> int:
    return spark.table(nombre_tabla).count() if spark.catalog.tableExists(nombre_tabla) else 0


def ingestar_bronze(entidad: str) -> None:
    """Carga incremental de los CSV de una entidad a su tabla Bronze, con log de ejecución."""
    tabla_destino = tabla(ESQUEMA_BRONZE, entidad)
    checkpoint = f"{RUTA_CHECKPOINTS}/bronze_{entidad}"
    inicio = datetime.now()
    try:
        filas_antes = contar_filas(tabla_destino)

        df_stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
            .option("header", "true")
            .load(f"{RUTA_VOLUME}/{entidad}")
        )
        # Primero las columnas técnicas (leen _metadata de la fuente), luego los nombres
        df_bronze = normalizar_columnas(agregar_columnas_ingesta(df_stream))

        consulta = (
            df_bronze.writeStream
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)          # procesa lo pendiente y se detiene
            .toTable(tabla_destino)
        )
        consulta.awaitTermination()

        filas_nuevas = contar_filas(tabla_destino) - filas_antes
        registrar_ejecucion(
            spark, TABLA_LOG, pipeline="vuelos", capa="bronze", tabla_destino=tabla_destino,
            inicio=inicio, estado="OK", filas_escritas=filas_nuevas,
        )
        print(f"✅ {entidad}: {filas_nuevas} filas nuevas")
    except Exception as error:
        registrar_ejecucion(
            spark, TABLA_LOG, pipeline="vuelos", capa="bronze", tabla_destino=tabla_destino,
            inicio=inicio, estado="ERROR", mensaje=str(error)[:1000],
        )
        raise

# COMMAND ----------

for entidad in ENTIDADES:
    ingestar_bronze(entidad)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contenido de Bronze (sin limpiar, por diseño)

# COMMAND ----------

display(spark.table(tabla(ESQUEMA_BRONZE, "vuelos")).limit(20))

# COMMAND ----------

spark.table(tabla(ESQUEMA_BRONZE, "vuelos")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Prueba de idempotencia:** vuelve a ejecutar la celda de ingesta. Debe reportar **0 filas nuevas**.
# MAGIC
# MAGIC **Nota:** `_rescued_data` guarda lo que no encaja con el schema inferido. Si algún día aparece con datos, la fuente cambió.
