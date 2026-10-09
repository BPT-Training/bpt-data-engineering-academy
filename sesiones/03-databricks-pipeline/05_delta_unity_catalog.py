# Databricks notebook source
# MAGIC %md
# MAGIC # M5 · Databricks Fundamentals: Unity Catalog y Delta Lake
# MAGIC
# MAGIC | Bloque | Tema |
# MAGIC |---|---|
# MAGIC | 1 | Unity Catalog: catálogo → esquema → tabla / volume |
# MAGIC | 2 | Crear una tabla Delta documentada |
# MAGIC | 3 | Historial y time travel |
# MAGIC | 4 | MERGE idempotente |
# MAGIC | 5 | Schema enforcement |
# MAGIC | 6 | RESTORE |
# MAGIC | 7 | Arquitectura Medallion: la base del M6 |
# MAGIC
# MAGIC > Guía: [03-delta-unity-catalog.md](../../guia-buenas-practicas/03-delta-unity-catalog.md)

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog
# MAGIC
# MAGIC ```
# MAGIC workspace                 ← catálogo  (en proyectos BPT: ambiente → dev / prod)
# MAGIC └── bpt_academy           ← esquema   (en proyectos BPT: capa → bronze / silver / gold)
# MAGIC     ├── aeropuertos_demo  ← tabla Delta
# MAGIC     └── raw               ← volume (archivos: CSV, JSON…)
# MAGIC ```
# MAGIC
# MAGIC **Regla BPT:** siempre nombres de tres niveles (`catalogo.esquema.tabla`).

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear una tabla Delta documentada
# MAGIC
# MAGIC En Databricks, `saveAsTable` crea una tabla **Delta administrada** (managed) por defecto.

# COMMAND ----------

from pyspark.sql import functions as F

TABLA_DEMO = get_table_name(SCHEMA, "aeropuertos_demo")
spark.sql(f"DROP TABLE IF EXISTS {TABLA_DEMO}")

df_aeropuertos = spark.createDataFrame(
    [("BOG", "Bogotá", "Colombia"), ("MDE", "Rionegro", "Colombia"), ("LIM", "Lima", "Perú")],
    "codigo_iata string, ciudad string, pais string",
)
df_aeropuertos.write.saveAsTable(TABLA_DEMO)

spark.sql(f"COMMENT ON TABLE {TABLA_DEMO} IS 'Aeropuertos de ejemplo del M5. Un registro por código IATA.'")
spark.sql(f"ALTER TABLE {TABLA_DEMO} ALTER COLUMN codigo_iata COMMENT 'Código IATA de 3 letras'")

display(spark.sql(f"DESCRIBE TABLE EXTENDED {TABLA_DEMO}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Historial y time travel
# MAGIC
# MAGIC Cada escritura crea una **nueva versión**. Delta guarda un log de transacciones (ACID), así que puedes consultar cualquier versión previa.

# COMMAND ----------

spark.sql(f"UPDATE {TABLA_DEMO} SET ciudad = 'Medellín' WHERE codigo_iata = 'MDE'")
spark.sql(f"DELETE FROM {TABLA_DEMO} WHERE codigo_iata = 'LIM'")

display(spark.sql(f"DESCRIBE HISTORY {TABLA_DEMO}").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

print("Versión 0 (original):")
display(spark.sql(f"SELECT * FROM {TABLA_DEMO} VERSION AS OF 0"))

print("Versión actual:")
display(spark.table(TABLA_DEMO))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE idempotente
# MAGIC
# MAGIC MERGE hace un *upsert*: actualiza las filas que coinciden por llave e inserta las nuevas.
# MAGIC **Reejecutarlo con la misma entrada deja la tabla igual.** Un `append` repetido, en cambio, duplica datos.

# COMMAND ----------

df_cambios = spark.createDataFrame(
    [("MDE", "Rionegro", "Colombia"), ("CLO", "Cali", "Colombia")],   # 1 actualización + 1 nueva
    "codigo_iata string, ciudad string, pais string",
)
df_cambios.createOrReplaceTempView("cambios_aeropuertos")

MERGE_SQL = f"""
    MERGE INTO {TABLA_DEMO} AS t
    USING cambios_aeropuertos AS s
    ON t.codigo_iata = s.codigo_iata
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""

for intento in (1, 2):
    spark.sql(MERGE_SQL)
    print(f"Después del MERGE #{intento}: {spark.table(TABLA_DEMO).count()} filas")

display(spark.table(TABLA_DEMO).orderBy("codigo_iata"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Nota:** En el M6 este patrón está empaquetado en `bpt_utils.delta_writer.merge_by_key(spark, df, tabla, llaves)`.
# MAGIC
# MAGIC También existe la API Python `DeltaTable.forName(spark, tabla).merge(...)`, que hace lo mismo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema enforcement
# MAGIC
# MAGIC Delta **rechaza** escrituras cuyo schema no coincide con el de la tabla. Es una protección, no un estorbo.

# COMMAND ----------

from pyspark.errors import AnalysisException

df_con_columna_extra = spark.createDataFrame(
    [("CTG", "Cartagena", "Colombia", "Rafael Núñez")], "codigo_iata string, ciudad string, pais string, nombre string"
)

try:
    df_con_columna_extra.write.mode("append").saveAsTable(TABLA_DEMO)
except AnalysisException as error:
    print("❌ Escritura rechazada por schema distinto:\n", str(error)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC Si la evolución del schema es **intencional**, se habilita de forma explícita:

# COMMAND ----------

df_con_columna_extra.write.mode("append").option("mergeSchema", "true").saveAsTable(TABLA_DEMO)
display(spark.table(TABLA_DEMO))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RESTORE: deshacer una carga errónea

# COMMAND ----------

spark.sql(f"DELETE FROM {TABLA_DEMO}")   # simulación de un borrado accidental
print("Filas tras el borrado:", spark.table(TABLA_DEMO).count())

version_buena = (
    spark.sql(f"DESCRIBE HISTORY {TABLA_DEMO}")
    .filter(F.col("operation") != "DELETE")
    .agg(F.max("version"))
    .first()[0]
)
spark.sql(f"RESTORE TABLE {TABLA_DEMO} TO VERSION AS OF {version_buena}")
print(f"Restaurada a la versión {version_buena}:", spark.table(TABLA_DEMO).count(), "filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Arquitectura Medallion
# MAGIC
# MAGIC ```
# MAGIC  CSV en Volume ──► BRONZE ──────────► SILVER ─────────────► GOLD
# MAGIC                  dato crudo       limpio, tipado,        modelo para consumo:
# MAGIC                  + metadatos      deduplicado,           dimensiones, hechos,
# MAGIC                  de ingesta       validado (quarantine)  KPIs
# MAGIC ```
# MAGIC
# MAGIC | Capa | Regla de oro |
# MAGIC |---|---|
# MAGIC | Bronze | No se limpia nada: siempre se puede reprocesar desde aquí |
# MAGIC | Silver | Una fila por entidad, confiable |
# MAGIC | Gold | Lista para responder preguntas de negocio |
# MAGIC
# MAGIC > Guía: [04-medallion.md](../../guia-buenas-practicas/04-medallion.md). Lo construimos completo en el M6.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente: [05_delta_unity_catalog_lab](./05_delta_unity_catalog_lab)
