# Databricks notebook source
# MAGIC %md
# MAGIC # M5 · Delta Lake y Unity Catalog
# MAGIC
# MAGIC **Propósito:** crear, escribir y recuperar tablas Delta en Unity Catalog con escrituras idempotentes.
# MAGIC
# MAGIC | Bloque | Minutos | Tema |
# MAGIC |---|---|---|
# MAGIC | 1 | 0–15 | Unity Catalog y tablas documentadas |
# MAGIC | 2 | 15–30 | Historial y time travel |
# MAGIC | 3 | 30–45 | Escritura idempotente: `append` frente a `MERGE`; schema enforcement |
# MAGIC | 4 | 45–60 | `RESTORE` y arquitectura Medallion |
# MAGIC
# MAGIC **Dependencias:** `00_config` (esquema del curso). **Compute:** Serverless.
# MAGIC
# MAGIC **Ejercicios:** notebook `05_delta_unity_catalog_lab` (en esta misma carpeta)
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/03-delta-unity-catalog.md` (en la raíz del repositorio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## El caso de la sesión: la tabla maestra de aeropuertos
# MAGIC
# MAGIC Casi todo proyecto tiene tablas maestras que otros procesos consultan. En esta sesión se crea una de aeropuertos y se le pasa todo lo que le pasa en la vida real: cambios, reintentos de carga, una columna nueva en la fuente y un borrado por error.
# MAGIC
# MAGIC | Etapa | Qué le pasa a la tabla | Bloque | En un proyecto real |
# MAGIC |---|---|---|---|
# MAGIC | 1. Nace documentada | Tabla Delta `airports_demo` con 3 aeropuertos (`BOG`, `MDE`, `LIM`) y comentarios en tabla y columna | 1 | Tablas en Unity Catalog que cualquiera entiende sin preguntar |
# MAGIC | 2. Cambia | `MDE` pasa a «Medellín» y se borra `LIM`; el historial guarda cada versión | 2 | Auditar qué cambió y consultar cómo estaba antes |
# MAGIC | 3. Se recarga | Un `append` reejecutado duplica filas; el `MERGE` por llave deja la tabla igual aunque se reintente; Delta rechaza una columna inesperada | 3 | Jobs que fallan y se reintentan sin dañar los datos |
# MAGIC | 4. Se recupera | Un `DELETE` sin filtro la deja vacía y `RESTORE` la devuelve a la última versión buena | 4 | Recuperarse de un error en minutos, sin reconstruir a mano |
# MAGIC
# MAGIC **Ejemplo de un registro a lo largo de la sesión:**
# MAGIC
# MAGIC | Momento | `MDE` |
# MAGIC |---|---|
# MAGIC | Creación | `city = "Rionegro"` |
# MAGIC | `UPDATE` | `city = "Medellín"` (la versión anterior sigue en el historial) |
# MAGIC | `MERGE` | `city = "Rionegro"`, sin duplicarse aunque el `MERGE` corra dos veces |
# MAGIC | Borrado y `RESTORE` | Desaparece con el `DELETE` y vuelve con el `RESTORE` |

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 1 · Unity Catalog
# MAGIC
# MAGIC ```
# MAGIC workspace                 ← catálogo  (en proyectos: ambiente dev / qa / prd)
# MAGIC └── bpt_academy           ← esquema   (en proyectos: capa bronze / silver / gold)
# MAGIC     ├── airports_demo     ← tabla Delta
# MAGIC     └── raw               ← volume (archivos: CSV, JSON…)
# MAGIC ```
# MAGIC
# MAGIC **Regla:** siempre nombres de tres niveles (`catalogo.esquema.tabla`), construidos desde la configuración, nunca escritos a mano.

# COMMAND ----------

# DBTITLE 1,Contenido del esquema y del Volume
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# DBTITLE 1,Crear una tabla Delta documentada
DEMO_TABLE = get_table_name(SCHEMA, "airports_demo")
spark.sql(f"DROP TABLE IF EXISTS {DEMO_TABLE}")

df_airports = spark.createDataFrame(
    [
        ("BOG", "Bogotá", "Colombia"),
        ("MDE", "Rionegro", "Colombia"),
        ("LIM", "Lima", "Perú"),
    ],
    "iata_code string, city string, country string",
)
df_airports.write.saveAsTable(DEMO_TABLE)  # Delta administrada por defecto

spark.sql(
    f"COMMENT ON TABLE {DEMO_TABLE} "
    "IS 'Aeropuertos de ejemplo. Un registro por código IATA.'"
)
spark.sql(
    f"ALTER TABLE {DEMO_TABLE} ALTER COLUMN iata_code "
    "COMMENT 'Código IATA de 3 letras'"
)
display(spark.sql(f"DESCRIBE TABLE EXTENDED {DEMO_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 2 · Historial y time travel
# MAGIC
# MAGIC Cada escritura crea una **versión nueva**. Delta guarda un log de transacciones, así que cualquier versión anterior se puede consultar.

# COMMAND ----------

# DBTITLE 1,Cambios y su historial
spark.sql(
    f"UPDATE {DEMO_TABLE} SET city = 'Medellín' WHERE iata_code = 'MDE'"
)
spark.sql(f"DELETE FROM {DEMO_TABLE} WHERE iata_code = 'LIM'")

display(
    spark.sql(f"DESCRIBE HISTORY {DEMO_TABLE}").select(
        "version", "timestamp", "operation", "operationMetrics"
    )
)

# COMMAND ----------

# DBTITLE 1,Time travel: la versión 0 frente a la actual
display(spark.sql(f"SELECT * FROM {DEMO_TABLE} VERSION AS OF 0"))
display(spark.table(DEMO_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 3 · Escritura idempotente
# MAGIC
# MAGIC Un proceso es **idempotente** si al reejecutarlo con la misma entrada la tabla queda igual. Los jobs fallan y se reintentan: sin idempotencia, cada reintento duplica datos.

# COMMAND ----------

# DBTITLE 1,Antipatrón: append reejecutado duplica filas
APPEND_TABLE = get_table_name(SCHEMA, "airports_append_demo")
spark.sql(f"DROP TABLE IF EXISTS {APPEND_TABLE}")

for attempt in (1, 2):
    df_airports.write.mode("append").saveAsTable(APPEND_TABLE)
    row_count = spark.table(APPEND_TABLE).count()
    print(f"Después del append #{attempt}: {row_count} filas")

# COMMAND ----------

# DBTITLE 1,Patrón: MERGE por llave
df_changes = spark.createDataFrame(
    [("MDE", "Rionegro", "Colombia"), ("CLO", "Cali", "Colombia")],
    "iata_code string, city string, country string",
)
df_changes.createOrReplaceTempView("airport_changes")

MERGE_SQL = f"""
    MERGE INTO {DEMO_TABLE} AS t
    USING airport_changes AS s
    ON t.iata_code = s.iata_code
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""
for attempt in (1, 2):
    spark.sql(MERGE_SQL)
    row_count = spark.table(DEMO_TABLE).count()
    print(f"Después del MERGE #{attempt}: {row_count} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC En el pipeline del M6 este patrón se usa desde `bpt_utils.delta_writer.merge_by_key`.
# MAGIC
# MAGIC ## Schema enforcement
# MAGIC
# MAGIC Delta **rechaza** escrituras cuyo schema no coincide con el de la tabla. Un cambio accidental en la fuente no altera una tabla de consumo.

# COMMAND ----------

# DBTITLE 1,Escritura rechazada por schema distinto
df_extra_column = spark.createDataFrame(
    [("CTG", "Cartagena", "Colombia", "Rafael Núñez")],
    "iata_code string, city string, country string, airport_name string",
)
try:
    df_extra_column.write.mode("append").saveAsTable(DEMO_TABLE)
except AnalysisException as error:
    print("Escritura rechazada:", str(error)[:200])

# COMMAND ----------

# DBTITLE 1,Evolución de schema intencional
(
    df_extra_column.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(DEMO_TABLE)
)
display(spark.table(DEMO_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 4 · RESTORE y arquitectura Medallion
# MAGIC
# MAGIC Si una carga dañó la tabla, se restaura la versión anterior en lugar de corregir a mano.

# COMMAND ----------

# DBTITLE 1,Simulación de un borrado accidental y su recuperación
spark.sql(f"DELETE FROM {DEMO_TABLE}")
print("Filas tras el borrado:", spark.table(DEMO_TABLE).count())

last_good_version = (
    spark.sql(f"DESCRIBE HISTORY {DEMO_TABLE}")
    .filter(F.col("operation") != "DELETE")
    .agg(F.max("version"))
    .first()[0]
)
spark.sql(f"RESTORE TABLE {DEMO_TABLE} TO VERSION AS OF {last_good_version}")
row_count = spark.table(DEMO_TABLE).count()
print(f"Restaurada a la versión {last_good_version}: {row_count} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquitectura Medallion
# MAGIC
# MAGIC ```
# MAGIC  Archivos (Volume) ─► BRONZE ─────────► SILVER ──────────────► GOLD
# MAGIC  Landing (10_)        Conformance (20_)  Curated (30_)          Gold (40_)
# MAGIC                       dato crudo +       limpio, tipado,        dimensiones,
# MAGIC                       metadatos          deduplicado y          hechos y KPIs
# MAGIC                                          validado
# MAGIC ```
# MAGIC
# MAGIC | Capa | Regla |
# MAGIC |---|---|
# MAGIC | Bronze | No se limpia nada: siempre se puede reprocesar desde aquí |
# MAGIC | Silver | Una fila por entidad, confiable |
# MAGIC | Gold | Lista para responder preguntas de negocio |
# MAGIC
# MAGIC Se construye completo en el M6: subcarpeta `pipeline` de esta misma carpeta.
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/04-medallion.md` (en la raíz del repositorio)
