# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 00 · Validación del entorno
# MAGIC
# MAGIC **Propósito:** verificar que el workspace está listo para el curso. Ejecutar con **Run all** y compute **Serverless**.
# MAGIC
# MAGIC | Paso | Qué valida |
# MAGIC |---|---|
# MAGIC | 1 | Spark responde y el catálogo `samples` es accesible |
# MAGIC | 2 | Se crean los esquemas y el Volume del curso |
# MAGIC | 3 | Los archivos de práctica se copian al Volume |
# MAGIC | 4 | La librería `bpt_utils` se puede importar |
# MAGIC
# MAGIC **Dependencias:** `00_config` y `shared/config.yml`.
# MAGIC
# MAGIC Cada paso termina con un mensaje de confirmación. Si un paso falla, el mensaje indica qué revisar. La guía de Free Edition (sección 8, Problemas frecuentes) cubre los casos más comunes.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Imports
import shutil

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spark y catálogo `samples`

# COMMAND ----------

# DBTITLE 1,Validar Spark y samples
print("Versión de Spark:", spark.version)

sample_rows = spark.table("samples.tpch.orders").limit(10).count()
assert sample_rows == 10, (
    "No se pudo leer samples.tpch.orders. "
    "Verificar que el notebook esté conectado a Serverless."
)
print("✅ Spark responde y samples.tpch es accesible")

# COMMAND ----------

display(spark.table("samples.tpch.orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Esquemas y Volume del curso
# MAGIC
# MAGIC `IF NOT EXISTS` permite reejecutar la celda sin error: el resultado es el mismo en cada ejecución (idempotencia).

# COMMAND ----------

# DBTITLE 1,Crear esquemas y Volume
course_schemas = [SCHEMA, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]
for schema_name in course_schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

existing_schemas = {
    row.databaseName
    for row in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
}
missing_schemas = set(course_schemas) - existing_schemas
assert not missing_schemas, f"Esquemas no creados: {missing_schemas}"
print("✅ Esquemas y Volume creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Archivos de práctica en el Volume
# MAGIC
# MAGIC Los CSV de `data/` viajan con el repositorio. Se copian al Volume porque es la ubicación de archivos gobernada por Unity Catalog.
# MAGIC
# MAGIC Si esta celda falla, subir los archivos manualmente: **Catalog → workspace → bpt_academy → raw → Upload**, respetando las subcarpetas `airports/`, `flights/` y `bookings/`.

# COMMAND ----------

# DBTITLE 1,Copiar CSV al Volume
ENTITIES = ["airports", "flights", "bookings"]

for entity in ENTITIES:
    target_dir = Path(VOLUME_PATH) / entity
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(
        REPO_ROOT / "data" / entity / f"{entity}.csv",
        target_dir / f"{entity}.csv",
    )

copied_files = dbutils.fs.ls(f"{VOLUME_PATH}/flights")
assert copied_files, f"No hay archivos en {VOLUME_PATH}/flights"
display(copied_files)
print("✅ Archivos de práctica disponibles en el Volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Librería `bpt_utils`

# COMMAND ----------

# DBTITLE 1,Importar bpt_utils
from bpt_utils.udf_cleaning import to_snake_case

assert to_snake_case("Código IATA") == "codigo_iata"
print("✅ bpt_utils importado desde", REPO_ROOT / "src")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entorno listo
# MAGIC
# MAGIC Siguiente paso: [Sesión 01 · Python](../01-python/).
