# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M5 · Delta Lake y Unity Catalog
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | Crear `lab_regions` documentada | Tabla administrada + COMMENT |
# MAGIC | 2 | `upsert_regions` idempotente | MERGE |
# MAGIC | 3 | Consultar y restaurar la versión original | Time travel + RESTORE |
# MAGIC
# MAGIC Solución: notebook `05_delta_unity_catalog_lab_solucion` (subcarpeta `soluciones` de esta misma carpeta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antes de empezar: trabajar sobre una copia
# MAGIC
# MAGIC Las respuestas se escriben en una **copia personal** de este notebook. Así no se pierden cuando se actualiza el repositorio con **Pull**.
# MAGIC
# MAGIC 1. En la barra superior de este notebook, abrir el menú **File** y seleccionar **Clone**.
# MAGIC 2. En **Name**, agregar `_mi_copia` al final del nombre: `05_delta_unity_catalog_lab_mi_copia`.
# MAGIC 3. En **Destination**, verificar que esté seleccionada la **misma carpeta** de este notebook (`sesiones/03-databricks-pipeline` dentro de la carpeta del repositorio). Si aparece otra, navegar hasta esa carpeta y seleccionarla.
# MAGIC 4. Seleccionar **Clone**. La copia se abre automáticamente.
# MAGIC 5. Confirmar que el nombre en la parte superior termina en `_mi_copia` y resolver los ejercicios ahí.
# MAGIC
# MAGIC **Por qué en la misma carpeta:** la copia usa la configuración del curso con rutas relativas a esta carpeta. Si se guarda en otra ubicación (por ejemplo, la carpeta personal), deja de encontrarla.

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports y configuración
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REGIONS_TABLE = get_table_name(SCHEMA, "lab_regions")
REGIONS_COMMENT = "Regiones TPC-H. Un registro por región."
spark.sql(f"DROP TABLE IF EXISTS {REGIONS_TABLE}")  # permite reejecutar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · Crear la tabla
# MAGIC
# MAGIC Crear `REGIONS_TABLE` con el contenido de `samples.tpch.region` (5 filas) y agregarle el comentario `REGIONS_COMMENT`.

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 1
table_detail = spark.sql(f"DESCRIBE DETAIL {REGIONS_TABLE}").first()
assert table_detail.format == "delta"
assert table_detail.description == REGIONS_COMMENT
assert spark.table(REGIONS_TABLE).count() == 5
ORIGINAL_VERSION = (
    spark.sql(f"DESCRIBE HISTORY {REGIONS_TABLE}")
    .agg(F.max("version"))
    .first()[0]
)
print(f"✅ Ejercicio 1 correcto (versión original: {ORIGINAL_VERSION})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · Upsert idempotente
# MAGIC
# MAGIC `upsert_regions(df_changes)` aplica un MERGE por `r_regionkey` sobre `REGIONS_TABLE`. La verificación lo ejecuta **dos veces**: el resultado debe ser el mismo.

# COMMAND ----------

DF_REGION_CHANGES = spark.createDataFrame(
    [(0, "ÁFRICA", "nombre actualizado"), (5, "ANTÁRTIDA", "región nueva")],
    "r_regionkey bigint, r_name string, r_comment string",
)


def upsert_regions(df_changes: DataFrame) -> None:
    """
    Inserta o actualiza regiones por r_regionkey.

    Args:
        df_changes: Regiones nuevas o modificadas, sin llaves repetidas.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 2
upsert_regions(DF_REGION_CHANGES)
upsert_regions(DF_REGION_CHANGES)  # segunda vez: no debe duplicar

df_regions = spark.table(REGIONS_TABLE)
assert df_regions.count() == 6, f"Se esperaban 6, hay {df_regions.count()}"
assert df_regions.filter("r_regionkey = 0").first().r_name == "ÁFRICA"
assert df_regions.filter("r_regionkey = 5").count() == 1
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Time travel y RESTORE
# MAGIC
# MAGIC 1. Guardar en `df_original` la tabla tal como estaba en `ORIGINAL_VERSION`.
# MAGIC 2. Restaurar la tabla a `ORIGINAL_VERSION`.

# COMMAND ----------

# TODO: df_original = ...

# TODO: RESTORE

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 3
assert df_original.count() == 5
assert df_original.filter("r_regionkey = 0").first().r_name == "AFRICA"
assert spark.table(REGIONS_TABLE).count() == 5
last_operation = (
    spark.sql(f"DESCRIBE HISTORY {REGIONS_TABLE}").first().operation
)
assert last_operation == "RESTORE"
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregable
# MAGIC
# MAGIC Una tabla Delta creada, documentada y versionada. Su historial completo:

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {REGIONS_TABLE}").select(
        "version", "timestamp", "operation"
    )
)
