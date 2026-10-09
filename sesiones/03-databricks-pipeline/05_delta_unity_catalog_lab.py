# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M5 · Delta Lake y Unity Catalog
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | Crear `lab_regiones` documentada | Tablas managed + COMMENT |
# MAGIC | 2 | `upsert_regiones` idempotente | MERGE |
# MAGIC | 3 | Consultar y restaurar la versión original | Time travel + RESTORE |
# MAGIC
# MAGIC Solución: [soluciones/05_delta_unity_catalog_lab_solucion](./soluciones/05_delta_unity_catalog_lab_solucion)

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TABLA_REGIONES = get_table_name(SCHEMA, "lab_regiones")
spark.sql(f"DROP TABLE IF EXISTS {TABLA_REGIONES}")   # permite reejecutar el lab desde cero

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · Crear la tabla
# MAGIC
# MAGIC Crea `TABLA_REGIONES` con el contenido de `samples.tpch.region` (5 filas) y agrega el comentario de tabla:
# MAGIC `'Regiones TPC-H. Un registro por región.'`

# COMMAND ----------

# TODO

# COMMAND ----------

detalle = spark.sql(f"DESCRIBE DETAIL {TABLA_REGIONES}").first()
assert detalle.format == "delta"
assert detalle.description == "Regiones TPC-H. Un registro por región."
assert spark.table(TABLA_REGIONES).count() == 5
VERSION_ORIGINAL = spark.sql(f"DESCRIBE HISTORY {TABLA_REGIONES}").agg(F.max("version")).first()[0]
print(f"✅ Ejercicio 1 correcto (versión original: {VERSION_ORIGINAL})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · Upsert idempotente
# MAGIC
# MAGIC `upsert_regiones(df_cambios)` aplica un MERGE por `r_regionkey` sobre `TABLA_REGIONES`.
# MAGIC La verificación lo ejecuta **dos veces**: el resultado debe ser el mismo.

# COMMAND ----------

CAMBIOS = spark.createDataFrame(
    [(0, "ÁFRICA", "nombre actualizado"), (5, "ANTÁRTIDA", "región nueva")],
    "r_regionkey bigint, r_name string, r_comment string",
)


def upsert_regiones(df_cambios: DataFrame) -> None:
    # TODO
    ...

# COMMAND ----------

upsert_regiones(CAMBIOS)
upsert_regiones(CAMBIOS)   # segunda vez: no debe duplicar

df = spark.table(TABLA_REGIONES)
assert df.count() == 6, f"Se esperaban 6 regiones, hay {df.count()}"
assert df.filter("r_regionkey = 0").first().r_name == "ÁFRICA"
assert df.filter("r_regionkey = 5").count() == 1
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Time travel y RESTORE
# MAGIC
# MAGIC 1. Guarda en `df_original` la tabla tal como estaba en `VERSION_ORIGINAL` (time travel).
# MAGIC 2. Restaura la tabla a `VERSION_ORIGINAL`.

# COMMAND ----------

# TODO: df_original = ...

# TODO: RESTORE

# COMMAND ----------

assert df_original.count() == 5
assert df_original.filter("r_regionkey = 0").first().r_name == "AFRICA"
assert spark.table(TABLA_REGIONES).count() == 5
assert spark.sql(f"DESCRIBE HISTORY {TABLA_REGIONES}").first().operation == "RESTORE"
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregable
# MAGIC
# MAGIC Una tabla Delta **creada, documentada y versionada**. Mira su historia completa:

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLA_REGIONES}").select("version", "timestamp", "operation"))
