# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M5

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports y configuración
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bpt_utils.delta_writer import merge_by_key

REGIONS_TABLE = get_table_name(SCHEMA, "lab_regions")
REGIONS_COMMENT = "Regiones TPC-H. Un registro por región."
spark.sql(f"DROP TABLE IF EXISTS {REGIONS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Ejercicio 1
spark.table("samples.tpch.region").write.saveAsTable(REGIONS_TABLE)
spark.sql(f"COMMENT ON TABLE {REGIONS_TABLE} IS '{REGIONS_COMMENT}'")

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

# DBTITLE 1,Ejercicio 2
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
    merge_by_key(spark, df_changes, REGIONS_TABLE, ["r_regionkey"])


upsert_regions(DF_REGION_CHANGES)
upsert_regions(DF_REGION_CHANGES)

df_regions = spark.table(REGIONS_TABLE)
assert df_regions.count() == 6, f"Se esperaban 6, hay {df_regions.count()}"
assert df_regions.filter("r_regionkey = 0").first().r_name == "ÁFRICA"
assert df_regions.filter("r_regionkey = 5").count() == 1
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC Alternativa sin la librería: un `MERGE INTO ... USING ...` en SQL sobre una vista temporal, como en el notebook guiado.

# COMMAND ----------

# DBTITLE 1,Ejercicio 3
df_original = spark.read.option("versionAsOf", ORIGINAL_VERSION).table(
    REGIONS_TABLE
)
spark.sql(f"RESTORE TABLE {REGIONS_TABLE} TO VERSION AS OF {ORIGINAL_VERSION}")

assert df_original.count() == 5
assert df_original.filter("r_regionkey = 0").first().r_name == "AFRICA"
assert spark.table(REGIONS_TABLE).count() == 5
last_operation = (
    spark.sql(f"DESCRIBE HISTORY {REGIONS_TABLE}").first().operation
)
assert last_operation == "RESTORE"
print("✅ Ejercicio 3 correcto")
