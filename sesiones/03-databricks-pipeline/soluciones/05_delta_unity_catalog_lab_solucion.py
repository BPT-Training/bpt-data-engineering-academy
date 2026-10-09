# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M5

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TABLA_REGIONES = tabla(ESQUEMA, "lab_regiones")
spark.sql(f"DROP TABLE IF EXISTS {TABLA_REGIONES}")

# COMMAND ----------

# Ejercicio 1
spark.table("samples.tpch.region").write.saveAsTable(TABLA_REGIONES)
spark.sql(f"COMMENT ON TABLE {TABLA_REGIONES} IS 'Regiones TPC-H. Un registro por región.'")

detalle = spark.sql(f"DESCRIBE DETAIL {TABLA_REGIONES}").first()
assert detalle.format == "delta"
assert detalle.description == "Regiones TPC-H. Un registro por región."
assert spark.table(TABLA_REGIONES).count() == 5
VERSION_ORIGINAL = spark.sql(f"DESCRIBE HISTORY {TABLA_REGIONES}").agg(F.max("version")).first()[0]
print(f"✅ Ejercicio 1 correcto (versión original: {VERSION_ORIGINAL})")

# COMMAND ----------

# Ejercicio 2
CAMBIOS = spark.createDataFrame(
    [(0, "ÁFRICA", "nombre actualizado"), (5, "ANTÁRTIDA", "región nueva")],
    "r_regionkey bigint, r_name string, r_comment string",
)


def upsert_regiones(df_cambios: DataFrame) -> None:
    """MERGE por r_regionkey: actualiza las existentes e inserta las nuevas."""
    df_cambios.createOrReplaceTempView("cambios_regiones")
    spark.sql(
        f"""
        MERGE INTO {TABLA_REGIONES} AS t
        USING cambios_regiones AS s
        ON t.r_regionkey = s.r_regionkey
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    # Alternativa con la librería del curso:
    # merge_por_llave(spark, df_cambios, TABLA_REGIONES, ["r_regionkey"])


upsert_regiones(CAMBIOS)
upsert_regiones(CAMBIOS)

df = spark.table(TABLA_REGIONES)
assert df.count() == 6, f"Se esperaban 6 regiones, hay {df.count()}"
assert df.filter("r_regionkey = 0").first().r_name == "ÁFRICA"
assert df.filter("r_regionkey = 5").count() == 1
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# Ejercicio 3
df_original = spark.read.option("versionAsOf", VERSION_ORIGINAL).table(TABLA_REGIONES)
# Equivalente en SQL: SELECT * FROM {TABLA_REGIONES} VERSION AS OF {VERSION_ORIGINAL}

spark.sql(f"RESTORE TABLE {TABLA_REGIONES} TO VERSION AS OF {VERSION_ORIGINAL}")

assert df_original.count() == 5
assert df_original.filter("r_regionkey = 0").first().r_name == "AFRICA"
assert spark.table(TABLA_REGIONES).count() == 5
assert spark.sql(f"DESCRIBE HISTORY {TABLA_REGIONES}").first().operation == "RESTORE"
print("✅ Ejercicio 3 correcto")
