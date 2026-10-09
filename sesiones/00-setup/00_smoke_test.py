# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 00 · Smoke test del entorno
# MAGIC
# MAGIC Ejecuta este notebook completo (**Run all**) con compute **Serverless**. Al final debes ver ✅ en todas las verificaciones.
# MAGIC
# MAGIC | Paso | Qué valida |
# MAGIC |---|---|
# MAGIC | 1 | Spark responde y el catálogo `samples` es accesible |
# MAGIC | 2 | Se crean el esquema del curso y el Volume |
# MAGIC | 3 | Los CSV del repo se copian al Volume |
# MAGIC | 4 | `bpt_utils` se puede importar desde `src/` |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spark y catálogo `samples`

# COMMAND ----------

print("Versión de Spark:", spark.version)

n_ordenes = spark.table("samples.tpch.orders").limit(10).count()
assert n_ordenes == 10, "No se pudo leer samples.tpch.orders"
print("✅ Spark responde y samples.tpch es accesible")

# COMMAND ----------

display(spark.table("samples.tpch.orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Esquemas y Volume del curso
# MAGIC
# MAGIC `IF NOT EXISTS` hace que la celda se pueda reejecutar sin error: **idempotencia** desde el día uno.

# COMMAND ----------

for esquema in [ESQUEMA, ESQUEMA_BRONZE, ESQUEMA_SILVER, ESQUEMA_GOLD]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUME}")

esquemas = {r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOGO}").collect()}
assert {ESQUEMA, ESQUEMA_BRONZE, ESQUEMA_SILVER, ESQUEMA_GOLD} <= esquemas
print("✅ Esquemas y Volume creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Copiar los CSV del repo al Volume
# MAGIC
# MAGIC Los archivos de `data/` viajan con el repo. Los copiamos al Volume, que es donde Unity Catalog gobierna los archivos.
# MAGIC Si esta celda falla, súbelos a mano: **Catalog Explorer → workspace → bpt_academy → raw → Upload**, respetando las subcarpetas `aeropuertos/`, `vuelos/` y `reservas/`.

# COMMAND ----------

import shutil

for entidad in ["aeropuertos", "vuelos", "reservas"]:
    destino = Path(RUTA_VOLUME) / entidad
    destino.mkdir(parents=True, exist_ok=True)
    shutil.copy(RAIZ_REPO / "data" / entidad / f"{entidad}.csv", destino / f"{entidad}.csv")

display(dbutils.fs.ls(f"{RUTA_VOLUME}/vuelos"))
assert len(dbutils.fs.ls(f"{RUTA_VOLUME}/vuelos")) >= 1
print("✅ CSV disponibles en el Volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Importar `bpt_utils`

# COMMAND ----------

from bpt_utils.limpieza import a_snake_case

assert a_snake_case("Código IATA") == "codigo_iata"
print("✅ bpt_utils importado desde", RAIZ_REPO / "src")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entorno listo
# MAGIC
# MAGIC Siguiente paso: [Sesión 01 · Python](../01-python/).
