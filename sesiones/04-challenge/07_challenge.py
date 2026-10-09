# Databricks notebook source
# MAGIC %md
# MAGIC # M7 · Challenge
# MAGIC
# MAGIC ⏱️ **20 minutos.** Elige **una** opción y trabaja sobre el pipeline de la sesión 03.
# MAGIC
# MAGIC | Opción | Capa | Reto |
# MAGIC |---|---|---|
# MAGIC | **A** | Silver | Una regla de calidad que detecte vuelos con datos imposibles |
# MAGIC | **B** | Gold | Un KPI nuevo: ingreso semanal por ruta |
# MAGIC
# MAGIC Al terminar, ejecuta la celda de **validación** de tu opción. La evaluación sigue la [rúbrica](./rubrica.md).
# MAGIC
# MAGIC **Requisito:** haber ejecutado `06_0` → `06_3` de la sesión 03.

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Opción A · Regla de calidad en Silver
# MAGIC
# MAGIC **Contexto:** operaciones reporta que en `fct_vuelo` hay vuelos con **miles de minutos de retraso negativo**. Míralos:

# COMMAND ----------

display(
    spark.table(get_table_name(SCHEMA_GOLD, "fct_vuelo"))
    .orderBy(F.col("minutos_retraso").asc_nulls_last())
    .select("vuelo_id", "salida_programada", "salida_real", "minutos_retraso")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tu tarea:**
# MAGIC 1. En [06_2_silver](../03-databricks-pipeline/06_2_silver), agrega a `REGLAS_VUELOS` una regla llamada **`salida_real_coherente`**.
# MAGIC    Un vuelo no puede salir más de **1 hora antes** de lo programado. Si no tiene salida real, la regla no aplica.
# MAGIC 2. Reejecuta `06_2_silver` y `06_3_gold`.
# MAGIC 3. Ejecuta la validación.
# MAGIC
# MAGIC **Nota:** En Spark SQL: `salida_programada - INTERVAL 1 HOUR`.

# COMMAND ----------

# Validación opción A
quarantine = spark.table(get_table_name(SCHEMA_SILVER, "vuelos_quarantine"))
atrapados = {
    r.vuelo_id
    for r in quarantine.filter(F.array_contains("_failed_rules", "salida_real_coherente")).collect()
}
assert atrapados, "La regla 'salida_real_coherente' no atrapó ningún vuelo. ¿Reejecutaste 06_2_silver?"
assert atrapados == {"VL0041", "VL0161"}, f"Se esperaban VL0041 y VL0161, se obtuvo {sorted(atrapados)}"

silver_ids = {r.vuelo_id for r in spark.table(get_table_name(SCHEMA_SILVER, "vuelos")).select("vuelo_id").collect()}
assert not (atrapados & silver_ids), "Los vuelos en quarantine siguen en Silver"

retraso_minimo = spark.table(get_table_name(SCHEMA_GOLD, "fct_vuelo")).agg(F.min("minutos_retraso")).first()[0]
assert retraso_minimo >= -60, f"Gold aún tiene retrasos de {retraso_minimo} min. ¿Reejecutaste 06_3_gold?"
print("✅ Opción A completada")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Opción B · KPI nuevo en Gold
# MAGIC
# MAGIC **Contexto:** comercial necesita ver el **ingreso semanal por ruta**.
# MAGIC
# MAGIC **Tu tarea:** crea la tabla `agg_ingreso_semanal_ruta` en el esquema Gold a partir de `fct_vuelo`:
# MAGIC
# MAGIC | Columna | Definición |
# MAGIC |---|---|
# MAGIC | `semana` | Lunes de la semana de `fecha` (tipo `date`) |
# MAGIC | `origen`, `destino` | Ruta |
# MAGIC | `vuelos` | Número de vuelos |
# MAGIC | `n_reservas` | Suma de reservas |
# MAGIC | `ingreso_total` | Suma de ingreso |
# MAGIC
# MAGIC Granularidad: **un registro por semana y ruta**. Publica con `overwrite_table(..., comment=...)`, declarando la granularidad en el comentario.
# MAGIC
# MAGIC **Nota:** `F.date_trunc("week", ...)` devuelve el lunes como timestamp; conviértelo con `F.to_date`.

# COMMAND ----------

from bpt_utils.delta_writer import overwrite_table

TABLA_KPI = get_table_name(SCHEMA_GOLD, "agg_ingreso_semanal_ruta")

# TODO: construye df_kpi a partir de fct_vuelo y publícalo en TABLA_KPI

# COMMAND ----------

# Validación opción B
assert spark.catalog.tableExists(TABLA_KPI), f"No existe {TABLA_KPI}"
df_kpi = spark.table(TABLA_KPI)
fct = spark.table(get_table_name(SCHEMA_GOLD, "fct_vuelo"))

faltantes = {"semana", "origen", "destino", "vuelos", "n_reservas", "ingreso_total"} - set(df_kpi.columns)
assert not faltantes, f"Faltan columnas: {faltantes}"
assert dict(df_kpi.dtypes)["semana"] == "date", "semana debe ser de tipo date"
assert df_kpi.filter(F.col("semana").isNull()).count() == 0, "Hay semanas nulas"
assert df_kpi.filter(F.dayofweek("semana") != 2).count() == 0, "semana debe ser el lunes"
assert df_kpi.groupBy("semana", "origen", "destino").count().filter("count > 1").count() == 0, \
    "Granularidad rota: hay semana+ruta repetidas"
assert df_kpi.agg(F.sum("vuelos")).first()[0] == fct.count(), "El total de vuelos no cuadra con fct_vuelo"
assert df_kpi.agg(F.sum("ingreso_total")).first()[0] == fct.agg(F.sum("ingreso_total")).first()[0], \
    "El ingreso total no cuadra con fct_vuelo"
assert spark.sql(f"DESCRIBE DETAIL {TABLA_KPI}").first().description, "Falta el COMMENT de la tabla"
print("✅ Opción B completada")

# COMMAND ----------

display(df_kpi.orderBy("semana", F.col("ingreso_total").desc()))
