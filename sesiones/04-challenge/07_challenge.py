# Databricks notebook source
# MAGIC %md
# MAGIC # M7 · Challenge
# MAGIC
# MAGIC **Duración:** 20 minutos. Elegir **una** opción y trabajar sobre el pipeline de la Sesión 03.
# MAGIC
# MAGIC | Opción | Capa | Reto |
# MAGIC |---|---|---|
# MAGIC | **A** | Silver | Una regla de calidad que detecte vuelos con datos imposibles |
# MAGIC | **B** | Gold | Un KPI nuevo: ingreso semanal por ruta |
# MAGIC
# MAGIC Al terminar, ejecutar la celda de **validación** de la opción elegida. La evaluación sigue la [rúbrica](./rubrica.md).
# MAGIC
# MAGIC **Requisito:** haber ejecutado el pipeline (`pipeline/99_flights_orch`).

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

from bpt_utils.delta_writer import overwrite_table  # noqa: F401 - opción B

FACT_TABLE = get_table_name(SCHEMA_GOLD, "fct_flight")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Opción A · Regla de calidad en Silver
# MAGIC
# MAGIC **Contexto:** en `fct_flight` aparecen vuelos con miles de minutos de retraso **negativo**:

# COMMAND ----------

display(
    spark.table(FACT_TABLE)
    .orderBy(F.col("delay_minutes").asc_nulls_last())
    .select(
        "flight_id", "scheduled_departure_ts",
        "actual_departure_ts", "delay_minutes",
    )
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tarea:**
# MAGIC 1. En [30_conformance_curated_flights_nt](../03-databricks-pipeline/pipeline/30_conformance_curated_flights_nt), agregar a `FLIGHT_RULES` una regla llamada **`actual_departure_consistent`**: un vuelo no puede salir más de **1 hora antes** de lo programado. Si no tiene salida real, la regla no aplica.
# MAGIC 2. Ejecutar de nuevo `30_conformance_curated_flights_nt` y `40_curated_gold_flights_nt`.
# MAGIC 3. Ejecutar la validación.
# MAGIC
# MAGIC Pista: en Spark SQL, `scheduled_departure_ts - INTERVAL 1 HOUR`.

# COMMAND ----------

# DBTITLE 1,Validación opción A
RULE_NAME = "actual_departure_consistent"
df_quarantine = spark.table(
    get_table_name(SCHEMA_SILVER, "flights_quarantine")
)
caught_flights = {
    row.flight_id
    for row in df_quarantine.filter(
        F.array_contains("_failed_rules", RULE_NAME)
    ).collect()
}
assert caught_flights, (
    f"La regla {RULE_NAME} no atrapó vuelos. "
    "¿Se ejecutó de nuevo el notebook de Silver?"
)
assert caught_flights == {"VL0041", "VL0161"}, (
    f"Se esperaban VL0041 y VL0161; se obtuvo {sorted(caught_flights)}"
)

silver_ids = {
    row.flight_id
    for row in spark.table(get_table_name(SCHEMA_SILVER, "flights"))
    .select("flight_id")
    .collect()
}
assert not caught_flights & silver_ids, "Los vuelos siguen en Silver"

min_delay = spark.table(FACT_TABLE).agg(F.min("delay_minutes")).first()[0]
assert min_delay >= -60, (
    f"Gold aún tiene retrasos de {min_delay} min. "
    "¿Se ejecutó de nuevo el notebook de Gold?"
)
print("✅ Opción A completada")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Opción B · KPI nuevo en Gold
# MAGIC
# MAGIC **Tarea:** crear la tabla `agg_route_weekly_revenue` en el esquema Gold a partir de `fct_flight`:
# MAGIC
# MAGIC | Columna | Definición |
# MAGIC |---|---|
# MAGIC | `week_start` | Lunes de la semana de `flight_date` (tipo `date`) |
# MAGIC | `origin`, `destination` | Ruta |
# MAGIC | `flight_count` | Número de vuelos |
# MAGIC | `booking_count` | Suma de reservas |
# MAGIC | `total_revenue` | Suma de ingreso |
# MAGIC
# MAGIC Granularidad: **un registro por semana y ruta**. Publicar con `overwrite_table(..., comment=...)`, declarando la granularidad en el comentario.
# MAGIC
# MAGIC Pista: `F.date_trunc("week", ...)` devuelve el lunes como timestamp; convertirlo con `F.to_date`.

# COMMAND ----------

KPI_TABLE = get_table_name(SCHEMA_GOLD, "agg_route_weekly_revenue")

# TODO: construir df_route_weekly_revenue desde FACT_TABLE y publicarlo

# COMMAND ----------

# DBTITLE 1,Validación opción B
assert spark.catalog.tableExists(KPI_TABLE), f"No existe {KPI_TABLE}"
df_kpi = spark.table(KPI_TABLE)
df_fact = spark.table(FACT_TABLE)

expected_columns = {
    "week_start", "origin", "destination",
    "flight_count", "booking_count", "total_revenue",
}
missing_columns = expected_columns - set(df_kpi.columns)
assert not missing_columns, f"Faltan columnas: {missing_columns}"
assert dict(df_kpi.dtypes)["week_start"] == "date", (
    "week_start debe ser date"
)
assert df_kpi.filter(F.col("week_start").isNull()).count() == 0
assert df_kpi.filter(F.dayofweek("week_start") != 2).count() == 0, (
    "week_start debe ser lunes"
)
duplicated_grain = (
    df_kpi.groupBy("week_start", "origin", "destination")
    .count()
    .filter("count > 1")
    .count()
)
assert duplicated_grain == 0, "Hay semana y ruta repetidas"
assert (
    df_kpi.agg(F.sum("flight_count")).first()[0] == df_fact.count()
), "El total de vuelos no cuadra con fct_flight"
assert (
    df_kpi.agg(F.sum("total_revenue")).first()[0]
    == df_fact.agg(F.sum("total_revenue")).first()[0]
), "El ingreso total no cuadra con fct_flight"
assert spark.sql(f"DESCRIBE DETAIL {KPI_TABLE}").first().description, (
    "Falta el comentario de la tabla"
)
print("✅ Opción B completada")

# COMMAND ----------

display(df_kpi.orderBy("week_start", F.col("total_revenue").desc()))
