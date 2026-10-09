# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Gold: modelo para consumo
# MAGIC
# MAGIC **Propósito:** construir el modelo que consultan los usuarios de negocio. **Sin lógica de limpieza**: eso ya se hizo en Silver.
# MAGIC
# MAGIC | Tabla | Tipo | Granularidad |
# MAGIC |---|---|---|
# MAGIC | `dim_airport` | Dimensión | Un registro por aeropuerto |
# MAGIC | `fct_flight` | Hechos | Un registro por vuelo |
# MAGIC | `agg_route_punctuality` | Agregado (KPI) | Un registro por ruta origen-destino |
# MAGIC
# MAGIC Las tablas Gold se **recalculan completas** desde Silver (`overwrite`), así que el proceso es idempotente.
# MAGIC
# MAGIC **Dependencias:** `30_conformance_curated_flights_nt`. **Salida:** `<esquema>_gold`.
# MAGIC
# MAGIC > Guía: [04-medallion.md §5](../../../guia-buenas-practicas/04-medallion.md)

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bpt_utils.audit_helper import log_execution
from bpt_utils.delta_writer import overwrite_table
from bpt_utils.validation_functions import (
    validate_not_empty,
    validate_unique_key,
)

# COMMAND ----------

# DBTITLE 1,Configuración
pipeline_params = config["pipeline_parameters"]
PIPELINE_NAME = pipeline_params["name"]
ON_TIME_THRESHOLD_MIN = pipeline_params["on_time_threshold_min"]
DOMESTIC_COUNTRY = "Colombia"

df_airports = spark.table(get_table_name(SCHEMA_SILVER, "airports"))
df_flights = spark.table(get_table_name(SCHEMA_SILVER, "flights"))
df_bookings = spark.table(get_table_name(SCHEMA_SILVER, "bookings"))

# COMMAND ----------

# DBTITLE 1,Persistencia común con validación y log


def publish_gold(
    table: str, df_final: DataFrame, keys: list[str], comment: str
) -> None:
    """
    Escribe una tabla Gold completa, la valida y registra la ejecución.

    Args:
        table: Nombre de la tabla dentro del esquema Gold.
        df_final: Contenido de la tabla.
        keys: Columnas que definen la granularidad.
        comment: Comentario de tabla con la granularidad.

    Raises:
        RuntimeError: Si el paso falla; el error queda en el log.
    """
    target_table = get_table_name(SCHEMA_GOLD, table)
    started_at = datetime.now()
    try:
        overwrite_table(spark, df_final, target_table, comment)
        df_written = spark.table(target_table)
        rows_written = validate_not_empty(df_written, target_table)
        validate_unique_key(df_written, keys, target_table)
        log_execution(
            spark, LOG_TABLE, pipeline=PIPELINE_NAME, layer="gold",
            target_table=target_table, started_at=started_at,
            status="OK", rows_written=rows_written,
        )
        print(f"✅ {table}: {rows_written} filas")
    except Exception as error:
        log_execution(
            spark, LOG_TABLE, pipeline=PIPELINE_NAME, layer="gold",
            target_table=target_table, started_at=started_at,
            status="ERROR", message=str(error)[:1000],
        )
        raise RuntimeError(f"Falló la tabla Gold {table}: {error}") from error

# COMMAND ----------

# DBTITLE 1,dim_airport
df_dim_airport = df_airports.select(
    "iata_code",
    "airport_name",
    "city",
    "country",
    (F.col("country") == DOMESTIC_COUNTRY).alias("is_domestic"),
)
publish_gold(
    "dim_airport", df_dim_airport, ["iata_code"],
    "Dimensión de aeropuertos. Un registro por iata_code.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fct_flight`
# MAGIC
# MAGIC - `delay_minutes` e `is_on_time` aplican solo a vuelos **operados**; en cancelados quedan en null.
# MAGIC - `route_type` es `domestic` si origen y destino están en Colombia.
# MAGIC - Se valida la **cardinalidad**: un registro por vuelo de Silver.

# COMMAND ----------

# DBTITLE 1,fct_flight
df_bookings_by_flight = df_bookings.groupBy("flight_id").agg(
    F.count("*").alias("booking_count"),
    F.sum("fare").alias("total_revenue"),
)
df_domestic = df_dim_airport.select("iata_code", "is_domestic")
is_operated = F.col("status") == "OPERADO"
delay_minutes = (
    (
        F.unix_timestamp("actual_departure_ts")
        - F.unix_timestamp("scheduled_departure_ts")
    )
    / 60
).cast("int")

df_fct_flight = (
    df_flights
    .join(df_bookings_by_flight, on="flight_id", how="left")
    .join(
        F.broadcast(
            df_domestic.withColumnsRenamed(
                {"iata_code": "origin", "is_domestic": "_tmp_origin_dom"}
            )
        ),
        on="origin",
        how="left",
    )
    .join(
        F.broadcast(
            df_domestic.withColumnsRenamed(
                {"iata_code": "destination", "is_domestic": "_tmp_dest_dom"}
            )
        ),
        on="destination",
        how="left",
    )
    .withColumns(
        {
            "flight_date": F.to_date("scheduled_departure_ts"),
            "route_type": F.when(
                F.col("_tmp_origin_dom") & F.col("_tmp_dest_dom"),
                "domestic",
            ).otherwise("international"),
            "delay_minutes": F.when(is_operated, delay_minutes),
            "booking_count": F.coalesce("booking_count", F.lit(0)),
            "total_revenue": F.coalesce(
                "total_revenue", F.lit(0).cast("decimal(24,2)")
            ),
        }
    )
    .withColumn(
        "is_on_time",
        F.when(is_operated, F.col("delay_minutes") <= ON_TIME_THRESHOLD_MIN),
    )
    .select(
        "flight_id", "flight_number", "flight_date", "origin",
        "destination", "route_type", "status", "scheduled_departure_ts",
        "actual_departure_ts", "delay_minutes", "is_on_time",
        "booking_count", "total_revenue",
    )
)

rows_silver, rows_fact = df_flights.count(), df_fct_flight.count()
assert rows_silver == rows_fact, (
    f"Cardinalidad rota: {rows_silver} vuelos en Silver "
    f"y {rows_fact} en fct_flight"
)
publish_gold(
    "fct_flight", df_fct_flight, ["flight_id"],
    "Hechos de vuelos. Un registro por vuelo (operado o cancelado).",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `agg_route_punctuality` · KPI
# MAGIC
# MAGIC **% de puntualidad** = vuelos operados a tiempo / vuelos operados × 100. El KPI se define **una sola vez**, aquí.

# COMMAND ----------

# DBTITLE 1,agg_route_punctuality
df_route_punctuality = (
    spark.table(get_table_name(SCHEMA_GOLD, "fct_flight"))
    .filter(F.col("status") == "OPERADO")
    .groupBy("origin", "destination", "route_type")
    .agg(
        F.count("*").alias("operated_flights"),
        F.sum(F.col("is_on_time").cast("int")).alias("on_time_flights"),
        F.round(F.avg("delay_minutes"), 1).alias("avg_delay_min"),
    )
    .withColumn(
        "on_time_pct",
        F.round(
            F.col("on_time_flights") / F.col("operated_flights") * 100, 1
        ),
    )
)
publish_gold(
    "agg_route_punctuality", df_route_punctuality, ["origin", "destination"],
    "KPI de puntualidad por ruta. Un registro por origen-destino. "
    "Puntual: retraso de 15 minutos o menos.",
)
