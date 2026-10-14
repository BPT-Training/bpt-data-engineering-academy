# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Silver: limpieza, conformación y calidad
# MAGIC
# MAGIC **Propósito:** convertir Bronze en datos confiables: una fila por entidad, tipada, con nombres en inglés y validada.
# MAGIC
# MAGIC ```
# MAGIC Bronze ─► conformar nombres ─► limpiar y tipar ─► deduplicar ─► reglas de calidad ─┬─► válidos   ─► MERGE a Silver
# MAGIC                                                                                    └─► inválidos ─► <tabla>_quarantine
# MAGIC ```
# MAGIC
# MAGIC - Las **reglas de calidad se declaran como datos** (listas `*_RULES`), no como `if` dispersos.
# MAGIC - Silver se recalcula desde Bronze completo; el MERGE borra lo que dejó de ser válido.
# MAGIC - La tabla resultado se valida (esquema, no vacía, llave única, nulos) antes de terminar.
# MAGIC
# MAGIC **Dependencias:** `20_landing_conformance_flights_nt`. **Salida:** `<esquema>_silver.<entidad>` y `<entidad>_quarantine`.
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/04-medallion.md` §4 y §6 (en la raíz del repositorio)

# COMMAND ----------

# MAGIC %run ../../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from collections.abc import Callable
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bpt_utils.audit_helper import log_execution
from bpt_utils.delta_writer import merge_by_key, overwrite_table
from bpt_utils.udf_cleaning import (
    _udf_clean_columns,
    _udf_drop_duplicates_by_key,
    parse_timestamp,
)
from bpt_utils.udf_quality import (
    _udf_apply_expectations,
    _udf_summarize_quality,
)
from bpt_utils.validation_functions import (
    validate_no_nulls,
    validate_not_empty,
    validate_schema,
    validate_unique_key,
)

# COMMAND ----------

# DBTITLE 1,Configuración
pipeline_params = config["pipeline_parameters"]
PIPELINE_NAME = pipeline_params["name"]
DATE_FORMATS = pipeline_params["source_date_formats"]

# Duplicados: gana la carga más reciente; desempate por archivo
LATEST_FIRST = [F.col("_ingest_ts").desc(), F.col("_source_file").desc()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Función común
# MAGIC
# MAGIC Una sola función aplica el patrón a las tres entidades: transformar → validar calidad → escribir → validar la tabla → registrar en el log.

# COMMAND ----------

# DBTITLE 1,Reglas de negocio: procesamiento común


def process_silver(
    entity: str,
    transform: Callable[[DataFrame], DataFrame],
    rules: list[dict],
    keys: list[str],
    critical_columns: list[str],
    comment: str,
) -> None:
    """
    Procesa una entidad de Bronze a Silver con quarantine y log.

    Args:
        entity: Nombre de la entidad (tabla Bronze y Silver).
        transform: Conformación, limpieza y deduplicación.
        rules: Reglas de calidad (name, expression).
        keys: Llave de negocio para el MERGE.
        critical_columns: Columnas que no admiten nulos.
        comment: Comentario de la tabla Silver.

    Raises:
        RuntimeError: Si el paso falla; el error queda en el log.
    """
    source_table = get_table_name(SCHEMA_BRONZE, entity)
    target_table = get_table_name(SCHEMA_SILVER, entity)
    started_at = datetime.now()
    try:
        df_raw = spark.table(source_table)
        df_clean = transform(df_raw)

        df_valid, df_invalid = _udf_apply_expectations(df_clean, rules)
        helper_columns = [
            name for name in df_clean.columns if name.startswith("_tmp_")
        ]
        df_valid = df_valid.drop(*helper_columns)
        df_invalid = df_invalid.drop(*helper_columns)

        merge_by_key(
            spark, df_valid, target_table, keys,
            should_delete_missing=True, comment=comment,
        )
        overwrite_table(spark, df_invalid, f"{target_table}_quarantine")

        df_final = spark.table(target_table)
        validate_schema(df_final, set(keys + critical_columns), target_table)
        rows_written = validate_not_empty(df_final, target_table)
        validate_unique_key(df_final, keys, target_table)
        validate_no_nulls(df_final, critical_columns, target_table)

        rows_read = df_raw.count()
        rows_rejected = spark.table(f"{target_table}_quarantine").count()
        log_execution(
            spark, LOG_TABLE, pipeline=PIPELINE_NAME, layer="silver",
            target_table=target_table, started_at=started_at,
            status="OK", rows_read=rows_read, rows_written=rows_written,
            rows_rejected=rows_rejected,
        )
        print(
            f"✅ {entity}: leídas {rows_read} · Silver {rows_written} "
            f"· quarantine {rows_rejected}"
        )
    except Exception as error:
        log_execution(
            spark, LOG_TABLE, pipeline=PIPELINE_NAME, layer="silver",
            target_table=target_table, started_at=started_at,
            status="ERROR", message=str(error)[:1000],
        )
        raise RuntimeError(
            f"Falló el procesamiento Silver de {entity}: {error}"
        ) from error

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aeropuertos

# COMMAND ----------

# DBTITLE 1,Aeropuertos: reglas y transformación
AIRPORT_COLUMNS = {
    "codigo_iata": "iata_code",
    "nombre": "airport_name",
    "ciudad": "city",
    "pais": "country",
}
AIRPORT_RULES = [
    {
        "name": "iata_code_valid",
        "expression": "iata_code RLIKE '^[A-Z]{3}$'",
    },
    {
        "name": "airport_name_not_null",
        "expression": "airport_name IS NOT NULL",
    },
]


def transform_airports(df_raw: DataFrame) -> DataFrame:
    """
    Conforma, limpia y deduplica los aeropuertos.

    Args:
        df_raw: Aeropuertos de Bronze.

    Returns:
        Aeropuertos con un registro por iata_code.
    """
    df_clean = (
        _udf_clean_columns(df_raw.drop("_rescued_data"))
        .withColumnsRenamed(AIRPORT_COLUMNS)
        .withColumn("iata_code", F.upper("iata_code"))
    )
    # " bog" y "BOG" quedan iguales tras limpiar: la dedup deja uno
    return _udf_drop_duplicates_by_key(df_clean, ["iata_code"], LATEST_FIRST)


process_silver(
    "airports", transform_airports, AIRPORT_RULES,
    keys=["iata_code"], critical_columns=["airport_name", "country"],
    comment="Aeropuertos limpios. Un registro por iata_code.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vuelos
# MAGIC
# MAGIC - Fechas en **dos formatos** en la misma columna: `parse_timestamp` prueba ambos.
# MAGIC - Registros **duplicados**: deduplicación por `flight_id`.
# MAGIC - **Integridad referencial:** origen y destino deben existir en Silver aeropuertos. Se marcan con columnas `_tmp_` que la regla evalúa y luego se eliminan.

# COMMAND ----------

# DBTITLE 1,Vuelos: reglas y transformación
FLIGHT_COLUMNS = {
    "vuelo_id": "flight_id",
    "numero_vuelo": "flight_number",
    "origen": "origin",
    "destino": "destination",
    "salida_programada": "scheduled_departure_ts",
    "salida_real": "actual_departure_ts",
    "estado": "status",
}
FLIGHT_RULES = [
    {"name": "flight_id_not_null", "expression": "flight_id IS NOT NULL"},
    {
        "name": "origin_not_destination",
        "expression": "origin <> destination",
    },
    {
        "name": "scheduled_departure_valid",
        "expression": "scheduled_departure_ts IS NOT NULL",
    },
    {
        "name": "status_valid",
        "expression": "status IN ('OPERADO', 'CANCELADO')",
    },
    {
        "name": "operated_has_actual_departure",
        "expression": "status = 'CANCELADO' "
        "OR actual_departure_ts IS NOT NULL",
    },
    {
        "name": "airports_exist",
        "expression": "_tmp_origin_exists AND _tmp_destination_exists",
    },
    # Challenge (opción A): agregar aquí la nueva regla
]


def transform_flights(df_raw: DataFrame) -> DataFrame:
    """
    Conforma, tipa, deduplica y marca la integridad de los vuelos.

    Args:
        df_raw: Vuelos de Bronze.

    Returns:
        Vuelos con un registro por flight_id y columnas _tmp_ de
        integridad referencial.
    """
    df_clean = (
        _udf_clean_columns(df_raw.drop("_rescued_data"))
        .withColumnsRenamed(FLIGHT_COLUMNS)
        .withColumns(
            {
                "origin": F.upper("origin"),
                "destination": F.upper("destination"),
                "status": F.upper("status"),
                "scheduled_departure_ts": parse_timestamp(
                    "scheduled_departure_ts", DATE_FORMATS
                ),
                "actual_departure_ts": parse_timestamp(
                    "actual_departure_ts", DATE_FORMATS
                ),
            }
        )
    )
    df_dedup = _udf_drop_duplicates_by_key(
        df_clean, ["flight_id"], LATEST_FIRST
    )
    df_codes = spark.table(get_table_name(SCHEMA_SILVER, "airports")).select(
        "iata_code"
    )
    df_origins = df_codes.select(
        F.col("iata_code").alias("origin"),
        F.lit(True).alias("_tmp_origin_exists"),
    )
    df_destinations = df_codes.select(
        F.col("iata_code").alias("destination"),
        F.lit(True).alias("_tmp_destination_exists"),
    )
    return (
        df_dedup
        .join(F.broadcast(df_origins), on="origin", how="left")
        .join(F.broadcast(df_destinations), on="destination", how="left")
        .select(
            *df_dedup.columns,
            "_tmp_origin_exists",
            "_tmp_destination_exists",
        )
    )


process_silver(
    "flights", transform_flights, FLIGHT_RULES,
    keys=["flight_id"],
    critical_columns=["origin", "destination", "scheduled_departure_ts"],
    comment="Vuelos limpios y validados. Un registro por flight_id.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reservas
# MAGIC
# MAGIC Una reserva es válida solo si su vuelo está en **Silver vuelos**. Si el vuelo fue a quarantine, la reserva también: no se puede confiar en ella hasta corregir el vuelo.

# COMMAND ----------

# DBTITLE 1,Reservas: reglas y transformación
BOOKING_COLUMNS = {
    "reserva_id": "booking_id",
    "vuelo_id": "flight_id",
    "pasajero_id": "passenger_id",
    "fecha_reserva": "booking_date",
    "tarifa": "fare",
    "moneda": "currency",
}
BOOKING_RULES = [
    {"name": "booking_id_not_null", "expression": "booking_id IS NOT NULL"},
    {"name": "fare_valid", "expression": "fare IS NOT NULL AND fare >= 0"},
    {"name": "booking_date_valid", "expression": "booking_date IS NOT NULL"},
    {"name": "flight_exists", "expression": "_tmp_flight_exists"},
]


def transform_bookings(df_raw: DataFrame) -> DataFrame:
    """
    Conforma, tipa, deduplica y marca la integridad de las reservas.

    Args:
        df_raw: Reservas de Bronze.

    Returns:
        Reservas con un registro por booking_id y la columna
        _tmp_flight_exists.
    """
    df_clean = (
        _udf_clean_columns(df_raw.drop("_rescued_data"))
        .withColumnsRenamed(BOOKING_COLUMNS)
        .withColumns(
            {
                "booking_date": F.to_date(
                    parse_timestamp("booking_date", ["yyyy-MM-dd"])
                ),
                "fare": F.expr("try_cast(fare AS DECIMAL(14,2))"),
                "currency": F.upper("currency"),
            }
        )
    )
    df_dedup = _udf_drop_duplicates_by_key(
        df_clean, ["booking_id"], LATEST_FIRST
    )
    df_flights = spark.table(get_table_name(SCHEMA_SILVER, "flights")).select(
        "flight_id", F.lit(True).alias("_tmp_flight_exists")
    )
    return df_dedup.join(
        F.broadcast(df_flights), on="flight_id", how="left"
    ).select(*df_dedup.columns, "_tmp_flight_exists")


process_silver(
    "bookings", transform_bookings, BOOKING_RULES,
    keys=["booking_id"], critical_columns=["flight_id", "fare"],
    comment="Reservas limpias con vuelo válido. Un registro por booking_id.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué quedó en quarantine

# COMMAND ----------

# DBTITLE 1,Resumen de calidad por entidad
for entity in pipeline_params["entities"]:
    quarantine_table = get_table_name(SCHEMA_SILVER, f"{entity}_quarantine")
    print(f"— {entity}")
    display(_udf_summarize_quality(spark.table(quarantine_table)))
