"""Columnas técnicas de ingesta y log de ejecución de pipelines."""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SCHEMA_LOG = (
    "pipeline string, capa string, tabla_destino string, "
    "filas_leidas long, filas_escritas long, filas_rechazadas long, "
    "inicio timestamp, fin timestamp, estado string, mensaje string"
)


def agregar_columnas_ingesta(df: DataFrame) -> DataFrame:
    """Agrega `_ingest_ts` y `_source_file`. Requiere un DataFrame leído desde archivos."""
    return df.withColumns(
        {
            "_ingest_ts": F.current_timestamp(),
            "_source_file": F.col("_metadata.file_path"),
        }
    )


def crear_tabla_log(spark: SparkSession, tabla_log: str) -> None:
    """Crea la tabla de log de ejecuciones si no existe."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {tabla_log} ({SCHEMA_LOG}) "
        "COMMENT 'Un registro por ejecución de cada paso del pipeline'"
    )


def registrar_ejecucion(
    spark: SparkSession,
    tabla_log: str,
    *,
    pipeline: str,
    capa: str,
    tabla_destino: str,
    inicio: datetime,
    estado: str,
    filas_leidas: int | None = None,
    filas_escritas: int | None = None,
    filas_rechazadas: int | None = None,
    mensaje: str | None = None,
) -> None:
    """Agrega una fila al log de ejecuciones."""
    if estado not in {"OK", "ERROR"}:
        raise ValueError(f"Estado inválido: {estado!r}. Usar 'OK' o 'ERROR'.")
    fila = [
        (
            pipeline,
            capa,
            tabla_destino,
            filas_leidas,
            filas_escritas,
            filas_rechazadas,
            inicio,
            datetime.now(),
            estado,
            mensaje,
        )
    ]
    spark.createDataFrame(fila, SCHEMA_LOG).write.mode("append").saveAsTable(tabla_log)
