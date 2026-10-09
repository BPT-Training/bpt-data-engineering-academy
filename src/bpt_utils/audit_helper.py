"""
Columnas técnicas de ingesta y log de ejecución de pipelines.

Permite responder sin abrir el código: qué corrió, cuándo, cuántas filas
procesó cada paso y si terminó con error.
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOG_SCHEMA = (
    "pipeline string, layer string, target_table string, "
    "rows_read long, rows_written long, rows_rejected long, "
    "started_at timestamp, finished_at timestamp, "
    "status string, message string"
)
VALID_STATUSES = {"OK", "ERROR"}


def _udf_add_ingestion_columns(df: DataFrame) -> DataFrame:
    """
    Agrega las columnas técnicas _ingest_ts y _source_file.

    Args:
        df: DataFrame leído desde archivos (requiere la columna oculta
            _metadata de la fuente).

    Returns:
        DataFrame con las dos columnas técnicas agregadas.
    """
    return df.withColumns(
        {
            "_ingest_ts": F.current_timestamp(),
            "_source_file": F.col("_metadata.file_path"),
        }
    )


def create_log_table(spark: SparkSession, log_table: str) -> None:
    """
    Crea la tabla de log de ejecuciones si no existe.

    Args:
        spark: Sesión de Spark activa.
        log_table: Nombre completo catalogo.esquema.tabla.
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {log_table} ({LOG_SCHEMA}) "
        "COMMENT 'Un registro por ejecución de cada paso del pipeline'"
    )


def log_execution(
    spark: SparkSession,
    log_table: str,
    *,
    pipeline: str,
    layer: str,
    target_table: str,
    started_at: datetime,
    status: str,
    rows_read: int | None = None,
    rows_written: int | None = None,
    rows_rejected: int | None = None,
    message: str | None = None,
) -> None:
    """
    Agrega una fila al log de ejecuciones.

    Args:
        spark: Sesión de Spark activa.
        log_table: Nombre completo de la tabla de log.
        pipeline: Nombre del pipeline.
        layer: Capa procesada (bronze, silver, gold).
        target_table: Tabla destino del paso.
        started_at: Momento de inicio del paso.
        status: "OK" o "ERROR".
        rows_read: Filas leídas (opcional).
        rows_written: Filas escritas (opcional).
        rows_rejected: Filas enviadas a quarantine (opcional).
        message: Detalle del error, si lo hubo (opcional).

    Raises:
        ValueError: Si status no es "OK" ni "ERROR".
    """
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Estado inválido: {status!r}. Valores permitidos: "
            f"{sorted(VALID_STATUSES)}"
        )
    row = [
        (
            pipeline,
            layer,
            target_table,
            rows_read,
            rows_written,
            rows_rejected,
            started_at,
            datetime.now(),
            status,
            message,
        )
    ]
    (
        spark.createDataFrame(row, LOG_SCHEMA)
        .write.mode("append")
        .saveAsTable(log_table)
    )
