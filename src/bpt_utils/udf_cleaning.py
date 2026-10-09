"""
Funciones de limpieza y estandarización de DataFrames.

Contiene transformaciones DataFrame -> DataFrame (prefijo _udf_) y
helpers de columnas usados en la capa Silver: nombres de columna en
snake_case, recorte de textos, deduplicación determinística y
conversión de fechas con varios formatos.
"""

import re
import unicodedata

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W


def to_snake_case(name: str) -> str:
    """
    Convierte un nombre de columna a snake_case sin tildes.

    Args:
        name: Nombre original, por ejemplo "Código IATA".

    Returns:
        Nombre en snake_case, por ejemplo "codigo_iata".
    """
    ascii_name = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    split_camel = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", ascii_name.strip())
    return re.sub(r"[^0-9a-zA-Z]+", "_", split_camel).strip("_").lower()


def _udf_normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Renombra las columnas a snake_case.

    Las columnas técnicas (prefijo "_") se conservan sin cambios porque
    las genera la plataforma, por ejemplo _rescued_data.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame con los nombres de columna normalizados.
    """
    new_names = [
        col_name if col_name.startswith("_") else to_snake_case(col_name)
        for col_name in df.columns
    ]
    return df.toDF(*new_names)


def _udf_clean_columns(
    df: DataFrame, columns: list[str] | None = None
) -> DataFrame:
    """
    Recorta espacios y convierte los textos vacíos en null.

    Args:
        df: DataFrame de entrada.
        columns: Columnas a limpiar. Si es None, se limpian todas las
            columnas de tipo string.

    Returns:
        DataFrame con las columnas limpias.
    """
    if columns is None:
        columns = [name for name, dtype in df.dtypes if dtype == "string"]
    return df.withColumns(
        {
            name: F.when(F.trim(F.col(name)) == "", None).otherwise(
                F.trim(F.col(name))
            )
            for name in columns
        }
    )


def _udf_drop_duplicates_by_key(
    df: DataFrame, keys: list[str], order_by: list[Column]
) -> DataFrame:
    """
    Deja un registro por llave: el primero según order_by.

    A diferencia de dropDuplicates, el resultado es determinístico
    siempre que order_by incluya un criterio de desempate.

    Args:
        df: DataFrame de entrada.
        keys: Columnas que forman la llave de negocio.
        order_by: Orden de prioridad, por ejemplo
            [F.col("_ingest_ts").desc(), F.col("_source_file").desc()].

    Returns:
        DataFrame con un registro por llave y las mismas columnas.
    """
    window = W.partitionBy(*keys).orderBy(*order_by)
    return (
        df.withColumn("_tmp_row_number", F.row_number().over(window))
        .filter(F.col("_tmp_row_number") == 1)
        .drop("_tmp_row_number")
    )


def parse_timestamp(column: str, formats: list[str]) -> Column:
    """
    Construye una expresión que prueba varios formatos de fecha.

    Args:
        column: Nombre de la columna de texto.
        formats: Formatos a probar en orden, por ejemplo
            ["yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm"].

    Returns:
        Columna timestamp con el primer formato válido, o null si
        ninguno aplica.
    """
    return F.coalesce(
        *[F.try_to_timestamp(F.col(column), F.lit(fmt)) for fmt in formats]
    )
