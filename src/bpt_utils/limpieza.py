"""Funciones de limpieza y estandarización de DataFrames."""

import re
import unicodedata

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def a_snake_case(nombre: str) -> str:
    """Convierte un nombre de columna a snake_case sin tildes: 'Código IATA' -> 'codigo_iata'."""
    sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode("ascii")
    con_guiones = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", sin_tildes.strip())
    return re.sub(r"[^0-9a-zA-Z]+", "_", con_guiones).strip("_").lower()


def normalizar_columnas(df: DataFrame) -> DataFrame:
    """Renombra las columnas a snake_case. Las técnicas (prefijo `_`) se dejan igual."""
    return df.toDF(*[c if c.startswith("_") else a_snake_case(c) for c in df.columns])


def recortar_textos(df: DataFrame, columnas: list[str] | None = None) -> DataFrame:
    """Aplica trim a columnas de texto y convierte los vacíos en null.

    Si no se indican columnas, se aplica a todas las de tipo string.
    """
    if columnas is None:
        columnas = [c for c, tipo in df.dtypes if tipo == "string"]
    return df.withColumns(
        {c: F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))) for c in columnas}
    )


def deduplicar(df: DataFrame, llaves: list[str], orden: list[Column]) -> DataFrame:
    """Deja un registro por llave: el primero según `orden`.

    `orden` debe ser determinístico (incluir un criterio de desempate), por ejemplo
    `[F.col("_ingest_ts").desc(), F.col("_source_file").desc()]`.
    """
    ventana = Window.partitionBy(*llaves).orderBy(*orden)
    return df.withColumn("_rn", F.row_number().over(ventana)).filter(F.col("_rn") == 1).drop("_rn")


def parsear_timestamp(columna: str, formatos: list[str]) -> Column:
    """Intenta cada formato en orden y devuelve el primer timestamp válido (o null)."""
    return F.coalesce(*[F.try_to_timestamp(F.col(columna), F.lit(fmt)) for fmt in formatos])
