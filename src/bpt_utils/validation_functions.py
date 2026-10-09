"""
Validaciones mínimas de datos para tablas resultado de un pipeline.

Implementa el primer nivel de pruebas orientadas a datos: esquema
esperado, tabla no vacía, unicidad de la llave de negocio y nulos en
columnas críticas. Cada función lanza AssertionError para que el job
falle cuando una validación no se cumple.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_schema(
    df: DataFrame, expected_columns: set[str], table_name: str
) -> None:
    """
    Valida que el DataFrame contenga las columnas esperadas.

    Args:
        df: DataFrame a validar.
        expected_columns: Columnas mínimas que deben existir.
        table_name: Nombre de la tabla, para el mensaje de error.

    Raises:
        AssertionError: Si falta alguna columna.
    """
    missing = expected_columns - set(df.columns)
    assert not missing, f"{table_name}: columnas faltantes {missing}"


def validate_not_empty(df: DataFrame, table_name: str) -> int:
    """
    Valida que el DataFrame tenga al menos una fila.

    Args:
        df: DataFrame a validar.
        table_name: Nombre de la tabla, para el mensaje de error.

    Returns:
        Número de filas del DataFrame.

    Raises:
        AssertionError: Si el DataFrame está vacío.
    """
    row_count = df.count()
    assert row_count > 0, (
        f"{table_name}: DataFrame vacío. Revisar filtros y fuente."
    )
    return row_count


def validate_unique_key(
    df: DataFrame, keys: list[str], table_name: str
) -> None:
    """
    Valida que la llave de negocio no tenga duplicados.

    Args:
        df: DataFrame a validar.
        keys: Columnas que forman la llave de negocio.
        table_name: Nombre de la tabla, para el mensaje de error.

    Raises:
        AssertionError: Si hay llaves duplicadas.
    """
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    assert total == distinct, (
        f"{table_name}: llave {keys} duplicada "
        f"({total} filas, {distinct} distintas)"
    )


def validate_no_nulls(
    df: DataFrame, critical_columns: list[str], table_name: str
) -> None:
    """
    Valida que las columnas críticas no tengan nulos.

    Args:
        df: DataFrame a validar.
        critical_columns: Columnas que no admiten nulos.
        table_name: Nombre de la tabla, para el mensaje de error.

    Raises:
        AssertionError: Si alguna columna crítica tiene nulos.
    """
    null_counts = df.select(
        *[
            F.sum(F.col(name).isNull().cast("int")).alias(name)
            for name in critical_columns
        ]
    ).first()
    columns_with_nulls = {
        name: null_counts[name]
        for name in critical_columns
        if null_counts[name]
    }
    assert not columns_with_nulls, (
        f"{table_name}: nulos en columnas críticas {columns_with_nulls}"
    )
