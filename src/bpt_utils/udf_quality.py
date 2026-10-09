"""
Reglas de calidad declarativas con separación en válidos y quarantine.

Las reglas se definen como datos (lista de diccionarios con nombre y
expresión SQL) y se aplican con una sola función, en lugar de
condiciones dispersas por el notebook.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

QualityRule = dict[str, str]


def _udf_apply_expectations(
    df: DataFrame, rules: list[QualityRule]
) -> tuple[DataFrame, DataFrame]:
    """
    Evalúa reglas de calidad y separa los registros válidos e inválidos.

    Una regla cuyo resultado es null (por ejemplo, fare >= 0 con fare
    nulo) cuenta como incumplida.

    Args:
        df: DataFrame a validar.
        rules: Reglas con las llaves "name" y "expression" (expresión
            SQL booleana).

    Returns:
        Tupla (df_valid, df_invalid). df_invalid agrega las columnas
        _failed_rules (lista de reglas incumplidas) y _quarantine_ts.

    Raises:
        ValueError: Si la lista de reglas está vacía.
    """
    if not rules:
        raise ValueError("Se requiere al menos una regla de calidad")

    failed = F.array(
        *[
            F.when(
                ~F.coalesce(F.expr(rule["expression"]), F.lit(False)),
                F.lit(rule["name"]),
            )
            for rule in rules
        ]
    )
    df_evaluated = df.withColumn(
        "_failed_rules", F.filter(failed, lambda item: item.isNotNull())
    )
    df_valid = df_evaluated.filter(F.size("_failed_rules") == 0).drop(
        "_failed_rules"
    )
    df_invalid = df_evaluated.filter(F.size("_failed_rules") > 0).withColumn(
        "_quarantine_ts", F.current_timestamp()
    )
    return df_valid, df_invalid


def _udf_summarize_quality(df_invalid: DataFrame) -> DataFrame:
    """
    Cuenta cuántos registros incumplen cada regla.

    Args:
        df_invalid: Registros inválidos generados por
            _udf_apply_expectations.

    Returns:
        DataFrame con las columnas rule y records, de mayor a menor.
    """
    return (
        df_invalid.select(F.explode("_failed_rules").alias("rule"))
        .groupBy("rule")
        .agg(F.count("*").alias("records"))
        .orderBy(F.col("records").desc())
    )
