"""Reglas de calidad declarativas con separación en válidos / quarantine."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Regla = dict[str, str]  # {"nombre": ..., "expresion": ...}


def aplicar_expectativas(df: DataFrame, reglas: list[Regla]) -> tuple[DataFrame, DataFrame]:
    """Evalúa cada regla (expresión SQL booleana) y separa los registros.

    Una regla cuyo resultado es null (por ejemplo, `tarifa >= 0` con tarifa nula) cuenta como incumplida.

    Returns:
        (validos, invalidos). `invalidos` trae dos columnas extra: `_reglas_fallidas`
        (array con los nombres de las reglas incumplidas) y `_quarantine_ts`.
    """
    if not reglas:
        raise ValueError("Se requiere al menos una regla de calidad")

    fallidas = F.array(
        *[F.when(~F.coalesce(F.expr(r["expresion"]), F.lit(False)), F.lit(r["nombre"])) for r in reglas]
    )
    df_eval = df.withColumn("_reglas_fallidas", F.filter(fallidas, lambda x: x.isNotNull()))

    validos = df_eval.filter(F.size("_reglas_fallidas") == 0).drop("_reglas_fallidas")
    invalidos = df_eval.filter(F.size("_reglas_fallidas") > 0).withColumn(
        "_quarantine_ts", F.current_timestamp()
    )
    return validos, invalidos


def resumen_calidad(df_invalidos: DataFrame) -> DataFrame:
    """Cuenta cuántos registros incumplen cada regla."""
    return (
        df_invalidos.select(F.explode("_reglas_fallidas").alias("regla"))
        .groupBy("regla")
        .agg(F.count("*").alias("registros"))
        .orderBy(F.col("registros").desc())
    )
