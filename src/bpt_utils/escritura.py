"""Escrituras idempotentes a tablas Delta."""

from pyspark.sql import DataFrame, SparkSession


def merge_por_llave(
    spark: SparkSession,
    df: DataFrame,
    tabla: str,
    llaves: list[str],
    *,
    borrar_faltantes: bool = False,
    comentario: str | None = None,
) -> None:
    """Upsert de `df` en `tabla` por `llaves`: actualiza lo existente e inserta lo nuevo.

    - Si la tabla no existe, la crea.
    - Reejecutar con la misma entrada deja la tabla igual (idempotente).
    - `df` no debe tener llaves duplicadas (el MERGE fallaría): deduplica antes.
    - `borrar_faltantes=True` elimina de la tabla las llaves que ya no vienen en `df`.
      Úsalo solo cuando `df` es la foto COMPLETA de la entidad (reproceso total),
      nunca con cargas incrementales.
    """
    if not spark.catalog.tableExists(tabla):
        df.write.saveAsTable(tabla)
        if comentario:
            spark.sql(f"COMMENT ON TABLE {tabla} IS '{comentario}'")
        return

    vista = "_fuente_merge_" + tabla.replace(".", "_")
    df.createOrReplaceTempView(vista)
    condicion = " AND ".join(f"t.{c} = s.{c}" for c in llaves)
    borrado = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if borrar_faltantes else ""
    spark.sql(
        f"""
        MERGE INTO {tabla} AS t
        USING {vista} AS s
        ON {condicion}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        {borrado}
        """
    )


def sobrescribir(spark: SparkSession, df: DataFrame, tabla: str, comentario: str | None = None) -> None:
    """Reemplaza el contenido de `tabla` (para tablas derivadas que se recalculan completas)."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    if comentario:
        spark.sql(f"COMMENT ON TABLE {tabla} IS '{comentario}'")
