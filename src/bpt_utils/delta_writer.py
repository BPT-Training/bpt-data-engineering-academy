"""
Escrituras idempotentes a tablas Delta.

Reejecutar un proceso con la misma entrada debe dejar la tabla igual:
estas funciones encapsulan MERGE por llave y sobrescritura completa.
"""

from pyspark.sql import DataFrame, SparkSession


def build_merge_condition(keys: list[str]) -> str:
    """
    Construye la condición de cruce de un MERGE.

    Args:
        keys: Columnas que forman la llave del MERGE.

    Returns:
        Condición SQL, por ejemplo "t.id = s.id AND t.date = s.date".
    """
    return " AND ".join(f"t.{key} = s.{key}" for key in keys)


def merge_by_key(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    keys: list[str],
    *,
    should_delete_missing: bool = False,
    comment: str | None = None,
) -> None:
    """
    Hace upsert de df en target_table por las llaves indicadas.

    Si la tabla no existe, la crea. df no debe tener llaves duplicadas
    porque el MERGE fallaría: deduplicar antes.

    Args:
        spark: Sesión de Spark activa.
        df: Datos de entrada, sin llaves duplicadas.
        target_table: Nombre completo catalogo.esquema.tabla.
        keys: Columnas que forman la llave.
        should_delete_missing: Si es True, elimina de la tabla las
            llaves que no vienen en df. Usar solo cuando df es la foto
            completa de la entidad, nunca con cargas incrementales.
        comment: Comentario de tabla, aplicado al crearla.
    """
    if not spark.catalog.tableExists(target_table):
        df.write.saveAsTable(target_table)
        if comment:
            spark.sql(f"COMMENT ON TABLE {target_table} IS '{comment}'")
        return

    source_view = "_tmp_merge_source_" + target_table.replace(".", "_")
    df.createOrReplaceTempView(source_view)
    delete_clause = (
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        if should_delete_missing
        else ""
    )
    spark.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING {source_view} AS s
        ON {build_merge_condition(keys)}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        {delete_clause}
        """
    )


def overwrite_table(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    comment: str | None = None,
) -> None:
    """
    Reemplaza el contenido completo de una tabla.

    Pensado para tablas derivadas que se recalculan completas (Gold,
    quarantine).

    Args:
        spark: Sesión de Spark activa.
        df: Datos que reemplazan el contenido de la tabla.
        target_table: Nombre completo catalogo.esquema.tabla.
        comment: Comentario de tabla (opcional).
    """
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    if comment:
        spark.sql(f"COMMENT ON TABLE {target_table} IS '{comment}'")
