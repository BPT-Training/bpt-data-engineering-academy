# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Paso 2: Silver
# MAGIC
# MAGIC **Contrato Silver:** una fila por entidad, limpia, tipada y **confiable**.
# MAGIC
# MAGIC Cada entidad sigue el mismo patrón:
# MAGIC
# MAGIC ```
# MAGIC Bronze ─► transformar (limpiar, tipar, deduplicar) ─► aplicar_expectativas ─┬─► válidos   ─► MERGE a Silver
# MAGIC                                                                             └─► inválidos ─► <tabla>_quarantine
# MAGIC ```
# MAGIC
# MAGIC Las **reglas de calidad se declaran como datos** (listas `REGLAS_*`), no como `if` sueltos.
# MAGIC
# MAGIC > Guía: [04-medallion.md §3](../../guia-buenas-practicas/04-medallion.md)

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

from collections.abc import Callable
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bpt_utils.auditoria import registrar_ejecucion
from bpt_utils.calidad import aplicar_expectativas, resumen_calidad
from bpt_utils.escritura import merge_por_llave, sobrescribir
from bpt_utils.limpieza import deduplicar, parsear_timestamp, recortar_textos

# En caso de duplicados, gana la carga más reciente; desempate por nombre de archivo
ORDEN_MAS_RECIENTE = [F.col("_ingest_ts").desc(), F.col("_source_file").desc()]
FORMATOS_FECHA_HORA = ["yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Función común de procesamiento
# MAGIC
# MAGIC Una sola función aplica el patrón a las tres entidades: transformar → validar → escribir → registrar en el log.

# COMMAND ----------

def procesar_silver(
    entidad: str,
    transformar: Callable[[DataFrame], DataFrame],
    reglas: list[dict],
    llaves: list[str],
    comentario: str,
) -> None:
    tabla_origen = tabla(ESQUEMA_BRONZE, entidad)
    tabla_destino = tabla(ESQUEMA_SILVER, entidad)
    inicio = datetime.now()
    try:
        df_bronze = spark.table(tabla_origen)
        df_limpio = transformar(df_bronze)

        validos, invalidos = aplicar_expectativas(df_limpio, reglas)
        columnas_auxiliares = [c for c in df_limpio.columns if c.startswith("_chk_")]
        validos, invalidos = validos.drop(*columnas_auxiliares), invalidos.drop(*columnas_auxiliares)

        # Procesamos Bronze completo → Silver es la foto total: lo que dejó de ser válido sale
        merge_por_llave(spark, validos, tabla_destino, llaves, borrar_faltantes=True, comentario=comentario)
        sobrescribir(spark, invalidos, f"{tabla_destino}_quarantine")

        n_leidas = df_bronze.count()
        n_escritas = spark.table(tabla_destino).count()
        n_rechazadas = spark.table(f"{tabla_destino}_quarantine").count()
        registrar_ejecucion(
            spark, TABLA_LOG, pipeline="vuelos", capa="silver", tabla_destino=tabla_destino, inicio=inicio,
            estado="OK", filas_leidas=n_leidas, filas_escritas=n_escritas, filas_rechazadas=n_rechazadas,
        )
        print(f"✅ {entidad}: leídas {n_leidas} · en Silver {n_escritas} · quarantine {n_rechazadas}")
    except Exception as error:
        registrar_ejecucion(
            spark, TABLA_LOG, pipeline="vuelos", capa="silver", tabla_destino=tabla_destino,
            inicio=inicio, estado="ERROR", mensaje=str(error)[:1000],
        )
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aeropuertos

# COMMAND ----------

REGLAS_AEROPUERTOS = [
    {"nombre": "codigo_iata_valido", "expresion": "codigo_iata RLIKE '^[A-Z]{3}$'"},
    {"nombre": "nombre_no_nulo", "expresion": "nombre IS NOT NULL"},
]


def transformar_aeropuertos(df: DataFrame) -> DataFrame:
    df_limpio = recortar_textos(df.drop("_rescued_data")).withColumn("codigo_iata", F.upper("codigo_iata"))
    # " bog" y "BOG" quedan iguales tras limpiar → la dedup deja uno
    return deduplicar(df_limpio, ["codigo_iata"], ORDEN_MAS_RECIENTE)


procesar_silver(
    "aeropuertos", transformar_aeropuertos, REGLAS_AEROPUERTOS, ["codigo_iata"],
    "Aeropuertos limpios. Un registro por código IATA.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vuelos
# MAGIC
# MAGIC - Fechas en **dos formatos** → `parsear_timestamp` prueba ambos.
# MAGIC - Registros **duplicados** → dedup por `vuelo_id`.
# MAGIC - Integridad referencial: origen y destino deben existir en Silver aeropuertos. Se marca con columnas auxiliares `_chk_*` que la regla evalúa y luego se descartan.

# COMMAND ----------

REGLAS_VUELOS = [
    {"nombre": "vuelo_id_no_nulo", "expresion": "vuelo_id IS NOT NULL"},
    {"nombre": "origen_distinto_destino", "expresion": "origen <> destino"},
    {"nombre": "salida_programada_valida", "expresion": "salida_programada IS NOT NULL"},
    {"nombre": "estado_valido", "expresion": "estado IN ('OPERADO', 'CANCELADO')"},
    {"nombre": "operado_con_salida_real", "expresion": "estado = 'CANCELADO' OR salida_real IS NOT NULL"},
    {"nombre": "aeropuertos_existentes", "expresion": "_chk_origen AND _chk_destino"},
    # Challenge (opción A): agregar aquí la nueva regla
]


def transformar_vuelos(df: DataFrame) -> DataFrame:
    df_limpio = recortar_textos(df.drop("_rescued_data")).withColumns(
        {
            "origen": F.upper("origen"),
            "destino": F.upper("destino"),
            "estado": F.upper("estado"),
            "salida_programada": parsear_timestamp("salida_programada", FORMATOS_FECHA_HORA),
            "salida_real": parsear_timestamp("salida_real", FORMATOS_FECHA_HORA),
        }
    )
    df_unico = deduplicar(df_limpio, ["vuelo_id"], ORDEN_MAS_RECIENTE)

    codigos = spark.table(tabla(ESQUEMA_SILVER, "aeropuertos")).select("codigo_iata")
    return (
        df_unico
        .join(F.broadcast(codigos.select(F.col("codigo_iata").alias("origen"), F.lit(True).alias("_chk_origen"))),
              on="origen", how="left")
        .join(F.broadcast(codigos.select(F.col("codigo_iata").alias("destino"), F.lit(True).alias("_chk_destino"))),
              on="destino", how="left")
        .select(*df_unico.columns, "_chk_origen", "_chk_destino")
    )


procesar_silver(
    "vuelos", transformar_vuelos, REGLAS_VUELOS, ["vuelo_id"],
    "Vuelos limpios y validados. Un registro por vuelo_id.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reservas
# MAGIC
# MAGIC Una reserva es válida solo si su vuelo está en **Silver vuelos**. Si el vuelo fue a quarantine, la reserva también: no se puede confiar en ella hasta corregir el vuelo.

# COMMAND ----------

REGLAS_RESERVAS = [
    {"nombre": "reserva_id_no_nulo", "expresion": "reserva_id IS NOT NULL"},
    {"nombre": "tarifa_valida", "expresion": "tarifa IS NOT NULL AND tarifa >= 0"},
    {"nombre": "fecha_reserva_valida", "expresion": "fecha_reserva IS NOT NULL"},
    {"nombre": "vuelo_existente", "expresion": "_chk_vuelo"},
]


def transformar_reservas(df: DataFrame) -> DataFrame:
    df_limpio = recortar_textos(df.drop("_rescued_data")).withColumns(
        {
            "fecha_reserva": F.to_date(parsear_timestamp("fecha_reserva", ["yyyy-MM-dd"])),
            "tarifa": F.expr("try_cast(tarifa AS DECIMAL(14,2))"),
            "moneda": F.upper("moneda"),
        }
    )
    df_unico = deduplicar(df_limpio, ["reserva_id"], ORDEN_MAS_RECIENTE)

    vuelos = spark.table(tabla(ESQUEMA_SILVER, "vuelos")).select("vuelo_id", F.lit(True).alias("_chk_vuelo"))
    return (
        df_unico.join(F.broadcast(vuelos), on="vuelo_id", how="left")
        .select(*df_unico.columns, "_chk_vuelo")
    )


procesar_silver(
    "reservas", transformar_reservas, REGLAS_RESERVAS, ["reserva_id"],
    "Reservas limpias con vuelo válido. Un registro por reserva_id.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué quedó en quarantine?

# COMMAND ----------

for entidad in ["aeropuertos", "vuelos", "reservas"]:
    print(f"— {entidad}")
    display(resumen_calidad(spark.table(tabla(ESQUEMA_SILVER, f"{entidad}_quarantine"))))

# COMMAND ----------

display(
    spark.table(tabla(ESQUEMA_SILVER, "vuelos_quarantine"))
    .select("vuelo_id", "origen", "destino", "salida_programada", "salida_real", "estado", "_reglas_fallidas")
)
