# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Paso 3: Gold
# MAGIC
# MAGIC **Contrato Gold:** modelos listos para consumo. **Sin lógica de limpieza**, que ya se hizo en Silver.
# MAGIC
# MAGIC | Tabla | Tipo | Granularidad |
# MAGIC |---|---|---|
# MAGIC | `dim_aeropuerto` | Dimensión | Un registro por aeropuerto |
# MAGIC | `fct_vuelo` | Hechos | Un registro por vuelo |
# MAGIC | `agg_puntualidad_ruta` | Agregado / KPI | Un registro por ruta (origen → destino) |
# MAGIC
# MAGIC Las tablas Gold se **recalculan completas** desde Silver (`overwrite`), así que el proceso es idempotente.
# MAGIC
# MAGIC > Guía: [04-medallion.md §4](../../guia-buenas-practicas/04-medallion.md)

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bpt_utils.auditoria import registrar_ejecucion
from bpt_utils.escritura import sobrescribir

UMBRAL_PUNTUALIDAD_MIN = 15   # un vuelo es puntual si sale máximo 15 min tarde

silver_aeropuertos = spark.table(tabla(ESQUEMA_SILVER, "aeropuertos"))
silver_vuelos = spark.table(tabla(ESQUEMA_SILVER, "vuelos"))
silver_reservas = spark.table(tabla(ESQUEMA_SILVER, "reservas"))

# COMMAND ----------

def publicar_gold(nombre: str, df: DataFrame, comentario: str) -> None:
    """Escribe una tabla Gold completa y registra la ejecución."""
    tabla_destino = tabla(ESQUEMA_GOLD, nombre)
    inicio = datetime.now()
    try:
        sobrescribir(spark, df, tabla_destino, comentario)
        n_filas = spark.table(tabla_destino).count()
        registrar_ejecucion(
            spark, TABLA_LOG, pipeline="vuelos", capa="gold", tabla_destino=tabla_destino,
            inicio=inicio, estado="OK", filas_escritas=n_filas,
        )
        print(f"✅ {nombre}: {n_filas} filas")
    except Exception as error:
        registrar_ejecucion(
            spark, TABLA_LOG, pipeline="vuelos", capa="gold", tabla_destino=tabla_destino,
            inicio=inicio, estado="ERROR", mensaje=str(error)[:1000],
        )
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_aeropuerto`

# COMMAND ----------

dim_aeropuerto = silver_aeropuertos.select(
    "codigo_iata", "nombre", "ciudad", "pais", (F.col("pais") == "Colombia").alias("es_nacional")
)
publicar_gold("dim_aeropuerto", dim_aeropuerto, "Dimensión de aeropuertos. Un registro por código IATA.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fct_vuelo`
# MAGIC
# MAGIC - `minutos_retraso` y `a_tiempo` solo aplican a vuelos **operados**; en cancelados quedan en null.
# MAGIC - `tipo_ruta` es nacional si origen y destino son de Colombia.
# MAGIC - Se valida la **cardinalidad**: un registro por vuelo de Silver.

# COMMAND ----------

reservas_por_vuelo = silver_reservas.groupBy("vuelo_id").agg(
    F.count("*").alias("n_reservas"),
    F.sum("tarifa").alias("ingreso_total"),
)
nacional = dim_aeropuerto.select("codigo_iata", "es_nacional")

es_operado = F.col("estado") == "OPERADO"
minutos_retraso = ((F.unix_timestamp("salida_real") - F.unix_timestamp("salida_programada")) / 60).cast("int")

fct_vuelo = (
    silver_vuelos
    .join(reservas_por_vuelo, on="vuelo_id", how="left")
    .join(F.broadcast(nacional.withColumnsRenamed({"codigo_iata": "origen", "es_nacional": "_origen_nacional"})),
          on="origen", how="left")
    .join(F.broadcast(nacional.withColumnsRenamed({"codigo_iata": "destino", "es_nacional": "_destino_nacional"})),
          on="destino", how="left")
    .withColumns(
        {
            "fecha": F.to_date("salida_programada"),
            "tipo_ruta": F.when(F.col("_origen_nacional") & F.col("_destino_nacional"), "nacional")
                          .otherwise("internacional"),
            "minutos_retraso": F.when(es_operado, minutos_retraso),
            "n_reservas": F.coalesce("n_reservas", F.lit(0)),
            "ingreso_total": F.coalesce("ingreso_total", F.lit(0).cast("decimal(24,2)")),
        }
    )
    .withColumn("a_tiempo", F.when(es_operado, F.col("minutos_retraso") <= UMBRAL_PUNTUALIDAD_MIN))
    .select(
        "vuelo_id", "numero_vuelo", "fecha", "origen", "destino", "tipo_ruta", "estado",
        "salida_programada", "salida_real", "minutos_retraso", "a_tiempo", "n_reservas", "ingreso_total",
    )
)

n_silver, n_fct = silver_vuelos.count(), fct_vuelo.count()
assert n_silver == n_fct, f"Cardinalidad rota: {n_silver} vuelos en Silver vs {n_fct} en fct_vuelo"

publicar_gold("fct_vuelo", fct_vuelo, "Hechos de vuelos. Un registro por vuelo (operado o cancelado).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `agg_puntualidad_ruta` · KPI
# MAGIC
# MAGIC **% de puntualidad** = vuelos operados a tiempo / vuelos operados × 100. Se define **una sola vez**, aquí.

# COMMAND ----------

agg_puntualidad_ruta = (
    spark.table(tabla(ESQUEMA_GOLD, "fct_vuelo"))
    .filter(F.col("estado") == "OPERADO")
    .groupBy("origen", "destino", "tipo_ruta")
    .agg(
        F.count("*").alias("vuelos_operados"),
        F.sum(F.col("a_tiempo").cast("int")).alias("vuelos_a_tiempo"),
        F.round(F.avg("minutos_retraso"), 1).alias("retraso_promedio_min"),
    )
    .withColumn("pct_puntualidad", F.round(F.col("vuelos_a_tiempo") / F.col("vuelos_operados") * 100, 1))
)
publicar_gold(
    "agg_puntualidad_ruta", agg_puntualidad_ruta,
    "KPI de puntualidad por ruta. Un registro por origen-destino. Puntual = retraso <= 15 min.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consumo: rutas con peor puntualidad

# COMMAND ----------

display(
    spark.table(tabla(ESQUEMA_GOLD, "agg_puntualidad_ruta"))
    .filter(F.col("vuelos_operados") >= 2)
    .orderBy("pct_puntualidad", F.col("vuelos_operados").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold también se consume con SQL (dashboards, Genie, analistas)
# MAGIC SELECT tipo_ruta,
# MAGIC        COUNT(*)                                             AS vuelos,
# MAGIC        ROUND(AVG(CASE WHEN a_tiempo THEN 100 ELSE 0 END), 1) AS pct_puntualidad,
# MAGIC        SUM(ingreso_total)                                   AS ingreso_total
# MAGIC FROM workspace.bpt_academy_gold.fct_vuelo
# MAGIC WHERE estado = 'OPERADO'
# MAGIC GROUP BY tipo_ruta
