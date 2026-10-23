# Databricks notebook source
# MAGIC %md
# MAGIC # M4 · PySpark Advanced Patterns
# MAGIC
# MAGIC **Propósito:** aplicar los patrones que más aparecen en pipelines reales: ventanas, deduplicación, nulos y fechas.
# MAGIC
# MAGIC | Bloque | Minutos | Tema |
# MAGIC |---|---|---|
# MAGIC | 1 | 0–15 | Window functions: ranking, valor anterior y acumulados |
# MAGIC | 2 | 15–30 | Deduplicación determinística |
# MAGIC | 3 | 30–45 | Nulos: decisiones explícitas |
# MAGIC | 4 | 45–60 | Fechas y funciones nativas frente a UDF |
# MAGIC
# MAGIC **Datos:** `samples.tpch`. **Compute:** Serverless.
# MAGIC
# MAGIC **Ejercicios:** notebook `04_pyspark_advanced_lab` (en esta misma carpeta)
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/02-pyspark.md` §4 y §6 (en la raíz del repositorio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## El caso de la sesión: los problemas de todo pipeline real
# MAGIC
# MAGIC Los datos de producción casi nunca llegan limpios: los eventos de un vuelo llegan repetidos desde varios archivos, faltan valores y las fechas vienen en formatos distintos. En esta sesión se resuelve cada uno de esos problemas con el patrón correcto.
# MAGIC
# MAGIC | Etapa | Qué le pasa a los datos | Bloque | En un proyecto real |
# MAGIC |---|---|---|---|
# MAGIC | 1. Historial | Órdenes de 3 clientes: número de orden, valor anterior y gasto acumulado, sin perder filas | 1 | Historiales, rankings y acumulados por cliente o producto |
# MAGIC | 2. Deduplicar | `VL0001` llega 3 veces (programado, demorado, operado), dos con la misma hora: se conserva siempre el mismo registro | 2 | Quedarse con el último estado de cada entidad |
# MAGIC | 3. Nulos | Conteos y promedios que cambian según cómo se tratan los nulos | 3 | Decidir y documentar qué se hace con cada nulo |
# MAGIC | 4. Fechas y funciones | Salidas en texto con dos formatos → timestamp, fecha, semana y UTC; reemplazar una UDF por funciones nativas | 4 | Fechas de fuentes distintas y rendimiento del pipeline |
# MAGIC
# MAGIC **Ejemplo de un vuelo a lo largo de la sesión:**
# MAGIC
# MAGIC | Momento | `VL0001` |
# MAGIC |---|---|
# MAGIC | Llega | 3 eventos: `PROGRAMADO` 05:00 (`a.csv`), `DEMORADO` 06:00 (`b.csv`), `OPERADO` 06:00 (`c.csv`) |
# MAGIC | `dropDuplicates` | Cualquiera de los 3: el resultado puede cambiar entre ejecuciones |
# MAGIC | Ventana con desempate | Siempre `OPERADO`: la hora más reciente y, en el empate, el archivo `c.csv` |

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W

from bpt_utils.udf_cleaning import _udf_drop_duplicates_by_key

df_orders = spark.table("samples.tpch.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 1 · Window functions
# MAGIC
# MAGIC Una ventana calcula sobre un **grupo de filas relacionadas sin colapsarlas**. `groupBy`, en cambio, devuelve una fila por grupo.
# MAGIC
# MAGIC ```python
# MAGIC W.partitionBy("cliente").orderBy("fecha")
# MAGIC #            ↑ grupos           ↑ orden dentro del grupo
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Historial por cliente: posición, orden anterior y acumulado
by_customer = W.partitionBy("o_custkey").orderBy("o_orderdate", "o_orderkey")

df_customer_history = (
    df_orders
    .filter(F.col("o_custkey").isin(1, 2, 4))
    .select("o_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
    .withColumns(
        {
            "order_number": F.row_number().over(by_customer),
            "previous_price": F.lag("o_totalprice").over(by_customer),
            "cumulative_spend": F.sum("o_totalprice").over(
                by_customer.rowsBetween(W.unboundedPreceding, W.currentRow)
            ),
        }
    )
    .orderBy("o_custkey", "order_number")
)
display(df_customer_history)

# COMMAND ----------

# MAGIC %md
# MAGIC | Valores | `row_number` | `rank` | `dense_rank` |
# MAGIC |---|---|---|---|
# MAGIC | 100 | 1 | 1 | 1 |
# MAGIC | 100 | 2 | 1 | 1 |
# MAGIC | 90 | 3 | 3 | 2 |
# MAGIC
# MAGIC **Regla:** en acumulados, marco explícito con `rowsBetween`. El marco por defecto trata como iguales las filas empatadas en el orden.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 2 · Deduplicación determinística
# MAGIC
# MAGIC `dropDuplicates(["id"])` conserva **cualquier** fila del grupo. Si las filas repetidas tienen datos distintos, dos ejecuciones pueden dar resultados distintos.

# COMMAND ----------

# DBTITLE 1,Datos con duplicados y empate en la fecha
df_flight_events = spark.createDataFrame(
    [
        ("VL0001", "PROGRAMADO", datetime(2026, 10, 14, 5, 0), "a.csv"),
        ("VL0001", "DEMORADO", datetime(2026, 10, 14, 6, 0), "b.csv"),
        ("VL0001", "OPERADO", datetime(2026, 10, 14, 6, 0), "c.csv"),
        ("VL0002", "CANCELADO", datetime(2026, 10, 14, 7, 0), "a.csv"),
    ],
    "flight_id string, status string, updated_at timestamp, "
    "source_file string",
)

# COMMAND ----------

# DBTITLE 1,Antipatrón: dropDuplicates sin criterio
display(df_flight_events.dropDuplicates(["flight_id"]))

# COMMAND ----------

# DBTITLE 1,Patrón: ventana con orden y desempate
latest_first = W.partitionBy("flight_id").orderBy(
    F.col("updated_at").desc(), F.col("source_file").desc()
)
df_dedup = (
    df_flight_events
    .withColumn("_tmp_row_number", F.row_number().over(latest_first))
    .filter(F.col("_tmp_row_number") == 1)
    .drop("_tmp_row_number")
)
display(df_dedup)

# COMMAND ----------

# MAGIC %md
# MAGIC El mismo patrón está empaquetado en `bpt_utils` para no reescribirlo en cada notebook:

# COMMAND ----------

# DBTITLE 1,Patrón reutilizable desde bpt_utils
df_dedup = _udf_drop_duplicates_by_key(
    df_flight_events,
    keys=["flight_id"],
    order_by=[F.col("updated_at").desc(), F.col("source_file").desc()],
)
display(df_dedup)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 3 · Nulos
# MAGIC
# MAGIC Los nulos cambian resultados sin avisar. Para cada columna clave se decide y se documenta si el nulo se descarta, se imputa o va a quarantine.

# COMMAND ----------

# DBTITLE 1,count(*) frente a count(columna)
df_nulls = spark.createDataFrame(
    [("A", 10), ("B", None), (None, 5), (None, None)],
    "code string, value int",
)
display(
    df_nulls.agg(
        F.count("*").alias("rows"),
        F.count("value").alias("non_null_values"),
        F.avg("value").alias("avg_ignoring_nulls"),
        F.avg(F.coalesce("value", F.lit(0))).alias("avg_nulls_as_zero"),
    )
)

# COMMAND ----------

# DBTITLE 1,Antipatrón: comparar con None usando ==
print(df_nulls.filter(F.col("code") == F.lit(None)).count())  # siempre 0

# COMMAND ----------

# DBTITLE 1,Patrón: isNull o eqNullSafe
print(df_nulls.filter(F.col("code").isNull()).count())
print(df_nulls.filter(F.col("code").eqNullSafe(F.lit(None))).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 4 · Fechas y funciones nativas
# MAGIC
# MAGIC ## 4.1 Fechas en varios formatos

# COMMAND ----------

# DBTITLE 1,try_to_timestamp con varios formatos
DATE_FORMATS = ["yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm"]

df_raw_dates = spark.createDataFrame(
    [("2026-10-14 08:30",), ("14/10/2026 22:15",), ("sin dato",)],
    "departure_text string",
)
df_dates = df_raw_dates.withColumn(
    "departure_ts",
    F.coalesce(
        *[
            F.try_to_timestamp("departure_text", F.lit(fmt))
            for fmt in DATE_FORMATS
        ]
    ),
).withColumns(
    {
        "departure_date": F.to_date("departure_ts"),
        "departure_week": F.date_trunc("week", "departure_ts"),
        "departure_utc": F.to_utc_timestamp(
            "departure_ts", "America/Bogota"
        ),
    }
)
display(df_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC `try_to_timestamp` devuelve null cuando el formato no coincide, en lugar de fallar. **Regla:** guardar los timestamps en UTC y convertir a hora local solo en la capa de consumo.
# MAGIC
# MAGIC ## 4.2 Funciones nativas frente a UDF de Python
# MAGIC
# MAGIC Las funciones nativas (`F.*`) las optimiza el motor y corren distribuidas. Una UDF de Python procesa fila a fila y el optimizador no puede ver qué hace. **Solo se crea una UDF cuando no existe la función nativa.**

# COMMAND ----------

# DBTITLE 1,Antipatrón: UDF de Python para algo que ya existe


@F.udf(T.StringType())
def get_priority_level(priority: str) -> str:
    return priority.split("-")[1].lower() if priority else None


display(
    df_orders.select(get_priority_level("o_orderpriority").alias("level"))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Patrón: funciones nativas
display(
    df_orders.select(
        F.lower(F.split("o_orderpriority", "-").getItem(1)).alias("level")
    ).distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Resolver los ejercicios en el notebook `04_pyspark_advanced_lab`, que está en esta misma carpeta (panel izquierdo, junto a este notebook).
