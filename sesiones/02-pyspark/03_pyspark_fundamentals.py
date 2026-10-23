# Databricks notebook source
# MAGIC %md
# MAGIC # M3 · PySpark Fundamentals
# MAGIC
# MAGIC **Propósito:** entender cómo trabaja Spark y dominar las operaciones base de un DataFrame, con el estándar de código de BPT.
# MAGIC
# MAGIC | Bloque | Minutos | Tema |
# MAGIC |---|---|---|
# MAGIC | 1 | 0–15 | Qué es Spark: driver, executors y ejecución diferida |
# MAGIC | 2 | 15–30 | DataFrames y schemas |
# MAGIC | 3 | 30–45 | Seleccionar, filtrar, crear columnas y agregar |
# MAGIC | 4 | 45–60 | Joins y validación de cardinalidad |
# MAGIC
# MAGIC **Datos:** `samples.tpch` (clientes, órdenes y naciones). **Compute:** Serverless.
# MAGIC
# MAGIC **Ejercicios:** notebook `03_pyspark_fundamentals_lab` (en esta misma carpeta)
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/02-pyspark.md` (en la raíz del repositorio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## El caso de la sesión: el informe mensual de ventas
# MAGIC
# MAGIC El área comercial pide dos cosas: **las ventas de cada mes desde 1998** y **la lista de clientes con su país**. Se resuelve sobre las tablas de `samples.tpch` (millones de órdenes), que ya no caben en pandas: es trabajo de PySpark.
# MAGIC
# MAGIC | Etapa | Qué le pasa a los datos | Bloque | En un proyecto real |
# MAGIC |---|---|---|---|
# MAGIC | 1. Leer sin calcular | `samples.tpch.orders` y un filtro por estado: Spark solo arma el plan hasta que se pide un resultado | 1 | Entender por qué un job tarda y dónde se ejecuta |
# MAGIC | 2. Conocer la estructura | Schema de la tabla y un schema explícito para datos de vuelos | 2 | Contrato de columnas y tipos al leer archivos de una fuente |
# MAGIC | 3. Transformar y resumir | Órdenes desde 1998 → columnas `order_year`, `order_month`, `is_urgent` → ventas, ticket promedio y urgentes por mes | 3 | La tabla agregada que alimenta el informe |
# MAGIC | 4. Cruzar | Clientes + naciones (dimensión pequeña con `broadcast`); clientes sin órdenes; validación de que el join no duplica filas | 4 | Enriquecer con dimensiones sin inflar los totales |
# MAGIC
# MAGIC **Ejemplo de una orden a lo largo de la sesión:**
# MAGIC
# MAGIC | Momento | Una orden de 1998 con prioridad `1-URGENT` |
# MAGIC |---|---|
# MAGIC | En la tabla | `o_orderdate`, `o_totalprice`, `o_orderpriority` |
# MAGIC | Transformada | `order_year = 1998`, `order_month`, `is_urgent = True` |
# MAGIC | Resumida | Suma en `total_sales` y en `urgent_count` de su mes |

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 1 · Qué es Spark
# MAGIC
# MAGIC ```
# MAGIC              ┌──────────┐
# MAGIC  código  ──► │  Driver  │  arma el plan y lo optimiza
# MAGIC              └────┬─────┘
# MAGIC        ┌──────────┼──────────┐
# MAGIC   ┌────▼───┐ ┌────▼───┐ ┌────▼───┐
# MAGIC   │Executor│ │Executor│ │Executor│  procesan partes de los datos en paralelo
# MAGIC   └────────┘ └────────┘ └────────┘
# MAGIC ```
# MAGIC
# MAGIC | Concepto | Qué significa |
# MAGIC |---|---|
# MAGIC | **Transformación** (`select`, `filter`, `join`, `groupBy`) | Agrega un paso al plan; **no ejecuta nada** |
# MAGIC | **Acción** (`count`, `display`, `collect`, `write`) | Ejecuta el plan completo |
# MAGIC | **Shuffle** | Mover datos entre executors (`groupBy`, `join`); es la operación más costosa |
# MAGIC
# MAGIC En compute serverless Databricks administra los executors, pero estos conceptos siguen definiendo el rendimiento.

# COMMAND ----------

# DBTITLE 1,Una transformación no ejecuta nada
df_orders = spark.table("samples.tpch.orders")
df_finished = df_orders.filter(F.col("o_orderstatus") == "F")
print(type(df_finished))  # solo un plan, todavía sin datos

# COMMAND ----------

# DBTITLE 1,Una acción ejecuta el plan
df_finished.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 2 · DataFrames y schemas
# MAGIC
# MAGIC Un DataFrame es una tabla distribuida con un **schema**: nombre y tipo de cada columna. En las tablas Delta el schema ya existe; al leer archivos (CSV, JSON) el estándar exige **declararlo explícitamente**.

# COMMAND ----------

# DBTITLE 1,Schema de una tabla
df_orders.printSchema()

# COMMAND ----------

# DBTITLE 1,Schema explícito
FLIGHT_SCHEMA = T.StructType(
    [
        T.StructField("flight_id", T.StringType(), nullable=False),
        T.StructField("origin", T.StringType()),
        T.StructField("destination", T.StringType()),
        T.StructField("delay_minutes", T.IntegerType()),
    ]
)

df_flights = spark.createDataFrame(
    [
        ("VL0001", "BOG", "MDE", 5),
        ("VL0002", "MDE", "CLO", 42),
        ("VL0003", "CLO", "CTG", None),
    ],
    schema=FLIGHT_SCHEMA,
)
df_flights.printSchema()

# Con un archivo: spark.read.csv(path, header=True, schema=FLIGHT_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC **Por qué no inferir el schema en producción:** `inferSchema` lee los datos dos veces y, si la fuente cambia, cambia los tipos sin avisar.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 3 · Seleccionar, filtrar, crear columnas y agregar

# COMMAND ----------

# DBTITLE 1,Antipatrón: un DataFrame por paso, nombres genéricos
df = spark.table("samples.tpch.orders")
df2 = df.filter(df.o_orderdate >= "1998-01-01")
df3 = df2.withColumn("year", F.year("o_orderdate"))
df4 = df3.withColumn("month", F.month("o_orderdate"))
df5 = df4.groupBy("year", "month").agg(F.sum("o_totalprice"))
df5.columns  # la columna calculada se llama "sum(o_totalprice)"

# COMMAND ----------

# MAGIC %md
# MAGIC **Patrón:**
# MAGIC - Transformaciones encadenadas entre paréntesis, una por línea.
# MAGIC - `F.col("columna")` para referenciar columnas.
# MAGIC - `withColumns` para crear varias columnas en un paso.
# MAGIC - `.alias()` en toda columna calculada.
# MAGIC - Nombre `df_` + el estado de los datos.

# COMMAND ----------

# DBTITLE 1,Patrón: encadenar, withColumns y alias
df_recent_orders = (
    df_orders
    .filter(F.col("o_orderdate") >= "1998-01-01")
    .select(
        "o_orderkey", "o_custkey", "o_totalprice",
        "o_orderdate", "o_orderpriority",
    )
    .withColumns(
        {
            "order_year": F.year("o_orderdate"),
            "order_month": F.month("o_orderdate"),
            "is_urgent": F.col("o_orderpriority") == "1-URGENT",
        }
    )
)
display(df_recent_orders.limit(10))

# COMMAND ----------

# DBTITLE 1,Agregaciones con alias
df_monthly_sales = (
    df_recent_orders
    .groupBy("order_year", "order_month")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("o_totalprice").alias("total_sales"),
        F.round(F.avg("o_totalprice"), 2).alias("avg_ticket"),
        F.sum(F.col("is_urgent").cast("int")).alias("urgent_count"),
    )
    .orderBy("order_year", "order_month")
)
display(df_monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 4 · Joins
# MAGIC
# MAGIC | Tipo | Devuelve |
# MAGIC |---|---|
# MAGIC | `inner` | Solo las filas con pareja en ambos lados |
# MAGIC | `left` | Todas las de la izquierda; nulos si no hay pareja |
# MAGIC | `left_anti` | Filas de la izquierda **sin** pareja: sirve para encontrar faltantes |
# MAGIC | `left_semi` | Filas de la izquierda **con** pareja, sin traer columnas de la derecha |
# MAGIC
# MAGIC **Reglas:** tipo de join siempre explícito (`how=`) y `F.broadcast()` para tablas pequeñas, que se copian a cada executor y evitan el shuffle de la tabla grande.

# COMMAND ----------

# DBTITLE 1,Join con broadcast de una dimensión pequeña
df_customers = spark.table("samples.tpch.customer")
df_nations = spark.table("samples.tpch.nation")  # 25 filas

df_customer_nation = df_customers.join(
    F.broadcast(df_nations),
    F.col("c_nationkey") == F.col("n_nationkey"),
    how="left",
).select(
    "c_custkey", "c_name", "c_mktsegment", F.col("n_name").alias("nation")
)
display(df_customer_nation.limit(10))

# COMMAND ----------

# DBTITLE 1,left_anti: clientes que nunca han hecho una orden
df_customers_without_orders = df_customers.join(
    df_orders, F.col("c_custkey") == F.col("o_custkey"), how="left_anti"
)
print("Clientes sin órdenes:", df_customers_without_orders.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar la cardinalidad
# MAGIC
# MAGIC Si la dimensión tiene llaves repetidas, un join N:1 **multiplica filas sin dar error** y los totales se inflan. Por eso se comparan los conteos antes y después.

# COMMAND ----------

# DBTITLE 1,Antipatrón: join sin validar
df_nations_dup = df_nations.unionByName(
    df_nations.filter(F.col("n_name") == "BRAZIL")
)
rows_before = df_customers.count()
rows_after = df_customers.join(
    df_nations_dup,
    F.col("c_nationkey") == F.col("n_nationkey"),
    how="left",
).count()
print(f"Antes: {rows_before:,} · después: {rows_after:,}")
print(f"Filas duplicadas por el join: {rows_after - rows_before:,}")

# COMMAND ----------

# DBTITLE 1,Patrón: validar y fallar con un mensaje claro
df_joined = df_customers.join(
    F.broadcast(df_nations),
    F.col("c_nationkey") == F.col("n_nationkey"),
    how="left",
)
assert df_joined.count() == rows_before, (
    "El join duplicó filas: la dimensión de naciones tiene llaves repetidas"
)
print("✅ Cardinalidad correcta")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Resolver los ejercicios en el notebook `03_pyspark_fundamentals_lab`, que está en esta misma carpeta (panel izquierdo, junto a este notebook).
