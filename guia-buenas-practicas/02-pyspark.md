# 02 · PySpark: convenciones, transformaciones y rendimiento

Reglas para escribir transformaciones PySpark legibles en un code review y que escalen en producción.

---

## 1. Imports y librerías

### 1.1 Alias estándar obligatorios

| Módulo | Alias |
|---|---|
| `pyspark.sql.functions` | `F` |
| `pyspark.sql.types` | `T` |
| `pyspark.sql.window.Window` | `W` |

```python
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W
```

**Prohibido:** `from pyspark.sql.functions import *`. Sobrescribe funciones de Python (`sum`, `max`, `round`, `filter`) y genera errores difíciles de ver. Tampoco se usan alias no estándar (`fn`, `types`).

### 1.2 Librerías aceptadas
- **Flujo estándar de ingeniería de datos:** `pyspark`, `delta-spark`, `databricks-sdk`, `dbutils` y `pandas`. pandas solo para operaciones ligeras, nunca como motor principal.
- **Flujos especiales** (analítica avanzada, ML): librerías adicionales en su propia carpeta y con `requirements.txt` explícito.
- **No se usa `%pip install` en notebooks productivos.** Las dependencias se resuelven en la configuración del compute o del job.

---

## 2. Naming en PySpark

| Elemento | Convención | Ejemplo |
|---|---|---|
| DataFrames | Prefijo `df_` + estado de los datos | `df_raw`, `df_clean`, `df_dedup`, `df_final` |
| Columnas temporales | Prefijo `_tmp_` | `_tmp_row_number`, `_tmp_rank` |
| Columnas técnicas | Prefijo `_` | `_ingest_ts`, `_source_file` |
| Columnas de negocio | `snake_case` | `departure_ts`, `delay_minutes` |
| Columnas booleanas | `is_`, `has_` | `is_on_time`, `is_domestic` |

**Por qué:** el nombre dice en qué estado están los datos sin leer cada línea. Si en un notebook se usa `df_clean`, se usa igual en todos los del dominio.

```python
# ❌
df = spark.read.table(source_table)
df2 = df.filter(F.col("status") == "OK")
df3 = df2.dropDuplicates(["flight_id"])

# ✅
df_raw = spark.read.table(source_table)
df_clean = _udf_clean_columns(df_raw)
df_dedup = _udf_drop_duplicates_by_key(df_clean, KEYS, ORDER_BY)
```

---

## 3. Estilo de transformaciones

### 3.1 Encadenar con paréntesis, una operación por línea
Así se respeta el límite de 79 caracteres y cada paso se lee solo.

```python
df_monthly_sales = (
    df_orders
    .filter(F.col("o_orderstatus") == "F")
    .withColumn("order_year", F.year("o_orderdate"))
    .groupBy("order_year")
    .agg(F.sum("o_totalprice").alias("total_sales"))
)
```

### 3.2 `F.col()` para referenciar columnas
En código reutilizable se usa `F.col("columna")`, no `df.columna` ni `df["columna"]`, para no atar la expresión a un DataFrame concreto.

### 3.3 Siempre `.alias()` en columnas calculadas
Sin alias la columna se llama `sum(o_totalprice)` y rompe el código posterior.

### 3.4 `withColumns` en vez de muchos `withColumn`
Agregar varias columnas en un solo paso mantiene el plan de ejecución simple.

```python
df_enriched = df_orders.withColumns(
    {
        "order_year": F.year("o_orderdate"),
        "order_month": F.month("o_orderdate"),
        "is_urgent": F.col("o_orderpriority") == "1-URGENT",
    }
)
```

---

## 4. Funciones de transformación (UDF)

En el estándar, una **UDF** es una función interna que encapsula lógica de transformación, limpieza o validación de un DataFrame.

| Regla | Detalle |
|---|---|
| Prefijo `_udf_` | Deja claro que es una función interna de transformación: `_udf_clean_columns` |
| Vive en un módulo versionado | Nunca definida ad hoc dentro de un notebook productivo |
| Contrato documentado | Docstring con `Args` y `Returns` ([01 §3.2](01-python.md#32-docstrings-en-formato-google)) |
| Nativas primero | Se usa `pyspark.sql.functions` siempre que exista el equivalente |

### 4.1 Funciones nativas antes que UDF de Python
Las funciones nativas las optimiza el motor (Catalyst) y se ejecutan distribuidas sin serializar datos. Una UDF de Python (`@F.udf`) procesa fila a fila entre la JVM y Python, y el optimizador no puede ver qué hace.

```python
# ❌ UDF de Python cuando existe una función nativa
@F.udf(T.StringType())
def to_upper(value):
    return value.upper() if value else None


df_final = df_clean.withColumn("origin", to_upper("origin"))

# ✅ Función nativa
df_final = df_clean.withColumn("origin", F.upper(F.col("origin")))
```

---

## 5. Schemas y tipos

- **Schema explícito** al leer fuentes (CSV, JSON) en producción. `inferSchema` solo para explorar.
- **Castear temprano** en Silver, con funciones explícitas: `F.to_timestamp(col, formato)`, `.cast("decimal(14,2)")`.

```python
FLIGHT_SCHEMA = T.StructType(
    [
        T.StructField("flight_id", T.StringType(), nullable=False),
        T.StructField("origin", T.StringType()),
        T.StructField("departure_ts", T.StringType()),
    ]
)
df_raw = spark.read.csv(path, header=True, schema=FLIGHT_SCHEMA)
```

---

## 6. Patrones frecuentes

### 6.1 Deduplicación determinística
`dropDuplicates(["id"])` conserva una fila cualquiera. Para elegir cuál, se usa una ventana con orden explícito y criterio de desempate.

```python
window = W.partitionBy("flight_id").orderBy(
    F.col("_ingest_ts").desc(), F.col("_source_file").desc()
)
df_dedup = (
    df_clean
    .withColumn("_tmp_row_number", F.row_number().over(window))
    .filter(F.col("_tmp_row_number") == 1)
    .drop("_tmp_row_number")
)
```

### 6.2 Nulos: decisión explícita
Para cada columna clave se decide y documenta si un nulo se descarta, se imputa (`F.coalesce`) o va a quarantine. `null == null` no es verdadero en Spark; para comparar nulos se usa `eqNullSafe`.

### 6.3 Joins
- Tipo de join siempre explícito (`how="left"`).
- `F.broadcast()` para dimensiones pequeñas.
- `how="left_anti"` para encontrar registros sin pareja.
- **Validar la cardinalidad:** en un join N:1, el número de filas no debe cambiar.

```python
rows_before = df_flights.count()
df_joined = df_flights.join(
    F.broadcast(df_airports), on="origin", how="left"
)
assert df_joined.count() == rows_before, (
    "El join duplicó filas: la dimensión tiene llaves repetidas"
)
```

### 6.4 Window functions
Partición y orden explícitos. En acumulados, marco explícito: `rowsBetween(W.unboundedPreceding, W.currentRow)`.

---

## 7. Rendimiento básico

| ❌ Evitar | ✅ Preferir |
|---|---|
| `collect()` o `toPandas()` sobre tablas grandes | Agregar primero, o usar `limit` |
| pandas como motor principal (`df.toPandas().apply(...)`) | Transformaciones PySpark |
| `count()` en cada celda | Una sola acción al final |
| Filtrar después del join | Filtrar y seleccionar columnas antes |
| `cache()` en compute serverless | No está soportado: materializar en una tabla si es necesario |
