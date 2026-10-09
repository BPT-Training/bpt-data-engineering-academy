# 02 · PySpark: rendimiento y legibilidad

Reglas para escribir transformaciones PySpark que se entiendan en un code review y escalen en producción.

---

## 1. Estilo

### 1.1 Imports con alias
**Regla:** `from pyspark.sql import functions as F, types as T`. Nunca `from pyspark.sql.functions import *`.
**Por qué:** el `import *` sobrescribe built-ins de Python (`sum`, `max`, `round`, `filter`) y produce errores difíciles de ver.

### 1.2 Transformaciones encadenadas, una por línea
**Regla:** encadena dentro de paréntesis, con una transformación por línea.

```python
# ❌
df2 = df.filter(F.col("o_orderstatus") == "F")
df3 = df2.withColumn("anio", F.year("o_orderdate"))
df4 = df3.groupBy("anio").agg(F.sum("o_totalprice"))

# ✅
df_ventas_por_anio = (
    df_ordenes
    .filter(F.col("o_orderstatus") == "F")
    .withColumn("anio", F.year("o_orderdate"))
    .groupBy("anio")
    .agg(F.sum("o_totalprice").alias("venta_total"))
)
```

### 1.3 `F.col()` para referenciar columnas
**Regla:** en código reutilizable usa `F.col("columna")`, no `df.columna` ni `df["columna"]`.
**Por qué:** `F.col` no ata la expresión a un DataFrame concreto, así que la función sirve para cualquiera.

### 1.4 Nombres de columna en `snake_case`
**Regla:** renombra en la primera capa (Silver) y usa `snake_case` en español o inglés, pero de forma consistente en todo el proyecto.

### 1.5 Siempre `.alias()` en agregaciones
**Regla:** toda columna calculada lleva nombre explícito.
**Por qué:** sin alias la columna se llama `sum(o_totalprice)` y romperá cualquier código posterior.

---

## 2. Schemas

### 2.1 Schema explícito al leer fuentes
**Regla:** en producción, las lecturas de CSV y JSON declaran el schema. `inferSchema` es aceptable solo para explorar.
**Por qué:** inferir obliga a leer los datos dos veces y un cambio en la fuente cambia los tipos en silencio.

```python
SCHEMA_VUELOS = T.StructType([
    T.StructField("vuelo_id", T.StringType(), nullable=False),
    T.StructField("origen", T.StringType()),
    T.StructField("salida_programada", T.StringType()),
])
df = spark.read.csv(ruta, header=True, schema=SCHEMA_VUELOS)
```

### 2.2 Castear temprano y explícitamente
**Regla:** convierte fechas, decimales y enteros en Silver, con funciones explícitas (`F.to_timestamp(col, formato)`, `.cast("decimal(12,2)")`).
**Por qué:** los errores de tipo aparecen en un solo lugar y no se propagan a Gold.

---

## 3. Transformaciones

### 3.1 Built-ins antes que UDFs
**Regla:** busca primero en `pyspark.sql.functions`. Si no existe, usa una pandas UDF. Las UDF de Python fila a fila son el último recurso.
**Por qué:** las funciones built-in corren dentro de la JVM y el optimizador las entiende. Una UDF de Python serializa cada fila entre la JVM y Python, así que es una caja negra y suele ser mucho más lenta.

```python
# ❌
@F.udf("string")
def limpiar(s):
    return s.strip().upper() if s else None
df.withColumn("origen", limpiar("origen"))

# ✅
df.withColumn("origen", F.upper(F.trim("origen")))
```

### 3.2 `withColumns` o `select` en vez de muchos `withColumn`
**Regla:** para agregar varias columnas, usa un solo `withColumns({...})` o un `select`.
**Por qué:** cada `withColumn` agrega un nodo al plan; con decenas de columnas el análisis del plan se vuelve lento.

```python
df = df.withColumns({
    "anio": F.year("o_orderdate"),
    "mes": F.month("o_orderdate"),
    "es_urgente": F.col("o_orderpriority").startswith("1"),
})
```

### 3.3 Deduplicación determinística
**Regla:** para quedarte con un registro por llave, usa una window con `row_number()` y un orden explícito que incluya un criterio de desempate.
**Por qué:** `dropDuplicates(["id"])` se queda con un registro cualquiera; dos ejecuciones pueden dar resultados distintos.

```python
w = Window.partitionBy("vuelo_id").orderBy(F.col("_ingest_ts").desc(), F.col("_source_file").desc())
df_dedup = (
    df.withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)
```

### 3.4 Nulos: decisión explícita y documentada
**Regla:** decide y deja escrito qué pasa con los nulos de cada columna clave: se descartan, se imputan (`F.coalesce`) o van a quarantine.
**Por qué:** en Spark, `null == null` no es verdadero y `count("col")` ignora nulos, así que los nulos cambian resultados sin avisar.

```python
# Comparación segura con nulos
df.filter(F.col("a").eqNullSafe(F.col("b")))
```

---

## 4. Joins

### 4.1 Tipo de join siempre explícito
**Regla:** escribe `how="inner"`, `how="left"`, etc., aunque sea el valor por defecto.

### 4.2 Broadcast de tablas pequeñas
**Regla:** cuando una tabla es pequeña (dimensiones, catálogos), usa `F.broadcast(df_pequeno)`.
**Por qué:** Spark envía la tabla pequeña a cada executor y evita mover (shuffle) la grande.

```python
df = df_ordenes.join(F.broadcast(df_naciones), on="n_nationkey", how="left")
```

### 4.3 Evitar columnas ambiguas
**Regla:** une por nombre de columna (`on="llave"`) cuando coincida en ambos lados. Si no coincide, usa alias y `select` explícito justo después del join.

### 4.4 Validar la cardinalidad
**Regla:** en joins que deberían ser 1:1 o N:1, compara el conteo de filas antes y después.
**Por qué:** una llave duplicada en la dimensión multiplica filas del hecho, y los totales se inflan sin error visible.

```python
n_antes = df_hechos.count()
df_join = df_hechos.join(df_dim, "llave", "left")
assert df_join.count() == n_antes, "El join duplicó filas: la dimensión tiene llaves repetidas"
```

### 4.5 Anti join para encontrar faltantes
**Regla:** para "registros sin pareja" usa `how="left_anti"`, no un left join seguido de un filtro por nulo.

---

## 5. Window functions

### 5.1 Partición y orden explícitos
**Regla:** toda window define `partitionBy` (salvo que el cálculo sea realmente global) y `orderBy`.
**Por qué:** una window sin partición lleva todos los datos a un solo executor.

### 5.2 Marco (frame) explícito en acumulados
**Regla:** para acumulados, declara `rowsBetween(Window.unboundedPreceding, Window.currentRow)`.
**Por qué:** el marco por defecto con `orderBy` es `RANGE` y trata como iguales las filas empatadas en el orden.

### 5.3 Patrones frecuentes
| Necesidad | Función |
|---|---|
| Último registro por entidad | `row_number()` con orden descendente, y filtrar `= 1` |
| Valor anterior / siguiente | `lag()` / `lead()` |
| Acumulado | `sum().over(w.rowsBetween(...))` |
| Ranking con empates | `dense_rank()` |

---

## 6. Rendimiento

### 6.1 Nada de `collect()` sobre datos grandes
**Regla:** `collect()`, `toPandas()` y `display()` sin límite solo sobre resultados agregados o acotados.

### 6.2 Filtrar y proyectar temprano
**Regla:** aplica `filter` y `select` lo antes posible, especialmente antes de joins.
**Por qué:** menos datos en el shuffle. Delta además salta archivos completos (data skipping).

### 6.3 `cache()` con criterio
**Regla:** cachea solo si el mismo DataFrame se usa en varias acciones, y llama `unpersist()` al terminar. En serverless, `cache()` no está soportado: materializa en una tabla si realmente lo necesitas.

### 6.4 Evitar acciones repetidas
**Regla:** no llames `count()` o `collect()` dentro de bucles o en cada celda para "ver cómo va".
**Por qué:** cada acción recalcula todo el linaje desde la fuente.

### 6.5 Leer el plan
**Regla:** ante una consulta lenta, revisa `df.explain(mode="formatted")` y la Spark UI antes de "optimizar a ciegas". Busca `BroadcastHashJoin` vs. `SortMergeJoin`, filtros empujados (`PushedFilters`) y el número de shuffles (`Exchange`).

---

## 7. Testing

### 7.1 Transformaciones como funciones
**Regla:** la lógica va en funciones `DataFrame -> DataFrame` en módulos `.py`. El notebook solo lee, llama las funciones y escribe.

```python
def filtrar_vuelos_validos(df: DataFrame) -> DataFrame:
    return df.filter(F.col("origen") != F.col("destino"))
```

### 7.2 Tests con DataFrames pequeños
**Regla:** cada función de transformación tiene un test con un DataFrame construido a mano que cubra el caso normal y los bordes (nulos, duplicados, vacío).

```python
from pyspark.testing import assertDataFrameEqual

def test_filtrar_vuelos_validos(spark):
    df = spark.createDataFrame([("BOG", "MDE"), ("BOG", "BOG")], "origen string, destino string")
    esperado = spark.createDataFrame([("BOG", "MDE")], "origen string, destino string")
    assertDataFrameEqual(filtrar_vuelos_validos(df), esperado)
```
