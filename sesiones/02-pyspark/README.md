# Sesión 02 · PySpark (M3 + M4)

**Duración:** 2 h (1 h + 1 h, con pausa)

**Objetivo:** entender cómo trabaja Spark y aplicar los patrones más frecuentes de ingeniería de datos con el estándar de código de BPT.

**Datos:** `samples.tpch` (`customer`, `orders`, `nation`).

**Enfoque de los labs:** cada ejercicio es una función `DataFrame → DataFrame`. Se verifica con DataFrames pequeños de resultado conocido y luego se aplica sin cambios a `samples.tpch`.

## M3 · PySpark Fundamentals (1 h)

| Bloque | Minutos | Contenido |
|---|---|---|
| 1 | 0–15 | Driver y executors; transformaciones frente a acciones; shuffle |
| 2 | 15–30 | DataFrames y schemas explícitos |
| 3 | 30–45 | `select`, `filter`, `withColumns`, agregaciones con alias (antipatrón y patrón) |
| 4 | 45–60 | Tipos de join, broadcast, `left_anti` y validación de cardinalidad |

| Archivo | Contenido |
|---|---|
| [`03_pyspark_fundamentals.py`](03_pyspark_fundamentals.py) | Notebook guiado |
| [`03_pyspark_fundamentals_lab.py`](03_pyspark_fundamentals_lab.py) | 5 ejercicios: venta por nación, clientes sin órdenes, órdenes urgentes por año, join con broadcast y corrección de un join que duplica filas |

## M4 · PySpark Advanced Patterns (1 h)

| Bloque | Minutos | Contenido |
|---|---|---|
| 1 | 0–15 | Window functions: `row_number`, `rank`, `lag`, acumulados con marco explícito |
| 2 | 15–30 | Deduplicación determinística (y su versión en `bpt_utils`) |
| 3 | 30–45 | Nulos: `count(*)` frente a `count(columna)`, `isNull`, `eqNullSafe`, `coalesce` |
| 4 | 45–60 | Fechas en varios formatos y UTC; funciones nativas frente a UDF |

| Archivo | Contenido |
|---|---|
| [`04_pyspark_advanced.py`](04_pyspark_advanced.py) | Notebook guiado |
| [`04_pyspark_advanced_lab.py`](04_pyspark_advanced_lab.py) | 3 ejercicios: **último registro válido por entidad** (entregable), venta acumulada y deduplicación con desempate por fuente |

Soluciones en [`soluciones/`](soluciones/).

**Entregable:** `get_latest_valid_record`, una función PySpark que obtiene el último registro válido por entidad, aplicada a `samples.tpch.orders`.

**Guía relacionada:** [02-pyspark.md](../../guia-buenas-practicas/02-pyspark.md)
