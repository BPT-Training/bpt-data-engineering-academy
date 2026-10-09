# Sesión 02 · PySpark Fundamentals + Advanced Patterns (M3 + M4)

**Duración:** 2 h (1 h + 1 h, con break)

**Objetivo:** entender Spark distribuido y aplicar los patrones más frecuentes de ingeniería de datos.

**Datos:** `samples.tpch` (`customer`, `orders`, `lineitem`, `nation`, `region`).

**Enfoque de los labs:** cada ejercicio es una función `DataFrame → DataFrame`. Se prueba con DataFrames pequeños de resultado conocido y luego se aplica sin cambios a `samples.tpch`.

## M3 · PySpark Fundamentals (1 h)
| Archivo | Contenido |
|---|---|
| [`03_pyspark_fundamentals.py`](03_pyspark_fundamentals.py) | Driver/executors, lazy evaluation, schemas explícitos, `select`/`filter`/`withColumns`, agregaciones, tipos de join, broadcast, validación de cardinalidad, `explain` |
| [`03_pyspark_fundamentals_lab.py`](03_pyspark_fundamentals_lab.py) | 5 ejercicios: venta por nación, clientes sin órdenes (`left_anti`), urgentes por año, join con broadcast, corrección de un join que duplica filas |

## M4 · PySpark Advanced Patterns (1 h)
| Archivo | Contenido |
|---|---|
| [`04_pyspark_advanced.py`](04_pyspark_advanced.py) | Windows (`row_number`/`rank`/`dense_rank`, `lag`, acumulados), dedup determinística, nulos, fechas y zonas horarias, UDF vs. pandas UDF vs. built-in (con medición), rendimiento y plan |
| [`04_pyspark_advanced_lab.py`](04_pyspark_advanced_lab.py) | 4 ejercicios: **último registro válido por entidad** (entregable), acumulado por cliente, reemplazar una UDF, dedup con desempate por fuente |

Soluciones en [`soluciones/`](soluciones/).

**Entregable:** `ultimo_registro_valido`, una query PySpark que obtiene el último registro válido por entidad, aplicada a `samples.tpch.orders`.

**Guía relacionada:** [02-pyspark.md](../../guia-buenas-practicas/02-pyspark.md)
