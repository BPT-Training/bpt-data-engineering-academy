# Sesión 01 · Python para datos (M1 + M2)

**Duración:** 2 h (1 h + 1 h, con pausa)

**Objetivo:** consolidar los fundamentos de Python con el estándar de código de BPT y aplicarlos a la limpieza y validación de datos.

## M1 · Python Fundamentals (1 h)

| Bloque | Minutos | Contenido |
|---|---|---|
| 1 | 0–15 | Cómo se trabaja en un notebook; PEP 8 en lo esencial (antipatrón y patrón) |
| 2 | 15–30 | Tipos de datos, conversiones, `None`, listas y diccionarios |
| 3 | 30–45 | `if` y `for`, funciones con docstring y type hints, `try`/`except` |
| 4 | 45–60 | pandas: crear, seleccionar, filtrar, nueva columna y agrupar |

| Archivo | Contenido |
|---|---|
| [`01_python_fundamentals.py`](01_python_fundamentals.py) | Notebook guiado |
| [`01_python_fundamentals_lab.py`](01_python_fundamentals_lab.py) | 5 ejercicios: `normalize_code`, `classify_delay`, `count_by_origin`, `to_int`, `get_average_delay_by_origin` |

## M2 · Python para procesamiento de datos (1 h)

| Bloque | Minutos | Contenido |
|---|---|---|
| 1 | 0–15 | Registros y colecciones; la estructura correcta para cada operación; comprensiones |
| 2 | 15–30 | Funciones puras y composición de pasos |
| 3 | 30–45 | Validación de registros; errores específicos y re-lanzados con contexto |
| 4 | 45–60 | De pandas a PySpark: el mismo cálculo sobre una muestra y sobre la tabla completa |

| Archivo | Contenido |
|---|---|
| [`02_python_data_processing.py`](02_python_data_processing.py) | Notebook guiado |
| [`02_python_data_processing_lab.py`](02_python_data_processing_lab.py) | 5 ejercicios: `validate_booking`, `split_bookings`, refactor a `get_revenue_by_origin`, `get_sales_by_status` en pandas y en PySpark |

Soluciones en [`soluciones/`](soluciones/).

**Entregable:** funciones de limpieza y validación aplicadas a un dataset (ejercicios 1 a 3 del lab M2).

**Material de referencia:** el temario de M1 toma como base las lecciones 01–06 del curso oficial *Introduction to Python for Data Science and Data Engineering* de Databricks Academy ([referencias.md](../../referencias.md)).

**Guía relacionada:** [01-python.md](../../guia-buenas-practicas/01-python.md)
