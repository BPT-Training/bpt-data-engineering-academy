# Sesión 01 · Python para ingeniería de datos (M1 + M2)

**Duración:** 2 h (1 h + 1 h, con break)

**Objetivo:** nivelar Python para transformar y validar datos, y preparar el salto a Spark.

## M1 · Python Fundamentals (1 h)
| Archivo | Contenido |
|---|---|
| [`01_python_fundamentals.py`](01_python_fundamentals.py) | Registros como `dict`, elección de estructuras, tipos y `None`, control de flujo, funciones puras con type hints, excepciones específicas, refactor antes/después |
| [`01_python_fundamentals_lab.py`](01_python_fundamentals_lab.py) | 5 ejercicios: `normalizar_codigo`, `parsear_fecha`, `validar_reserva`, `separar_reservas`, refactor de `calc` → `ingreso_por_origen` |

## M2 · Python for Data Processing (1 h)
| Archivo | Contenido |
|---|---|
| [`02_python_data_processing.py`](02_python_data_processing.py) | Composición de pasos, comprensiones, generadores (memoria), pandas sobre una muestra de `samples.tpch.orders`, el mismo cálculo en PySpark, pandas API on Spark |
| [`02_python_data_processing_lab.py`](02_python_data_processing_lab.py) | 4 ejercicios: `aplicar_pasos`, generador `en_lotes`, `venta_por_estado` en pandas y en PySpark (con comparación sobre datos reales) |

Soluciones en [`soluciones/`](soluciones/). Todos los ejercicios del M1 son Python puro.

**Entregable:** notebook con funciones de limpieza y validación aplicadas a un dataset (lab M1 completo en ✅).

**Guía relacionada:** [01-python.md](../../guia-buenas-practicas/01-python.md)
