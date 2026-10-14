# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M1 · Python Fundamentals
# MAGIC
# MAGIC **Instrucciones:** completar cada función donde dice `# TODO` y ejecutar la celda de verificación que sigue. Cada ejercicio termina con un mensaje de confirmación cuando está correcto.
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `normalize_code` | Textos y `None` |
# MAGIC | 2 | `classify_delay` | `if` / `elif` / `else` |
# MAGIC | 3 | `count_by_origin` | `for` y diccionarios |
# MAGIC | 4 | `to_int` | `try` / `except` |
# MAGIC | 5 | `get_average_delay_by_origin` | pandas |
# MAGIC
# MAGIC Recordatorio del estándar: nombres en `snake_case`, docstring en cada función y líneas de máximo 79 caracteres.
# MAGIC
# MAGIC Solución de referencia: notebook `01_python_fundamentals_lab_solucion` (subcarpeta `soluciones` de esta misma carpeta). Se recomienda consultarla solo después de resolver los ejercicios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antes de empezar: trabajar sobre una copia
# MAGIC
# MAGIC Las respuestas se escriben en una **copia personal** de este notebook. Así no se pierden cuando se actualiza el repositorio con **Pull**.
# MAGIC
# MAGIC 1. En la barra superior de este notebook, abrir el menú **File** y seleccionar **Clone**.
# MAGIC 2. En **Name**, agregar `_mi_copia` al final del nombre: `01_python_fundamentals_lab_mi_copia`.
# MAGIC 3. En **Destination**, verificar que esté seleccionada la **misma carpeta** de este notebook (`sesiones/01-python` dentro de la carpeta del repositorio). Si aparece otra, navegar hasta esa carpeta y seleccionarla.
# MAGIC 4. Seleccionar **Clone**. La copia se abre automáticamente.
# MAGIC 5. Confirmar que el nombre en la parte superior termina en `_mi_copia` y resolver los ejercicios ahí.
# MAGIC
# MAGIC **Por qué en la misma carpeta:** la copia usa la configuración del curso con rutas relativas a esta carpeta. Si se guarda en otra ubicación (por ejemplo, la carpeta personal), deja de encontrarla.

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Funciones de verificación (no modificar)


def check_pending(result, exercise, function_name):
    """Falla con un mensaje claro si la función todavía no está resuelta."""
    if result is None:
        raise AssertionError(
            f"{exercise} pendiente: completar la función {function_name} "
            "donde dice # TODO y ejecutar de nuevo su celda."
        )


def check_equal(actual, expected, description):
    """Compara un resultado con el esperado y explica la diferencia."""
    if actual != expected:
        raise AssertionError(
            f"{description}: se obtuvo {actual!r}; se esperaba {expected!r}."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · `normalize_code`
# MAGIC
# MAGIC Quitar los espacios al inicio y al final, y pasar a mayúscula. Si el código es `None` o queda vacío, devolver `None`.
# MAGIC
# MAGIC Pista: los métodos `strip()` y `upper()` de los textos.

# COMMAND ----------

def normalize_code(code: str | None) -> str | None:
    """
    Normaliza un código de aeropuerto.

    Args:
        code: Código original, por ejemplo " bog".

    Returns:
        Código en mayúscula y sin espacios, o None si está vacío.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 1
check_pending(normalize_code(" bog"), "Ejercicio 1", "normalize_code")
check_equal(normalize_code(" bog"), "BOG", "normalize_code(' bog')")
check_equal(normalize_code("mde "), "MDE", "normalize_code('mde ')")
check_equal(normalize_code("   "), None, "normalize_code('   ')")
check_equal(normalize_code(None), None, "normalize_code(None)")
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · `classify_delay`
# MAGIC
# MAGIC Clasificar un retraso en minutos:
# MAGIC
# MAGIC | Condición | Resultado |
# MAGIC |---|---|
# MAGIC | `None` | `"unknown"` |
# MAGIC | Menor o igual a 15 | `"on_time"` |
# MAGIC | Mayor a 15 y menor o igual a 60 | `"delayed"` |
# MAGIC | Mayor a 60 | `"severely_delayed"` |
# MAGIC
# MAGIC Usar las constantes definidas en la celda (no escribir 15 ni 60 dentro de la función).

# COMMAND ----------

ON_TIME_THRESHOLD_MIN = 15
SEVERE_DELAY_THRESHOLD_MIN = 60


def classify_delay(delay_minutes: int | None) -> str:
    """
    Clasifica un retraso según los umbrales definidos.

    Args:
        delay_minutes: Minutos de retraso, o None si no hay dato.

    Returns:
        "unknown", "on_time", "delayed" o "severely_delayed".
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 2
check_pending(classify_delay(0), "Ejercicio 2", "classify_delay")
for delay, expected in [
    (None, "unknown"),
    (0, "on_time"),
    (15, "on_time"),
    (16, "delayed"),
    (60, "delayed"),
    (61, "severely_delayed"),
]:
    check_equal(classify_delay(delay), expected, f"classify_delay({delay})")
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · `count_by_origin`
# MAGIC
# MAGIC Recibir una lista de vuelos y devolver un diccionario con la cantidad de vuelos por aeropuerto de origen. Reutilizar `normalize_code` del ejercicio 1, porque los códigos llegan con espacios y en minúscula.

# COMMAND ----------

FLIGHTS = [
    {"flight_id": "VL0001", "origin": "BOG"},
    {"flight_id": "VL0002", "origin": " bog"},
    {"flight_id": "VL0003", "origin": "MDE"},
    {"flight_id": "VL0004", "origin": "clo "},
    {"flight_id": "VL0005", "origin": "BOG"},
]


def count_by_origin(flights: list[dict]) -> dict[str, int]:
    """
    Cuenta los vuelos por aeropuerto de origen.

    Args:
        flights: Lista de vuelos con la llave "origin".

    Returns:
        Diccionario {codigo_origen: cantidad_de_vuelos}.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 3
check_pending(count_by_origin(FLIGHTS), "Ejercicio 3", "count_by_origin")
check_equal(
    count_by_origin(FLIGHTS),
    {"BOG": 3, "MDE": 1, "CLO": 1},
    "count_by_origin(FLIGHTS)",
)
check_equal(count_by_origin([]), {}, "count_by_origin([])")
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4 · `to_int`
# MAGIC
# MAGIC Convertir un texto a entero. Si el texto no es un número, devolver `None`.
# MAGIC
# MAGIC Capturar **solo** `ValueError`. No usar `except:` sin tipo.

# COMMAND ----------

def to_int(value: str) -> int | None:
    """
    Convierte un texto a entero.

    Args:
        value: Texto a convertir, por ejemplo "42".

    Returns:
        El entero, o None si el texto no es numérico.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 4
check_pending(to_int("42"), "Ejercicio 4", "to_int")
check_equal(to_int("42"), 42, "to_int('42')")
check_equal(to_int(" 7 "), 7, "to_int(' 7 ')")
check_equal(to_int("sin dato"), None, "to_int('sin dato')")
check_equal(to_int(""), None, "to_int('')")
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 5 · `get_average_delay_by_origin` (pandas)
# MAGIC
# MAGIC Recibir un DataFrame de vuelos y devolver otro con las columnas `origin` y `avg_delay` (retraso promedio), ordenado de mayor a menor retraso y con el índice reiniciado.
# MAGIC
# MAGIC Pista: `groupby`, `agg`, `sort_values` y `reset_index(drop=True)`.

# COMMAND ----------

DF_FLIGHTS = pd.DataFrame(
    {
        "flight_id": ["VL1", "VL2", "VL3", "VL4", "VL5"],
        "origin": ["BOG", "MDE", "BOG", "CLO", "MDE"],
        "delay_minutes": [10, 50, 30, 0, 70],
    }
)


def get_average_delay_by_origin(df_flights: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el retraso promedio por aeropuerto de origen.

    Args:
        df_flights: Vuelos con las columnas origin y delay_minutes.

    Returns:
        DataFrame con origin y avg_delay, de mayor a menor retraso.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 5
df_result = get_average_delay_by_origin(DF_FLIGHTS)
check_pending(df_result, "Ejercicio 5", "get_average_delay_by_origin")
check_equal(
    list(df_result.columns), ["origin", "avg_delay"], "Columnas del resultado"
)
check_equal(
    list(df_result.itertuples(index=False, name=None)),
    [("MDE", 60.0), ("BOG", 20.0), ("CLO", 0.0)],
    "Filas del resultado (en orden)",
)
print("✅ Ejercicio 5 correcto")
