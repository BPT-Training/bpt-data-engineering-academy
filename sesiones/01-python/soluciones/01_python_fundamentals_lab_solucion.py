# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M1
# MAGIC
# MAGIC Solución de referencia. Otras soluciones son válidas si pasan las verificaciones y respetan la [guía](../../../guia-buenas-practicas/01-python.md).

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Ejercicio 1


def normalize_code(code: str | None) -> str | None:
    """
    Normaliza un código de aeropuerto.

    Args:
        code: Código original, por ejemplo " bog".

    Returns:
        Código en mayúscula y sin espacios, o None si está vacío.
    """
    if code is None:
        return None
    clean_code = code.strip().upper()
    return clean_code or None


assert normalize_code(" bog") == "BOG"
assert normalize_code("mde ") == "MDE"
assert normalize_code("   ") is None
assert normalize_code(None) is None
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 2
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
    if delay_minutes is None:
        return "unknown"
    if delay_minutes <= ON_TIME_THRESHOLD_MIN:
        return "on_time"
    if delay_minutes <= SEVERE_DELAY_THRESHOLD_MIN:
        return "delayed"
    return "severely_delayed"


assert classify_delay(None) == "unknown"
assert classify_delay(0) == "on_time"
assert classify_delay(15) == "on_time"
assert classify_delay(16) == "delayed"
assert classify_delay(60) == "delayed"
assert classify_delay(61) == "severely_delayed"
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 3
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
    counts = {}
    for flight in flights:
        origin = normalize_code(flight["origin"])
        counts[origin] = counts.get(origin, 0) + 1
    return counts


assert count_by_origin(FLIGHTS) == {"BOG": 3, "MDE": 1, "CLO": 1}
assert count_by_origin([]) == {}
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 4


def to_int(value: str) -> int | None:
    """
    Convierte un texto a entero.

    Args:
        value: Texto a convertir, por ejemplo "42".

    Returns:
        El entero, o None si el texto no es numérico.
    """
    try:
        return int(value)
    except ValueError:
        return None


assert to_int("42") == 42
assert to_int(" 7 ") == 7
assert to_int("sin dato") is None
assert to_int("") is None
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 5
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
    return (
        df_flights.groupby("origin", as_index=False)
        .agg(avg_delay=("delay_minutes", "mean"))
        .sort_values("avg_delay", ascending=False)
        .reset_index(drop=True)
    )


df_result = get_average_delay_by_origin(DF_FLIGHTS)
assert list(df_result.columns) == ["origin", "avg_delay"]
assert list(df_result.itertuples(index=False, name=None)) == [
    ("MDE", 60.0),
    ("BOG", 20.0),
    ("CLO", 0.0),
]
print("✅ Ejercicio 5 correcto")
