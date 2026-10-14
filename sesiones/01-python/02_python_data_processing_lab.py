# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M2 · Python para procesamiento de datos
# MAGIC
# MAGIC **Instrucciones:** completar cada función donde dice `# TODO` y ejecutar la celda de verificación que sigue.
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `validate_booking` | Reglas de calidad explícitas |
# MAGIC | 2 | `split_bookings` | Funciones puras: no modificar la entrada |
# MAGIC | 3 | `get_revenue_by_origin` | Refactor de antipatrón a patrón |
# MAGIC | 4 | `get_sales_by_status` | Agregación en pandas |
# MAGIC | 5 | `get_sales_by_status_spark` | El mismo cálculo en PySpark |
# MAGIC
# MAGIC **Entregable de la sesión:** los ejercicios 1 a 3 forman un conjunto de funciones de limpieza y validación aplicadas a un dataset.
# MAGIC
# MAGIC Solución de referencia: notebook `02_python_data_processing_lab_solucion` (subcarpeta `soluciones` de esta misma carpeta).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antes de empezar: trabajar sobre una copia
# MAGIC
# MAGIC Las respuestas se escriben en una **copia personal** de este notebook. Así no se pierden cuando se actualiza el repositorio con **Pull**.
# MAGIC
# MAGIC 1. En la barra superior de este notebook, abrir el menú **File** y seleccionar **Clone**.
# MAGIC 2. En **Name**, agregar `_mi_copia` al final del nombre: `02_python_data_processing_lab_mi_copia`.
# MAGIC 3. En **Destination**, verificar que esté seleccionada la **misma carpeta** de este notebook (`sesiones/01-python` dentro de la carpeta del repositorio). Si aparece otra, navegar hasta esa carpeta y seleccionarla.
# MAGIC 4. Seleccionar **Clone**. La copia se abre automáticamente.
# MAGIC 5. Confirmar que el nombre en la parte superior termina en `_mi_copia` y resolver los ejercicios ahí.
# MAGIC
# MAGIC **Por qué en la misma carpeta:** la copia usa la configuración del curso con rutas relativas a esta carpeta. Si se guarda en otra ubicación (por ejemplo, la carpeta personal), deja de encontrarla.

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date, datetime

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: F401 - para el ejercicio 5

# COMMAND ----------

# DBTITLE 1,Datos y funciones de apoyo (ya resueltas)
BOOKINGS = [
    {"booking_id": "RS001", "origin": " bog", "destination": "MDE",
     "booking_date": "2026-10-14", "fare": 320000.0, "pax": 1},
    {"booking_id": "RS002", "origin": "CLO", "destination": "clo ",
     "booking_date": "2026-10-15", "fare": 280000.0, "pax": 2},
    {"booking_id": "RS003", "origin": "CTG", "destination": "BOG",
     "booking_date": "15/10/2026", "fare": -50000.0, "pax": 1},
    {"booking_id": "RS004", "origin": "MDE", "destination": "MIA",
     "booking_date": "2026-13-01", "fare": 1450000.0, "pax": 1},
    {"booking_id": "RS005", "origin": "BOG", "destination": "LIM",
     "booking_date": "16/10/2026", "fare": 980000.0, "pax": 0},
    {"booking_id": "RS006", "origin": "bog", "destination": "CTG",
     "booking_date": "2026-10-17", "fare": 410000.0, "pax": 3},
]
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")


def normalize_code(code: str | None) -> str | None:
    """Pasa un código a mayúscula sin espacios; vacío o None -> None."""
    if code is None:
        return None
    return code.strip().upper() or None


def parse_date(raw_date: str | None) -> date | None:
    """Convierte un texto a fecha con los formatos conocidos, o None."""
    if raw_date is None:
        return None
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(raw_date.strip(), date_format).date()
        except ValueError:
            continue
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · `validate_booking`
# MAGIC
# MAGIC Devolver la lista de reglas que incumple una reserva (vacía si es válida), evaluadas en este orden:
# MAGIC
# MAGIC | Regla | Condición de error |
# MAGIC |---|---|
# MAGIC | `origin_equals_destination` | Origen y destino normalizados son iguales |
# MAGIC | `negative_fare` | `fare < 0` |
# MAGIC | `invalid_date` | `parse_date` devuelve `None` |
# MAGIC | `no_passengers` | `pax < 1` |

# COMMAND ----------

def validate_booking(booking: dict) -> list[str]:
    """
    Evalúa las reglas de calidad de una reserva.

    Args:
        booking: Reserva con origin, destination, booking_date, fare
            y pax.

    Returns:
        Lista de reglas incumplidas; vacía si la reserva es válida.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 1
EXPECTED_ERRORS = {
    "RS001": [],
    "RS002": ["origin_equals_destination"],
    "RS003": ["negative_fare"],
    "RS004": ["invalid_date"],
    "RS005": ["no_passengers"],
    "RS006": [],
}
for booking in BOOKINGS:
    errors = validate_booking(booking)
    booking_id = booking["booking_id"]
    assert errors == EXPECTED_ERRORS[booking_id], (
        f"{booking_id}: se obtuvo {errors}"
    )
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · `split_bookings`
# MAGIC
# MAGIC Devolver una tupla `(valid_bookings, invalid_bookings)`:
# MAGIC - **Válidas:** copias con `origin` y `destination` normalizados y `booking_date` convertida a `date`.
# MAGIC - **Inválidas:** copias con una llave adicional `errors`.
# MAGIC
# MAGIC **No modificar** los diccionarios originales: crear copias con `{**booking, ...}`.

# COMMAND ----------

def split_bookings(
    bookings: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Separa las reservas en válidas (normalizadas) e inválidas.

    Args:
        bookings: Reservas originales.

    Returns:
        Tupla (valid_bookings, invalid_bookings).
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 2
valid_bookings, invalid_bookings = split_bookings(BOOKINGS)

assert [b["booking_id"] for b in valid_bookings] == ["RS001", "RS006"]
assert valid_bookings[0]["origin"] == "BOG"
assert valid_bookings[0]["booking_date"] == date(2026, 10, 14)
assert [b["booking_id"] for b in invalid_bookings] == [
    "RS002", "RS003", "RS004", "RS005",
]
assert invalid_bookings[0]["errors"] == ["origin_equals_destination"]
assert BOOKINGS[0]["origin"] == " bog", "Se modificó la reserva original"
assert "errors" not in BOOKINGS[1], "Se modificó la reserva original"
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Refactor de `calc`
# MAGIC
# MAGIC La función `calc` funciona, pero no pasaría una revisión de código. Escribir `get_revenue_by_origin`, que devuelva **el mismo resultado** y cumpla el estándar:
# MAGIC - Nombre descriptivo, type hints y docstring.
# MAGIC - Sin repetir `x["origin"].strip().upper()`: reutilizar `normalize_code`.
# MAGIC - Sin `range(len(...))` ni variables de una letra.
# MAGIC
# MAGIC Ingreso de una reserva: `fare * pax`. Solo cuentan las tarifas positivas.

# COMMAND ----------

def calc(l):  # noqa: E741 - código a refactorizar
    d = {}
    for i in range(len(l)):
        x = l[i]
        if x["fare"] > 0:
            if x["origin"].strip().upper() in d:
                d[x["origin"].strip().upper()] = (
                    d[x["origin"].strip().upper()] + x["fare"] * x["pax"]
                )
            else:
                d[x["origin"].strip().upper()] = x["fare"] * x["pax"]
    return d


def get_revenue_by_origin(bookings: list[dict]) -> dict[str, float]:
    """
    Suma el ingreso (tarifa por pasajeros) por aeropuerto de origen.

    Args:
        bookings: Reservas con origin, fare y pax.

    Returns:
        Diccionario {codigo_origen: ingreso}, solo con tarifas positivas.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 3
assert get_revenue_by_origin(BOOKINGS) == calc(BOOKINGS)
assert get_revenue_by_origin(valid_bookings) == {
    "BOG": 320000.0 + 410000.0 * 3
}
assert get_revenue_by_origin([]) == {}
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4 · `get_sales_by_status` (pandas)
# MAGIC
# MAGIC Devolver un DataFrame con las columnas `o_orderstatus`, `total_sales` (suma de `o_totalprice`) y `order_count`, ordenado por `total_sales` de mayor a menor y con el índice reiniciado.

# COMMAND ----------

DF_TEST_ORDERS = pd.DataFrame(
    {
        "o_orderkey": [1, 2, 3, 4, 5],
        "o_orderstatus": ["F", "O", "F", "P", "O"],
        "o_totalprice": [100.0, 50.0, 25.0, 10.0, 70.0],
    }
)


def get_sales_by_status(df_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la venta total y el número de órdenes por estado.

    Args:
        df_orders: Órdenes con o_orderkey, o_orderstatus y o_totalprice.

    Returns:
        DataFrame ordenado de mayor a menor venta.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 4
df_result = get_sales_by_status(DF_TEST_ORDERS)
assert list(df_result.columns) == [
    "o_orderstatus", "total_sales", "order_count",
]
assert list(df_result.itertuples(index=False, name=None)) == [
    ("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1),
]
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 5 · `get_sales_by_status_spark` (PySpark)
# MAGIC
# MAGIC El mismo cálculo con PySpark: `groupBy`, `agg` con `F.sum` y `F.count`, `.alias()` en cada columna calculada y `orderBy` descendente.

# COMMAND ----------

def get_sales_by_status_spark(df_orders: DataFrame) -> DataFrame:
    """
    Calcula la venta total y el número de órdenes por estado.

    Args:
        df_orders: Órdenes con o_orderkey, o_orderstatus y o_totalprice.

    Returns:
        DataFrame ordenado de mayor a menor venta.
    """
    # TODO
    ...

# COMMAND ----------

# DBTITLE 1,Verificación ejercicio 5
df_result_spark = get_sales_by_status_spark(
    spark.createDataFrame(DF_TEST_ORDERS)
)
assert df_result_spark.columns == [
    "o_orderstatus", "total_sales", "order_count",
]
assert [tuple(row) for row in df_result_spark.collect()] == [
    ("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1),
]
print("✅ Ejercicio 5 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicación sobre la tabla completa
# MAGIC
# MAGIC La misma función, sin cambios, sobre los millones de filas de `samples.tpch.orders`.

# COMMAND ----------

display(get_sales_by_status_spark(spark.table("samples.tpch.orders")))
