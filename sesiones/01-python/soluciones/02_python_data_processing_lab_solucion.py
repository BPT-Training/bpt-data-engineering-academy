# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M2

# COMMAND ----------

# DBTITLE 1,Imports
from collections import defaultdict
from datetime import date, datetime

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Datos y funciones de apoyo
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

# DBTITLE 1,Ejercicio 1


def validate_booking(booking: dict) -> list[str]:
    """
    Evalúa las reglas de calidad de una reserva.

    Args:
        booking: Reserva con origin, destination, booking_date, fare
            y pax.

    Returns:
        Lista de reglas incumplidas; vacía si la reserva es válida.
    """
    errors = []
    origin = normalize_code(booking["origin"])
    if origin == normalize_code(booking["destination"]):
        errors.append("origin_equals_destination")
    if booking["fare"] < 0:
        errors.append("negative_fare")
    if parse_date(booking["booking_date"]) is None:
        errors.append("invalid_date")
    if booking["pax"] < 1:
        errors.append("no_passengers")
    return errors


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

# DBTITLE 1,Ejercicio 2


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
    valid_bookings, invalid_bookings = [], []
    for booking in bookings:
        errors = validate_booking(booking)
        if errors:
            invalid_bookings.append({**booking, "errors": errors})
            continue
        valid_bookings.append(
            {
                **booking,
                "origin": normalize_code(booking["origin"]),
                "destination": normalize_code(booking["destination"]),
                "booking_date": parse_date(booking["booking_date"]),
            }
        )
    return valid_bookings, invalid_bookings


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

# DBTITLE 1,Ejercicio 3


def calc(l):  # noqa: E741 - código original del ejercicio
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
    revenue_by_origin: defaultdict[str, float] = defaultdict(float)
    for booking in bookings:
        if booking["fare"] > 0:
            origin = normalize_code(booking["origin"])
            revenue_by_origin[origin] += booking["fare"] * booking["pax"]
    return dict(revenue_by_origin)


assert get_revenue_by_origin(BOOKINGS) == calc(BOOKINGS)
assert get_revenue_by_origin(valid_bookings) == {
    "BOG": 320000.0 + 410000.0 * 3
}
assert get_revenue_by_origin([]) == {}
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 4
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
    return (
        df_orders.groupby("o_orderstatus", as_index=False)
        .agg(
            total_sales=("o_totalprice", "sum"),
            order_count=("o_orderkey", "count"),
        )
        .sort_values("total_sales", ascending=False)
        .reset_index(drop=True)
    )


df_result = get_sales_by_status(DF_TEST_ORDERS)
assert list(df_result.columns) == [
    "o_orderstatus", "total_sales", "order_count",
]
assert list(df_result.itertuples(index=False, name=None)) == [
    ("F", 125.0, 2), ("O", 120.0, 2), ("P", 10.0, 1),
]
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# DBTITLE 1,Ejercicio 5


def get_sales_by_status_spark(df_orders: DataFrame) -> DataFrame:
    """
    Calcula la venta total y el número de órdenes por estado.

    Args:
        df_orders: Órdenes con o_orderkey, o_orderstatus y o_totalprice.

    Returns:
        DataFrame ordenado de mayor a menor venta.
    """
    return (
        df_orders.groupBy("o_orderstatus")
        .agg(
            F.sum("o_totalprice").alias("total_sales"),
            F.count("o_orderkey").alias("order_count"),
        )
        .orderBy(F.col("total_sales").desc())
    )


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
