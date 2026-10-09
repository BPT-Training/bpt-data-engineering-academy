# Databricks notebook source
# MAGIC %md
# MAGIC # Solución · Lab M1
# MAGIC
# MAGIC Solución de referencia. Hay otras soluciones válidas: lo importante es que pasen los asserts y respeten la [guía](../../../guia-buenas-practicas/01-python.md).

# COMMAND ----------

from collections import defaultdict
from datetime import date, datetime

RESERVAS = [
    {"reserva_id": "RS001", "origen": " bog", "destino": "MDE", "fecha": "2026-10-14", "tarifa": 320000.0, "pasajeros": 1},
    {"reserva_id": "RS002", "origen": "CLO", "destino": "clo ", "fecha": "2026-10-15", "tarifa": 280000.0, "pasajeros": 2},
    {"reserva_id": "RS003", "origen": "CTG", "destino": "BOG", "fecha": "15/10/2026", "tarifa": -50000.0, "pasajeros": 1},
    {"reserva_id": "RS004", "origen": "MDE", "destino": "MIA", "fecha": "2026-13-01", "tarifa": 1450000.0, "pasajeros": 1},
    {"reserva_id": "RS005", "origen": "BOG", "destino": "LIM", "fecha": "16/10/2026", "tarifa": 980000.0, "pasajeros": 0},
    {"reserva_id": "RS006", "origen": "bog", "destino": "CTG", "fecha": "2026-10-17", "tarifa": 410000.0, "pasajeros": 3},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1

# COMMAND ----------

def normalizar_codigo(codigo: str | None) -> str | None:
    """Quita espacios y pasa a mayúscula. None o vacío → None."""
    if codigo is None:
        return None
    limpio = codigo.strip().upper()
    return limpio or None

# COMMAND ----------

assert normalizar_codigo(" bog") == "BOG"
assert normalizar_codigo("clo ") == "CLO"
assert normalizar_codigo("   ") is None
assert normalizar_codigo(None) is None
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2

# COMMAND ----------

FORMATOS_FECHA = ("%Y-%m-%d", "%d/%m/%Y")


def parsear_fecha(texto: str | None) -> date | None:
    """Convierte texto a date probando cada formato conocido. Sin coincidencia → None."""
    if texto is None:
        return None
    for formato in FORMATOS_FECHA:
        try:
            return datetime.strptime(texto.strip(), formato).date()
        except ValueError:
            continue
    return None

# COMMAND ----------

assert parsear_fecha("2026-10-14") == date(2026, 10, 14)
assert parsear_fecha("15/10/2026") == date(2026, 10, 15)
assert parsear_fecha("2026-13-01") is None, "El mes 13 no existe"
assert parsear_fecha(None) is None
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3

# COMMAND ----------

def validar_reserva(reserva: dict) -> list[str]:
    """Lista de reglas que incumple la reserva. Vacía si es válida."""
    errores = []
    if normalizar_codigo(reserva["origen"]) == normalizar_codigo(reserva["destino"]):
        errores.append("origen_igual_destino")
    if reserva["tarifa"] < 0:
        errores.append("tarifa_negativa")
    if parsear_fecha(reserva["fecha"]) is None:
        errores.append("fecha_invalida")
    if reserva["pasajeros"] < 1:
        errores.append("sin_pasajeros")
    return errores

# COMMAND ----------

errores_esperados = {
    "RS001": [],
    "RS002": ["origen_igual_destino"],
    "RS003": ["tarifa_negativa"],
    "RS004": ["fecha_invalida"],
    "RS005": ["sin_pasajeros"],
    "RS006": [],
}
for r in RESERVAS:
    obtenido = validar_reserva(r)
    assert obtenido == errores_esperados[r["reserva_id"]], f"{r['reserva_id']}: se obtuvo {obtenido}"
print("✅ Ejercicio 3 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4

# COMMAND ----------

def separar_reservas(reservas: list[dict]) -> tuple[list[dict], list[dict]]:
    """Separa en (válidas normalizadas, inválidas con sus errores) sin modificar la entrada."""
    validas, invalidas = [], []
    for reserva in reservas:
        errores = validar_reserva(reserva)
        if errores:
            invalidas.append({**reserva, "errores": errores})
        else:
            validas.append(
                {
                    **reserva,
                    "origen": normalizar_codigo(reserva["origen"]),
                    "destino": normalizar_codigo(reserva["destino"]),
                    "fecha": parsear_fecha(reserva["fecha"]),
                }
            )
    return validas, invalidas

# COMMAND ----------

validas, invalidas = separar_reservas(RESERVAS)

assert [r["reserva_id"] for r in validas] == ["RS001", "RS006"]
assert validas[0]["origen"] == "BOG" and validas[1]["origen"] == "BOG"
assert validas[0]["fecha"] == date(2026, 10, 14)
assert [r["reserva_id"] for r in invalidas] == ["RS002", "RS003", "RS004", "RS005"]
assert invalidas[0]["errores"] == ["origen_igual_destino"]
assert RESERVAS[0]["origen"] == " bog", "¡Se modificó la reserva original!"
assert "errores" not in RESERVAS[1], "¡Se modificó la reserva original!"
print("✅ Ejercicio 4 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 5

# COMMAND ----------

def calc(l):
    d = {}
    for i in range(len(l)):
        x = l[i]
        if x["tarifa"] > 0:
            if x["origen"].strip().upper() in d:
                d[x["origen"].strip().upper()] = d[x["origen"].strip().upper()] + x["tarifa"] * x["pasajeros"]
            else:
                d[x["origen"].strip().upper()] = x["tarifa"] * x["pasajeros"]
    return d


def ingreso_por_origen(reservas: list[dict]) -> dict[str, float]:
    """Suma tarifa × pasajeros por aeropuerto de origen, solo con tarifas positivas."""
    ingresos: defaultdict[str, float] = defaultdict(float)
    for reserva in reservas:
        if reserva["tarifa"] > 0:
            ingresos[normalizar_codigo(reserva["origen"])] += reserva["tarifa"] * reserva["pasajeros"]
    return dict(ingresos)

# COMMAND ----------

assert ingreso_por_origen(RESERVAS) == calc(RESERVAS)
assert ingreso_por_origen(validas) == {"BOG": 320000.0 + 410000.0 * 3}
assert ingreso_por_origen([]) == {}
print("✅ Ejercicio 5 correcto")
