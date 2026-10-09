# Databricks notebook source
# MAGIC %md
# MAGIC # Lab M1 · Limpieza y validación de reservas con Python puro
# MAGIC
# MAGIC Completa cada función donde dice `# TODO` y ejecuta la celda de verificación que sigue. Cada ejercicio termina en ✅ cuando está bien.
# MAGIC
# MAGIC | # | Ejercicio | Concepto |
# MAGIC |---|---|---|
# MAGIC | 1 | `normalizar_codigo` | Strings, `None` |
# MAGIC | 2 | `parsear_fecha` | Excepciones específicas |
# MAGIC | 3 | `validar_reserva` | Funciones puras, reglas |
# MAGIC | 4 | `separar_reservas` | Colecciones, no mutar entradas |
# MAGIC | 5 | `ingreso_por_origen` | Refactor a estilo BPT |
# MAGIC
# MAGIC Solución de referencia: [soluciones/01_python_fundamentals_lab_solucion](./soluciones/01_python_fundamentals_lab_solucion). Se recomienda consultarla solo después de resolver los ejercicios.

# COMMAND ----------

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
# MAGIC ## Ejercicio 1 · `normalizar_codigo`
# MAGIC
# MAGIC Quita espacios y pasa a mayúscula. Si el código es `None` o queda vacío, devuelve `None`.

# COMMAND ----------

def normalizar_codigo(codigo: str | None) -> str | None:
    # TODO
    ...

# COMMAND ----------

assert normalizar_codigo(" bog") == "BOG"
assert normalizar_codigo("clo ") == "CLO"
assert normalizar_codigo("   ") is None
assert normalizar_codigo(None) is None
print("✅ Ejercicio 1 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · `parsear_fecha`
# MAGIC
# MAGIC Acepta los formatos `AAAA-MM-DD` y `DD/MM/AAAA` y devuelve un `date`. Si el texto es `None` o no coincide con ningún formato, devuelve `None`.
# MAGIC
# MAGIC **Nota:** `datetime.strptime` lanza `ValueError` si el formato no coincide. Captura **solo** esa excepción.

# COMMAND ----------

FORMATOS_FECHA = ("%Y-%m-%d", "%d/%m/%Y")


def parsear_fecha(texto: str | None) -> date | None:
    # TODO
    ...

# COMMAND ----------

assert parsear_fecha("2026-10-14") == date(2026, 10, 14)
assert parsear_fecha("15/10/2026") == date(2026, 10, 15)
assert parsear_fecha("2026-13-01") is None, "El mes 13 no existe"
assert parsear_fecha(None) is None
print("✅ Ejercicio 2 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · `validar_reserva`
# MAGIC
# MAGIC Devuelve la **lista de errores** de una reserva (vacía si es válida), evaluados en este orden:
# MAGIC
# MAGIC | Error | Condición |
# MAGIC |---|---|
# MAGIC | `origen_igual_destino` | Origen y destino, ya normalizados, son iguales |
# MAGIC | `tarifa_negativa` | `tarifa < 0` |
# MAGIC | `fecha_invalida` | `parsear_fecha` devuelve `None` |
# MAGIC | `sin_pasajeros` | `pasajeros < 1` |
# MAGIC
# MAGIC Reutiliza las funciones de los ejercicios 1 y 2.

# COMMAND ----------

def validar_reserva(reserva: dict) -> list[str]:
    # TODO
    ...

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
# MAGIC ## Ejercicio 4 · `separar_reservas`
# MAGIC
# MAGIC Devuelve una tupla `(validas, invalidas)`:
# MAGIC - **validas:** copias de las reservas válidas con `origen` y `destino` normalizados y `fecha` convertida a `date`.
# MAGIC - **invalidas:** copias de las reservas inválidas con una llave extra `errores`.
# MAGIC
# MAGIC ⚠️ **No modifiques** los diccionarios originales: usa `{**reserva, ...}`.

# COMMAND ----------

def separar_reservas(reservas: list[dict]) -> tuple[list[dict], list[dict]]:
    # TODO
    ...

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
# MAGIC ## Ejercicio 5 · Refactor a estilo BPT
# MAGIC
# MAGIC Esta función funciona, pero no pasaría un code review. Escribe `ingreso_por_origen`, que devuelva **lo mismo** con:
# MAGIC - Nombres descriptivos y type hints.
# MAGIC - Sin repetir `x["origen"].strip().upper()` (reutiliza `normalizar_codigo`).
# MAGIC - Sin `range(len(...))`.
# MAGIC
# MAGIC Ingreso de una reserva = `tarifa * pasajeros`. Solo cuentan tarifas positivas.

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
    # TODO
    ...

# COMMAND ----------

assert ingreso_por_origen(RESERVAS) == calc(RESERVAS)
assert ingreso_por_origen(validas) == {"BOG": 320000.0 + 410000.0 * 3}
assert ingreso_por_origen([]) == {}
print("✅ Ejercicio 5 correcto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregable de la sesión
# MAGIC
# MAGIC Si llegaste hasta aquí con todo en ✅, tienes un conjunto de **funciones de limpieza y validación aplicadas a un dataset**.
# MAGIC
# MAGIC En el lab M2 las encadenamos en un pipeline y damos el salto a Spark.
