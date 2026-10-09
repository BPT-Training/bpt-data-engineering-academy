# Databricks notebook source
# MAGIC %md
# MAGIC # M1 · Python Fundamentals para ingeniería de datos
# MAGIC
# MAGIC **Objetivo:** repasar el Python que usamos todos los días para transformar y validar datos, con el estilo que exige BPT.
# MAGIC
# MAGIC | Bloque | Tema |
# MAGIC |---|---|
# MAGIC | 1 | Registros como `dict` y colecciones de registros |
# MAGIC | 2 | Tipos, `None` y conversiones |
# MAGIC | 3 | Control de flujo |
# MAGIC | 4 | Funciones con type hints |
# MAGIC | 5 | Excepciones específicas |
# MAGIC | 6 | PEP 8 y legibilidad: antes y después |
# MAGIC
# MAGIC > Guía: [01-python.md](../../guia-buenas-practicas/01-python.md)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Registros como `dict`
# MAGIC
# MAGIC Un registro de datos (una fila) se representa naturalmente como un `dict`: la llave es el nombre de la columna y el valor es el dato.
# MAGIC Una tabla pequeña es una `list` de `dict`.

# COMMAND ----------

vuelos = [
    {"vuelo_id": "VL0001", "origen": "BOG", "destino": "MDE", "salida": "2026-10-14 06:00", "retraso_min": 5},
    {"vuelo_id": "VL0002", "origen": "mde", "destino": "CLO", "salida": "2026-10-14 08:30", "retraso_min": 42},
    {"vuelo_id": "VL0003", "origen": "CLO", "destino": "CTG", "salida": "14/10/2026 11:15", "retraso_min": None},
    {"vuelo_id": "VL0004", "origen": "CTG", "destino": "CTG", "salida": "2026-10-14 13:45", "retraso_min": 0},
]

primer_vuelo = vuelos[0]
print(primer_vuelo["origen"])            # acceso por llave
print(primer_vuelo.get("aerolinea"))     # .get devuelve None si la llave no existe (no lanza KeyError)
print(len(vuelos), "vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Elegir la estructura correcta
# MAGIC
# MAGIC | Estructura | Úsala para | Ejemplo |
# MAGIC |---|---|---|
# MAGIC | `list` | Secuencias ordenadas | Filas de un archivo |
# MAGIC | `dict` | Búsqueda por llave | `codigo_iata → ciudad` |
# MAGIC | `set` | Pertenencia y únicos | Códigos válidos |
# MAGIC | `tuple` | Registros inmutables, llaves compuestas | `(origen, destino)` |

# COMMAND ----------

AEROPUERTOS_VALIDOS = {"BOG", "MDE", "CLO", "CTG"}   # set: `in` es O(1)
CIUDAD_POR_CODIGO = {"BOG": "Bogotá", "MDE": "Rionegro", "CLO": "Cali", "CTG": "Cartagena"}

rutas = {(v["origen"].upper(), v["destino"]) for v in vuelos}   # set de tuplas: rutas únicas
print(rutas)
print("MDE" in AEROPUERTOS_VALIDOS, CIUDAD_POR_CODIGO["CLO"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tipos, `None` y conversiones
# MAGIC
# MAGIC Los datos de archivos llegan como texto. Convertir tipos es trabajo diario, y **`None` es el `null` de Python**.

# COMMAND ----------

from datetime import date, datetime

texto_tarifa = "320000"
tarifa = float(texto_tarifa)
print(type(texto_tarifa).__name__, "→", type(tarifa).__name__)

fecha = datetime.strptime("2026-10-14 06:00", "%Y-%m-%d %H:%M")
print(fecha.date(), fecha.hour)

# Comparar con None: siempre con `is`, nunca con `==`
retraso = vuelos[2]["retraso_min"]
print("Sin dato de retraso" if retraso is None else retraso)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Cuidado con los "falsy":** `0`, `""`, `[]` y `None` son todos falsos en un `if`. Un retraso de `0` minutos **es un dato válido**.

# COMMAND ----------

for v in vuelos:
    # ❌ `if not v["retraso_min"]` trataría el retraso 0 como faltante
    # ✅ comparar explícitamente contra None
    estado = "sin dato" if v["retraso_min"] is None else f"{v['retraso_min']} min"
    print(v["vuelo_id"], estado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Control de flujo

# COMMAND ----------

UMBRAL_PUNTUAL_MIN = 15   # constante en MAYÚSCULA: no es un "número mágico" enterrado en el código

for v in vuelos:
    retraso = v["retraso_min"]
    if retraso is None:
        categoria = "desconocido"
    elif retraso <= UMBRAL_PUNTUAL_MIN:
        categoria = "a tiempo"
    elif retraso <= 60:
        categoria = "retraso moderado"
    else:
        categoria = "retraso severo"
    print(f"{v['vuelo_id']}: {categoria}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funciones con type hints
# MAGIC
# MAGIC Reglas BPT:
# MAGIC - Hacen **una sola cosa**.
# MAGIC - Son **puras**: no modifican sus entradas ni dependen de globales.
# MAGIC - Llevan **type hints** y un docstring corto.

# COMMAND ----------

def categorizar_retraso(retraso_min: int | None, umbral_puntual: int = 15) -> str:
    """Clasifica un retraso en minutos. None → 'desconocido'."""
    if retraso_min is None:
        return "desconocido"
    if retraso_min <= umbral_puntual:
        return "a tiempo"
    if retraso_min <= 60:
        return "retraso moderado"
    return "retraso severo"


def con_categoria(vuelo: dict) -> dict:
    """Devuelve una COPIA del vuelo con la categoría agregada: no modifica el original."""
    return {**vuelo, "categoria": categorizar_retraso(vuelo["retraso_min"])}


vuelos_categorizados = [con_categoria(v) for v in vuelos]
print(vuelos_categorizados[1])
print("¿Original intacto?", "categoria" not in vuelos[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Excepciones específicas
# MAGIC
# MAGIC - Captura **solo** la excepción que esperas (`ValueError`, `KeyError`…). Un `except:` desnudo esconde bugs.
# MAGIC - Cuando lances un error, di **qué dato** falló.

# COMMAND ----------

FORMATOS_FECHA = ("%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M")


def parsear_salida(texto: str) -> datetime:
    """Intenta cada formato conocido; si ninguno sirve lanza ValueError con el valor recibido."""
    for formato in FORMATOS_FECHA:
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    raise ValueError(f"Fecha de salida con formato desconocido: {texto!r}")


for v in vuelos:
    print(v["vuelo_id"], parsear_salida(v["salida"]))

# COMMAND ----------

try:
    parsear_salida("mañana temprano")
except ValueError as error:
    print("Error controlado →", error)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. PEP 8 y legibilidad: antes y después
# MAGIC
# MAGIC Las dos funciones hacen lo mismo. ¿Cuál preferirías revisar en un PR?

# COMMAND ----------

# ❌ Antes
def f(l):
    r = []
    for i in range(len(l)):
        if l[i]["origen"].upper() != l[i]["destino"].upper():
            if l[i]["retraso_min"] != None and l[i]["retraso_min"] > 15:
                r.append(l[i]["vuelo_id"])
    return r


# ✅ Después
def vuelos_retrasados(vuelos: list[dict], umbral_min: int = 15) -> list[str]:
    """IDs de vuelos válidos (origen ≠ destino) con retraso mayor al umbral."""
    return [
        v["vuelo_id"]
        for v in vuelos
        if v["origen"].upper() != v["destino"].upper()
        and v["retraso_min"] is not None
        and v["retraso_min"] > umbral_min
    ]


assert f(vuelos) == vuelos_retrasados(vuelos)
print(vuelos_retrasados(vuelos))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio breve (2 min)
# MAGIC
# MAGIC Escribe `normalizar_codigo(codigo: str) -> str`, que quite espacios y pase a mayúscula. Úsala para contar cuántos vuelos salen de cada aeropuerto.
# MAGIC
# MAGIC Luego sigue con el lab: [01_python_fundamentals_lab](./01_python_fundamentals_lab).

# COMMAND ----------

# Tu código aquí
