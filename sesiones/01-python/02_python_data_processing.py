# Databricks notebook source
# MAGIC %md
# MAGIC # M2 · Python para procesamiento de datos
# MAGIC
# MAGIC **Propósito:** pasar de instrucciones sueltas a funciones reutilizables que limpian y validan datos, y dar el paso de pandas a PySpark.
# MAGIC
# MAGIC | Bloque | Minutos | Tema |
# MAGIC |---|---|---|
# MAGIC | 1 | 0–15 | Registros, colecciones y la estructura correcta para cada operación |
# MAGIC | 2 | 15–30 | Funciones puras y composición de pasos |
# MAGIC | 3 | 30–45 | Validación de registros y manejo de errores con contexto |
# MAGIC | 4 | 45–60 | De pandas a PySpark |
# MAGIC
# MAGIC **Dependencias:** catálogo `samples` (bloque 4). Se ejecuta con compute Serverless.
# MAGIC
# MAGIC **Ejercicios:** notebook `02_python_data_processing_lab` (en esta misma carpeta)
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/01-python.md` §4–§6 (en la raíz del repositorio)

# COMMAND ----------

# DBTITLE 1,Imports
from collections.abc import Callable
from datetime import date, datetime

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 1 · Registros y colecciones
# MAGIC
# MAGIC Un registro es un `dict`; una tabla pequeña es una `list` de `dict`. La estructura correcta depende de la operación:
# MAGIC
# MAGIC | Operación | Estructura | Por qué |
# MAGIC |---|---|---|
# MAGIC | ¿Este código es válido? | `set` | Buscar en un `set` es inmediato; en una `list` se recorre completa |
# MAGIC | Buscar un valor por llave | `dict` | Acceso directo por llave |
# MAGIC | Mantener el orden | `list` | Conserva la secuencia |
# MAGIC | Llave compuesta | `tuple` | Inmutable, se puede usar como llave |

# COMMAND ----------

# DBTITLE 1,Datos de ejemplo
BOOKINGS = [
    {"booking_id": "RS001", "origin": " bog", "fare": 320000.0, "pax": 1},
    {"booking_id": "RS002", "origin": "MDE", "fare": 280000.0, "pax": 2},
    {"booking_id": "RS003", "origin": "xxx", "fare": 150000.0, "pax": 1},
    {"booking_id": "RS004", "origin": "clo", "fare": -5000.0, "pax": 1},
]

VALID_AIRPORTS = {"BOG", "MDE", "CLO", "CTG"}
CITY_BY_AIRPORT = {"BOG": "Bogotá", "MDE": "Rionegro", "CLO": "Cali"}

# COMMAND ----------

# DBTITLE 1,Comprensiones: transformar en una línea
origins = [booking["origin"].strip().upper() for booking in BOOKINGS]
unique_origins = set(origins)
fare_by_booking = {
    booking["booking_id"]: booking["fare"] for booking in BOOKINGS
}

print(origins)
print(unique_origins & VALID_AIRPORTS)  # intersección de conjuntos
print(fare_by_booking)

# COMMAND ----------

# MAGIC %md
# MAGIC **Regla:** una comprensión es para transformaciones de una línea. Si necesita varias condiciones, un `for` explícito se lee mejor.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 2 · Funciones puras y composición de pasos
# MAGIC
# MAGIC Una función **pura**:
# MAGIC - Solo depende de sus parámetros (no usa variables globales que cambian).
# MAGIC - No modifica lo que recibe: devuelve una copia nueva.
# MAGIC - Con la misma entrada, siempre da el mismo resultado.
# MAGIC
# MAGIC Por eso se puede probar sola y reejecutar una celda sin dañar los datos originales.

# COMMAND ----------

# DBTITLE 1,Antipatrón: modifica el registro original
def normalize_origin_in_place(booking):
    booking["origin"] = booking["origin"].strip().upper()
    return booking

# COMMAND ----------

# DBTITLE 1,Patrón: devuelve una copia


def normalize_origin(booking: dict) -> dict:
    """
    Normaliza el código de origen de una reserva.

    Args:
        booking: Reserva con la llave "origin".

    Returns:
        Copia de la reserva con el origen en mayúscula y sin espacios.
    """
    return {**booking, "origin": booking["origin"].strip().upper()}


def add_revenue(booking: dict) -> dict:
    """
    Agrega el ingreso de la reserva (tarifa por pasajeros).

    Args:
        booking: Reserva con las llaves "fare" y "pax".

    Returns:
        Copia de la reserva con la llave "revenue".
    """
    return {**booking, "revenue": booking["fare"] * booking["pax"]}


print(normalize_origin(BOOKINGS[0]))
print("Original sin cambios:", BOOKINGS[0]["origin"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composición: un pipeline como lista de pasos
# MAGIC
# MAGIC En Python las funciones se pueden guardar en una lista. Un proceso de limpieza queda como una secuencia de pasos pequeños, fáciles de probar y de reordenar.

# COMMAND ----------

# DBTITLE 1,Aplicar una lista de pasos
Step = Callable[[dict], dict]
CLEANING_STEPS: list[Step] = [normalize_origin, add_revenue]


def apply_steps(record: dict, steps: list[Step]) -> dict:
    """
    Aplica en orden cada paso a un registro.

    Args:
        record: Registro de entrada.
        steps: Funciones que reciben y devuelven un registro.

    Returns:
        Registro resultante después de todos los pasos.
    """
    for step in steps:
        record = step(record)
    return record


clean_bookings = [apply_steps(b, CLEANING_STEPS) for b in BOOKINGS]
clean_bookings

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 3 · Validación y manejo de errores
# MAGIC
# MAGIC ## 3.1 Validar un registro: devolver la lista de errores
# MAGIC
# MAGIC En lugar de detener el proceso en el primer problema, la función informa **todas** las reglas que incumple el registro. Es el mismo principio que la quarantine de la capa Silver (Sesión 03).

# COMMAND ----------

# DBTITLE 1,Validación con reglas explícitas


def validate_booking(booking: dict) -> list[str]:
    """
    Evalúa las reglas de calidad de una reserva.

    Args:
        booking: Reserva con origen ya normalizado.

    Returns:
        Lista de reglas incumplidas; vacía si la reserva es válida.
    """
    errors = []
    if booking["origin"] not in VALID_AIRPORTS:
        errors.append("unknown_airport")
    if booking["fare"] < 0:
        errors.append("negative_fare")
    return errors


for booking in clean_bookings:
    print(booking["booking_id"], validate_booking(booking))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Errores: específicos y con contexto
# MAGIC
# MAGIC | Situación | Qué hacer |
# MAGIC |---|---|
# MAGIC | Se espera un error concreto (texto que no es fecha) | Capturar **solo** ese tipo (`ValueError`) |
# MAGIC | El error debe detener el proceso | Re-lanzarlo con un mensaje que diga qué falló y qué revisar |
# MAGIC | Cualquier caso | Nunca `except: pass` |

# COMMAND ----------

# DBTITLE 1,Error esperado: probar varios formatos de fecha
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")


def parse_date(raw_date: str) -> date | None:
    """
    Convierte un texto a fecha probando los formatos conocidos.

    Args:
        raw_date: Fecha en texto, por ejemplo "2026-10-14".

    Returns:
        La fecha, o None si ningún formato coincide.
    """
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(raw_date.strip(), date_format).date()
        except ValueError:
            continue
    return None


print(parse_date("2026-10-14"), parse_date("15/10/2026"), parse_date("x"))

# COMMAND ----------

# DBTITLE 1,Error fatal: re-lanzar con contexto


def get_route_fare(fares: dict[str, float], route: str) -> float:
    """
    Obtiene la tarifa de una ruta.

    Args:
        fares: Tarifas por ruta, por ejemplo {"BOG-MDE": 320000.0}.
        route: Ruta a consultar.

    Returns:
        Tarifa de la ruta.

    Raises:
        KeyError: Si la ruta no existe en la tabla de tarifas.
    """
    try:
        return fares[route]
    except KeyError as error:
        raise KeyError(
            f"Ruta {route!r} sin tarifa. Revisar la tabla de tarifas "
            f"(rutas disponibles: {sorted(fares)})"
        ) from error


try:
    get_route_fare({"BOG-MDE": 320000.0}, "BOG-LIM")
except KeyError as error:
    print("Error controlado:", error)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 4 · De pandas a PySpark
# MAGIC
# MAGIC | | pandas | PySpark |
# MAGIC |---|---|---|
# MAGIC | Dónde corre | Memoria de una sola máquina (el driver) | Distribuido en el cluster |
# MAGIC | Cuándo ejecuta | De inmediato | Al pedir un resultado (`display`, `count`, `write`) |
# MAGIC | Uso en BPT | Operaciones ligeras sobre datos pequeños | Motor principal de procesamiento |

# COMMAND ----------

# MAGIC %md
# MAGIC **Regla:** `toPandas()` solo sobre datos acotados (`limit`, filtro fuerte o agregación). Sobre una tabla grande trae todo a una sola máquina y la sesión puede fallar.

# COMMAND ----------

# DBTITLE 1,Antipatrón: traer la tabla completa a pandas
# df_all_lines = spark.table("samples.tpch.lineitem").toPandas()
# Se deja comentado a propósito: la tabla tiene millones de filas.

# COMMAND ----------

# DBTITLE 1,pandas sobre una muestra acotada
df_orders_sample = (
    spark.table("samples.tpch.orders").limit(1000).toPandas()
)
df_orders_sample["o_totalprice"] = df_orders_sample["o_totalprice"].astype(
    float
)

df_sales_pandas = (
    df_orders_sample.groupby("o_orderpriority", as_index=False)
    .agg(total_sales=("o_totalprice", "sum"))
    .sort_values("o_orderpriority")
)
df_sales_pandas

# COMMAND ----------

# DBTITLE 1,El mismo cálculo en PySpark, sobre la tabla completa
df_sales_spark = (
    spark.table("samples.tpch.orders")
    .groupBy("o_orderpriority")
    .agg(F.sum("o_totalprice").alias("total_sales"))
    .orderBy("o_orderpriority")
)
display(df_sales_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC La sintaxis es parecida, pero PySpark procesa la tabla completa de forma distribuida. Esa es la base de la Sesión 02.
# MAGIC
# MAGIC ---
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Resolver los ejercicios en el notebook `02_python_data_processing_lab`, que está en esta misma carpeta (panel izquierdo, junto a este notebook).
