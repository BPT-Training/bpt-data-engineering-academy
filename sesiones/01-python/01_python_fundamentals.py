# Databricks notebook source
# MAGIC %md
# MAGIC # M1 · Python Fundamentals
# MAGIC
# MAGIC **Propósito:** repasar los fundamentos de Python que se usan para trabajar con datos, con el estándar de código de BPT desde el primer ejemplo.
# MAGIC
# MAGIC | Bloque | Minutos | Tema |
# MAGIC |---|---|---|
# MAGIC | 1 | 0–15 | Notebooks y lineamientos de código (PEP 8) |
# MAGIC | 2 | 15–30 | Tipos de datos, variables, listas y diccionarios |
# MAGIC | 3 | 30–45 | Control de flujo, funciones y manejo de errores |
# MAGIC | 4 | 45–60 | pandas: primeros pasos con tablas |
# MAGIC
# MAGIC **Dependencias:** ninguna. Se ejecuta con compute Serverless.
# MAGIC
# MAGIC **Ejercicios:** [01_python_fundamentals_lab](./01_python_fundamentals_lab)
# MAGIC
# MAGIC > Guía: [01-python.md](../../guia-buenas-practicas/01-python.md)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 1 · Notebooks y lineamientos de código
# MAGIC
# MAGIC ## 1.1 Cómo se trabaja en un notebook
# MAGIC
# MAGIC - Un notebook está hecho de **celdas**: de código (Python o SQL) o de texto (`%md`).
# MAGIC - Una celda se ejecuta con **Shift + Enter**. El resultado aparece debajo.
# MAGIC - Las variables creadas en una celda quedan disponibles para las siguientes.
# MAGIC - Si la sesión se reinicia, las variables se pierden: se vuelve a ejecutar desde el inicio.

# COMMAND ----------

# DBTITLE 1,Primera celda
print("Hola, Databricks")
2 + 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 PEP 8: el estándar de estilo
# MAGIC
# MAGIC PEP 8 es la guía oficial de estilo de Python y es **obligatoria** en los desarrollos de BPT. Lo esencial:
# MAGIC
# MAGIC | Regla | Ejemplo |
# MAGIC |---|---|
# MAGIC | Indentación de 4 espacios | Dentro de `if`, `for`, `def` |
# MAGIC | Máximo 79 caracteres por línea | Las líneas largas se parten con paréntesis |
# MAGIC | Variables y funciones en `snake_case` | `flight_count`, `get_delay` |
# MAGIC | Constantes en `UPPER_SNAKE_CASE` | `ON_TIME_THRESHOLD_MIN = 15` |
# MAGIC | Un espacio después de comas y alrededor de `=` | `total = a + b` |
# MAGIC | Nombres descriptivos y en inglés | `delay_minutes`, no `x` ni `dm` |
# MAGIC
# MAGIC Las mismas reglas las revisa SonarCloud en cada Pull Request de un proyecto real.

# COMMAND ----------

# DBTITLE 1,Antipatrón: funciona, pero no se entiende
# Antipatrón intencional: ruff y SonarCloud lo marcan
x=15
l=[5,42,0,120]  # noqa: E741
r=[]
for i in l:
    if i>x:r.append(i)  # noqa: E701
print(r)

# COMMAND ----------

# DBTITLE 1,Patrón: el mismo cálculo con PEP 8
ON_TIME_THRESHOLD_MIN = 15
delays_minutes = [5, 42, 0, 120]

late_delays = []
for delay in delays_minutes:
    if delay > ON_TIME_THRESHOLD_MIN:
        late_delays.append(delay)

print(late_delays)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 2 · Tipos de datos y variables
# MAGIC
# MAGIC ## 2.1 Tipos básicos
# MAGIC
# MAGIC | Tipo | Ejemplo | Uso típico en datos |
# MAGIC |---|---|---|
# MAGIC | `str` | `"BOG"` | Códigos, nombres |
# MAGIC | `int` | `42` | Conteos, minutos |
# MAGIC | `float` | `320000.5` | Montos |
# MAGIC | `bool` | `True` | Indicadores |
# MAGIC | `None` | `None` | Dato faltante (equivale a `null`) |

# COMMAND ----------

# DBTITLE 1,Tipos básicos
origin_code = "BOG"
delay_minutes = 42
fare = 320000.5
is_cancelled = False
gate = None

print(type(origin_code), type(delay_minutes), type(fare))
print(type(is_cancelled), type(gate))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Conversiones
# MAGIC
# MAGIC Los datos que llegan de archivos son texto. Antes de operar, se convierten al tipo correcto.

# COMMAND ----------

# DBTITLE 1,Conversiones y f-strings
raw_fare = "320000"
fare = float(raw_fare)
passengers = int("3")

total = fare * passengers
print(f"Total de la reserva: {total:,.0f} COP")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 `None` y los valores "falsos"
# MAGIC
# MAGIC `0`, `""`, `[]` y `None` se evalúan como falsos en un `if`. Un retraso de `0` minutos **es un dato válido**, no un dato faltante.

# COMMAND ----------

# DBTITLE 1,Antipatrón: confunde 0 con dato faltante
delay_minutes = 0
if not delay_minutes:
    print("Sin dato de retraso")  # incorrecto: el dato sí existe

# COMMAND ----------

# DBTITLE 1,Patrón: comparar explícitamente con None
delay_minutes = 0
if delay_minutes is None:
    print("Sin dato de retraso")
else:
    print(f"Retraso: {delay_minutes} min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Listas y diccionarios
# MAGIC
# MAGIC - **Lista (`list`):** secuencia ordenada. Por ejemplo, los retrasos de un día.
# MAGIC - **Diccionario (`dict`):** pares llave-valor. Representa un registro: la llave es la columna y el valor es el dato.

# COMMAND ----------

# DBTITLE 1,Listas
airports = ["BOG", "MDE", "CLO"]
airports.append("CTG")

print(airports[0])  # primer elemento
print(len(airports))  # cantidad de elementos

# COMMAND ----------

# DBTITLE 1,Diccionarios: un registro
flight = {
    "flight_id": "VL0001",
    "origin": "BOG",
    "destination": "MDE",
    "delay_minutes": 5,
}

print(flight["origin"])
print(flight.get("gate"))  # .get devuelve None si la llave no existe
flight["is_on_time"] = flight["delay_minutes"] <= ON_TIME_THRESHOLD_MIN
print(flight)

# COMMAND ----------

# DBTITLE 1,Lista de diccionarios: una tabla pequeña
flights = [
    {"flight_id": "VL0001", "origin": "BOG", "delay_minutes": 5},
    {"flight_id": "VL0002", "origin": "MDE", "delay_minutes": 42},
    {"flight_id": "VL0003", "origin": "BOG", "delay_minutes": None},
]
print(f"{len(flights)} vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 3 · Control de flujo, funciones y errores
# MAGIC
# MAGIC ## 3.1 `if` y `for`

# COMMAND ----------

# DBTITLE 1,Recorrer registros y decidir
for flight in flights:
    delay = flight["delay_minutes"]
    if delay is None:
        status = "sin dato"
    elif delay <= ON_TIME_THRESHOLD_MIN:
        status = "a tiempo"
    else:
        status = "retrasado"
    print(f"{flight['flight_id']}: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Funciones
# MAGIC
# MAGIC Una función agrupa una lógica con un nombre, para reutilizarla. Estándar BPT:
# MAGIC - Nombre en `snake_case` con un verbo (`classify_`, `get_`, `build_`).
# MAGIC - **Docstring** con descripción, `Args` y `Returns`.
# MAGIC - **Type hints:** qué tipo recibe y qué tipo devuelve.

# COMMAND ----------

# DBTITLE 1,Antipatrón: nombre ambiguo, sin documentación
def f(d):
    if d is None:
        return "sin dato"
    if d <= 15:
        return "a tiempo"
    return "retrasado"

# COMMAND ----------

# DBTITLE 1,Patrón: función documentada


def classify_delay(delay_minutes: int | None) -> str:
    """
    Clasifica un vuelo según sus minutos de retraso.

    Args:
        delay_minutes: Minutos de retraso, o None si no hay dato.

    Returns:
        "sin dato", "a tiempo" o "retrasado".
    """
    if delay_minutes is None:
        return "sin dato"
    if delay_minutes <= ON_TIME_THRESHOLD_MIN:
        return "a tiempo"
    return "retrasado"


for flight in flights:
    print(flight["flight_id"], classify_delay(flight["delay_minutes"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Manejo de errores con `try` / `except`
# MAGIC
# MAGIC - Se captura **solo el error que se espera** (por ejemplo, `ValueError` al convertir un texto a número).
# MAGIC - `except: pass` está **prohibido**: oculta errores que después aparecen como datos incorrectos.

# COMMAND ----------

# DBTITLE 1,Antipatrón: silenciar cualquier error
def to_number_bad(value):
    try:
        return float(value)
    except:  # noqa: E722 - antipatrón intencional: captura todo
        pass

# COMMAND ----------

# DBTITLE 1,Patrón: capturar el error esperado


def to_number(value: str) -> float | None:
    """
    Convierte un texto a número.

    Args:
        value: Texto a convertir, por ejemplo "320000".

    Returns:
        El número, o None si el texto no es numérico.
    """
    try:
        return float(value)
    except ValueError:
        return None


print(to_number("320000"))
print(to_number("sin dato"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bloque 4 · pandas: primeros pasos con tablas
# MAGIC
# MAGIC **pandas** trabaja con tablas (DataFrames) en la memoria de una sola máquina. Es útil para datos pequeños; para tablas grandes se usa PySpark (Sesión 02).

# COMMAND ----------

# DBTITLE 1,Crear un DataFrame
import pandas as pd

df_flights = pd.DataFrame(
    {
        "flight_id": ["VL0001", "VL0002", "VL0003", "VL0004"],
        "origin": ["BOG", "MDE", "BOG", "CLO"],
        "delay_minutes": [5, 42, 120, 0],
    }
)
df_flights

# COMMAND ----------

# DBTITLE 1,Seleccionar y filtrar
print(df_flights["origin"])  # una columna

df_late = df_flights[df_flights["delay_minutes"] > ON_TIME_THRESHOLD_MIN]
df_late

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nueva columna: operación vectorizada
# MAGIC
# MAGIC pandas aplica una operación a toda la columna de una vez. Recorrer fila por fila es más lento y más largo.

# COMMAND ----------

# DBTITLE 1,Antipatrón: recorrer fila por fila
is_late_values = []
for _, row in df_flights.iterrows():
    is_late_values.append(row["delay_minutes"] > ON_TIME_THRESHOLD_MIN)

# COMMAND ----------

# DBTITLE 1,Patrón: operación sobre la columna completa
df_flights["is_late"] = df_flights["delay_minutes"] > ON_TIME_THRESHOLD_MIN
df_flights

# COMMAND ----------

# DBTITLE 1,Agrupar y resumir
df_delay_by_origin = (
    df_flights.groupby("origin", as_index=False)
    .agg(
        flights=("flight_id", "count"),
        avg_delay=("delay_minutes", "mean"),
    )
    .sort_values("avg_delay", ascending=False)
)
df_delay_by_origin

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Resolver los ejercicios en [01_python_fundamentals_lab](./01_python_fundamentals_lab). Cada ejercicio tiene una celda de verificación que confirma si la solución es correcta.
