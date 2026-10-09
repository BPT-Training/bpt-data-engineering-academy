# 01 · Python: estilo, funciones y manejo de errores

Aplica a notebooks de Databricks, módulos `.py`, scripts de soporte y funciones reutilizables.

---

## 1. PEP 8 como estándar obligatorio

[PEP 8](https://peps.python.org/pep-0008/) es la guía oficial de estilo de Python. En BPT no es una sugerencia estética: es un requisito porque habilita revisiones de código consistentes, el análisis estático con SonarCloud ([06](06-calidad-y-seguridad.md)) y la mantenibilidad del código productivo.

| Regla | Detalle |
|---|---|
| Indentación | 4 espacios por nivel. Nunca tabulaciones |
| Longitud de línea | Máximo **79 caracteres** en código y **72** en docstrings y comentarios |
| Imports | Tres bloques separados por una línea en blanco: (1) estándar, (2) terceros, (3) locales. Cada bloque en orden alfabético |
| Espacios | Un espacio después de comas y alrededor de `=` en asignaciones. Sin espacio antes del paréntesis de una llamada |
| Líneas en blanco | Dos antes de funciones y clases de primer nivel; una entre métodos |
| Idioma | Un solo idioma por módulo en nombres de variables, funciones y columnas. **BPT usa inglés en el código** y español en docstrings y documentación |

El límite de 79 caracteres es de estilo, no del lenguaje ni de Databricks: el código corre con líneas más largas, pero la revisión y SonarCloud lo marcan.

### Herramienta
`ruff check` y `ruff format` aplican estas reglas automáticamente. La configuración del repositorio está en `pyproject.toml`.

```python
# ✅ Imports en tres bloques, en orden alfabético
# --- Estándar ---
import os
from datetime import datetime

# --- Terceros ---
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# --- Locales ---
from bpt_utils.udf_cleaning import _udf_clean_columns
```

```python
# ❌ Imports mezclados y sin orden
from pyspark.sql import functions as F
import os
from bpt_utils.udf_cleaning import _udf_clean_columns
from datetime import datetime
```

---

## 2. Naming

| Elemento | Convención | Ejemplo |
|---|---|---|
| Variables y funciones | `snake_case` | `flight_count`, `build_route_key` |
| Constantes | `UPPER_SNAKE_CASE` | `MAX_RETRIES`, `ON_TIME_THRESHOLD_MIN` |
| Clases | `PascalCase` | `PipelineConfig` |
| Booleanos | Prefijo `is_`, `has_`, `should_` | `is_incremental`, `has_duplicates` |
| Variables de bucle | Nombre descriptivo | `for flight in flights:` |
| Funciones | Verbo de acción: `get_`, `build_`, `read_`, `write_`, `merge_`, `validate_` | `read_bronze_table` |

**Regla:** nada de abreviaturas opacas ni nombres genéricos.
**Por qué:** en un notebook de 40 celdas, `x` o `df2` obligan a leer todo el código para saber qué contienen.

```python
# ❌
lt = dbutils.widgets.get("nb_load_type")
incremental = lt == "incremental"
for r in rows:
    x = r["col"]

# ✅
nb_load_type = dbutils.widgets.get("nb_load_type")
is_incremental = nb_load_type == "incremental"
for flight in flights:
    origin_code = flight["origin"]
```

---

## 3. Comentarios y docstrings

### 3.1 Los comentarios explican el "por qué"
El código ya muestra qué hace. El comentario explica la decisión.

```python
# ❌ Repite lo que dice el código
retries = retries + 1  # incrementa retries en 1

# ✅ Explica la decisión
# La fuente publica con hasta 2 h de retraso: se reprocesa el día previo
start_date = process_date - timedelta(days=1)
```

**No se deja código comentado** sin una justificación escrita. Para eso existe el historial de Git.

### 3.2 Docstrings en formato Google
Obligatorios en toda función que se importa desde otro módulo o notebook, y en todo módulo compartido. Siempre incluyen una descripción breve, `Args` y `Returns`; incluyen `Raises` cuando hay excepciones esperadas o restricciones de plataforma. Un solo formato por proyecto.

```python
def build_route_key(origin: str, destination: str) -> str:
    """
    Construye la llave de una ruta a partir de sus aeropuertos.

    Args:
        origin: Código IATA de origen.
        destination: Código IATA de destino.

    Returns:
        Llave de la ruta, por ejemplo "BOG-MDE".

    Raises:
        ValueError: Si origen y destino son iguales.
    """
    if origin == destination:
        raise ValueError(f"Ruta inválida: {origin}-{destination}")
    return f"{origin}-{destination}"
```

---

## 4. Funciones

| Regla | Por qué |
|---|---|
| Una sola responsabilidad por función | Se prueba y se reutiliza sola |
| Parámetros explícitos; sin dependencias de variables globales | El resultado depende solo de lo que entra |
| Retornos claros: nunca `None` implícito si se espera un valor | El llamador no recibe sorpresas |
| Type hints en funciones de módulos compartidos | El lector y el IDE saben qué entra y qué sale |
| No modificar los argumentos de entrada | Reejecutar una celda no cambia los datos originales |
| Sin argumentos mutables por defecto (`def f(x=[])`) | El valor por defecto se comparte entre llamadas |

```python
# ❌ Modifica la entrada y depende de una global
def filter_fares(bookings):
    for booking in bookings:
        if booking["fare"] < MIN_FARE:
            bookings.remove(booking)


# ✅ Pura, con parámetros explícitos
def filter_by_min_fare(
    bookings: list[dict], min_fare: float
) -> list[dict]:
    """Devuelve las reservas con tarifa mayor o igual al mínimo."""
    return [booking for booking in bookings if booking["fare"] >= min_fare]
```

**Nada hardcodeado en el cuerpo:** rutas, tablas, ambientes y umbrales llegan como parámetros o desde la configuración ([05 §3](05-estructura-repos.md#3-configuración-y-parámetros)).

---

## 5. Estructuras de datos

| Necesidad | Estructura |
|---|---|
| Comprobar pertenencia (`in`) | `set` |
| Buscar por llave | `dict` |
| Mantener orden | `list` |
| Registro inmutable o llave compuesta | `tuple` |

- **Comprensiones** para transformaciones de una línea; si hay más de una condición, un `for` explícito se lee mejor.
- **Generadores** (`yield`) para procesar secuencias grandes sin cargarlas completas en memoria.

---

## 6. Manejo de errores

### 6.1 Nunca silenciar errores
`except: pass` y `except Exception: pass` están **prohibidos**. Todo error se registra y se re-lanza con contexto.

```python
# ❌ Oculta un error que deja la tabla incompleta
try:
    df_final.write.saveAsTable(target_table)
except:
    pass
```

### 6.2 Recuperables vs. fatales
- **Recuperables:** permiten reintentar o continuar (por ejemplo, liberar una caché). Se registran y el flujo sigue.
- **Fatales:** comprometen la consistencia de los datos. Detienen el pipeline (*fail fast*).

### 6.3 Patrones permitidos

**Re-lanzar con contexto** (errores que deben detener el flujo):

```python
try:
    df_dedup = _udf_drop_duplicates_by_key(df_clean, keys, order_by)
except Exception as error:
    raise RuntimeError(
        f"Error al deduplicar {table_name} por {keys}: {error}"
    ) from error
```

**Retornar una estructura de error** (helpers de consulta donde el llamador decide):

```python
except Exception as error:
    return {
        "error": str(error),
        "message": "Verificar permisos sobre la tabla de origen.",
    }
```

**Registrar y continuar** (solo operaciones secundarias):

```python
try:
    spark.sql("UNCACHE TABLE IF EXISTS tmp_lookup")
except Exception as error:
    logger.warning("No se pudo liberar la caché: %s", error)
```

### 6.4 Mensajes accionables
Un mensaje de error dice **qué falló**, **en qué paso** y **qué revisar** (permisos, metadata, configuración).

### 6.5 Excepciones específicas cuando se conocen
Si el error esperado es concreto, se captura solo ese tipo.

```python
try:
    flight_date = datetime.strptime(raw_date, "%Y-%m-%d")
except ValueError:
    flight_date = None
```

---

## 7. Logging
En código reutilizable se usa `logging` con nivel (`info`, `warning`, `error`), no `print`. **Nunca se registran credenciales ni datos personales** ([06 §4](06-calidad-y-seguridad.md#4-datos-sensibles-en-logs)).

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Filas leídas de %s: %s", source_table, row_count)
```
