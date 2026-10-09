# 01 · Python para ingeniería de datos

Reglas para el código Python que acompaña a los pipelines: utilidades, validaciones, orquestación y procesamiento de datos pequeños.

---

## 1. Estilo y legibilidad

### 1.1 PEP 8 con formateo automático
**Regla:** todo el código pasa por `ruff format` y `ruff check` antes del PR.
**Por qué:** las discusiones de estilo en el code review se pierden; un formateador las elimina.

### 1.2 Nombres que describen el dato
**Regla:** `snake_case` para variables y funciones, `UPPER_CASE` para constantes, y nombres que digan qué contiene la variable.
**Por qué:** en un notebook de 40 celdas, `df2` no dice nada; `df_vuelos_limpios` sí.

```python
# ❌
df2 = df.filter(...)
x = 0.15

# ✅
df_vuelos_confirmados = df_vuelos.filter(...)
TASA_IVA = 0.15
```

### 1.3 Type hints y docstrings en funciones públicas
**Regla:** toda función que se reutiliza lleva type hints y un docstring corto.
**Por qué:** el lector sabe qué entra y qué sale sin leer el cuerpo, y el IDE detecta errores.

```python
# ❌
def limpiar(c):
    return c.strip().upper()

# ✅
def normalizar_codigo(codigo: str) -> str:
    """Quita espacios y pasa a mayúscula un código (IATA, país, etc.)."""
    return codigo.strip().upper()
```

---

## 2. Funciones

### 2.1 Funciones pequeñas y puras
**Regla:** una función hace una sola cosa, no modifica sus argumentos y no depende de variables globales.
**Por qué:** se prueba sola, se reutiliza y no produce efectos sorpresa al reejecutar celdas.

```python
# ❌ modifica la lista original y depende de una global
def filtrar(registros):
    for r in registros:
        if r["tarifa"] < MINIMO:
            registros.remove(r)

# ✅
def filtrar_por_tarifa(registros: list[dict], minimo: float) -> list[dict]:
    return [r for r in registros if r["tarifa"] >= minimo]
```

### 2.2 Nada hardcodeado en el cuerpo
**Regla:** rutas, nombres de tabla, fechas de corte y umbrales entran como parámetros o vienen de la configuración.
**Por qué:** el mismo código corre en dev, qa y prod sin editarlo.

```python
# ❌
def cargar():
    return spark.table("prod.silver.vuelos")

# ✅
def cargar_vuelos(spark, tabla: str):
    return spark.table(tabla)
```

### 2.3 Sin argumentos mutables por defecto
**Regla:** usa `None` como valor por defecto y crea la lista o el dict dentro de la función.
**Por qué:** el valor por defecto se crea una sola vez y se comparte entre llamadas.

```python
# ❌
def agregar_error(error, errores=[]):
    errores.append(error)
    return errores

# ✅
def agregar_error(error: str, errores: list[str] | None = None) -> list[str]:
    errores = [] if errores is None else list(errores)
    errores.append(error)
    return errores
```

---

## 3. Estructuras de datos

### 3.1 La estructura correcta para cada operación
**Regla:** `set` para comprobar pertenencia, `dict` para búsquedas por llave, `list` cuando importa el orden.
**Por qué:** `x in lista` es O(n); `x in conjunto` es O(1). Con miles de registros se nota.

```python
# ❌
codigos_validos = ["BOG", "MDE", "CLO", ...]
validos = [r for r in registros if r["origen"] in codigos_validos]

# ✅
CODIGOS_VALIDOS = {"BOG", "MDE", "CLO"}
validos = [r for r in registros if r["origen"] in CODIGOS_VALIDOS]
```

### 3.2 Comprensiones sí, pero legibles
**Regla:** usa comprensiones para transformaciones de una línea; si necesitas más de un `if` o anidar, usa un bucle.
**Por qué:** una comprensión de tres líneas con condiciones anidadas es más difícil de leer que un `for`.

### 3.3 Generadores para flujos grandes
**Regla:** si procesas un archivo o una secuencia larga registro a registro, usa un generador (`yield`).
**Por qué:** no carga todo en memoria y permite procesar por lotes.

```python
def en_lotes(registros: Iterable[dict], tamano: int) -> Iterator[list[dict]]:
    lote = []
    for r in registros:
        lote.append(r)
        if len(lote) == tamano:
            yield lote
            lote = []
    if lote:
        yield lote
```

### 3.4 `dataclass` para configuración
**Regla:** agrupa los parámetros de un proceso en una `dataclass(frozen=True)`, no en variables sueltas.

```python
@dataclass(frozen=True)
class ConfigPipeline:
    catalogo: str
    esquema: str
    ruta_volume: str
```

---

## 4. Errores y logging

### 4.1 Excepciones específicas
**Regla:** captura la excepción concreta que esperas. Nunca uses `except:` ni `except Exception: pass`.
**Por qué:** un `except` genérico también esconde errores de programación (un `KeyError` por un typo, por ejemplo).

```python
# ❌
try:
    fecha = datetime.strptime(texto, "%Y-%m-%d")
except:
    fecha = None

# ✅
try:
    fecha = datetime.strptime(texto, "%Y-%m-%d")
except ValueError:
    fecha = None
```

### 4.2 Fallar pronto y con contexto
**Regla:** valida las entradas al inicio y lanza errores que digan qué dato falló y por qué.

```python
if tarifa < 0:
    raise ValueError(f"Tarifa negativa en reserva {reserva_id}: {tarifa}")
```

### 4.3 `logging`, no `print`
**Regla:** en código reutilizable usa `logging` con nivel (`info`, `warning`, `error`).
**Por qué:** se puede filtrar, lleva timestamp y queda en los logs del job.

```python
logger = logging.getLogger(__name__)
logger.info("Filas leídas: %s", n_filas)
```

---

## 5. pandas vs. Spark

### 5.1 Elegir según el volumen
**Regla:** pandas para datos que caben con holgura en la memoria del driver (orden de cientos de MB o menos). Spark para todo lo demás, y para todo lo que va a producción en Databricks.
**Por qué:** pandas corre en un solo nodo; Spark distribuye.

### 5.2 `toPandas()` solo sobre datos acotados
**Regla:** antes de `toPandas()` siempre va un `limit`, un filtro fuerte o una agregación.
**Por qué:** trae todo al driver; con una tabla grande el cluster se cae.

```python
# ❌
pdf = spark.table("samples.tpch.lineitem").toPandas()

# ✅
pdf = spark.table("samples.tpch.orders").limit(1000).toPandas()
```

### 5.3 pandas API on Spark como puente
**Regla:** para migrar código pandas existente, `import pyspark.pandas as ps` permite mantener la sintaxis y ejecutar distribuido. Para código nuevo, escribe PySpark directamente.

---

## 6. Configuración y secretos

### 6.1 Sin credenciales en el código
**Regla:** tokens, passwords y cadenas de conexión se leen con `dbutils.secrets.get(scope, key)`.
**Por qué:** los notebooks se versionan en Git; un secreto en el repo queda expuesto para siempre.

### 6.2 Parámetros con widgets
**Regla:** los notebooks parametrizables usan `dbutils.widgets` y los jobs pasan los valores.
**Por qué:** así no hay que editar celdas para cambiar de ambiente o de fecha.

```python
dbutils.widgets.text("fecha_proceso", "")
fecha_proceso = dbutils.widgets.get("fecha_proceso")
```
