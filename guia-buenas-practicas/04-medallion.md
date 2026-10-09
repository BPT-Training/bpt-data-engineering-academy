# 04 · Arquitectura Medallion, calidad y auditoría

Cómo estructura BPT un pipeline de datos de punta a punta.

---

## 1. Contrato por capa

| Capa | Qué contiene | Qué **sí** se hace | Qué **no** se hace |
|---|---|---|---|
| **Bronze** | El dato tal como llegó | Agregar metadatos de ingesta; conservar todo como texto o con el schema de la fuente | Limpiar, filtrar, deduplicar, aplicar reglas de negocio |
| **Silver** | Dato limpio y confiable, a nivel de entidad | Tipar, estandarizar, deduplicar, validar calidad, conformar llaves | Agregar por KPIs de negocio |
| **Gold** | Modelos listos para consumo | Dimensiones, hechos, agregados y KPIs | Lógica de limpieza (eso es Silver) |

**Regla:** si una transformación no respeta el contrato de su capa, está en la capa equivocada.
**Por qué:** si Bronze se limpia, no hay forma de reprocesar desde el dato original. Si Gold limpia, cada tabla Gold limpia distinto.

---

## 2. Ingesta a Bronze

### 2.1 Carga incremental
**Regla:** usa Auto Loader (`cloudFiles`) o `COPY INTO` para leer solo los archivos nuevos.
**Por qué:** los dos llevan registro de qué archivos ya se cargaron, así que la carga es idempotente.

```python
(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("header", "true")
    .load(ruta_origen)
    .withColumns({"_ingest_ts": F.current_timestamp(), "_source_file": F.col("_metadata.file_path")})
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable(tabla_bronze)
)
```

### 2.2 Columnas técnicas obligatorias
| Columna | Contenido |
|---|---|
| `_ingest_ts` | Timestamp de carga |
| `_source_file` | Archivo de origen (`_metadata.file_path`) |
| `_batch_id` | Identificador de la ejecución (opcional si hay tabla de log) |

### 2.3 No perder datos mal formados
**Regla:** Auto Loader guarda en `_rescued_data` lo que no encaja con el schema. No lo descartes: es la señal de que la fuente cambió.

---

## 3. Silver: calidad de datos

### 3.1 Reglas declaradas como datos
**Regla:** las reglas de calidad se definen en una lista (nombre + expresión SQL) y se aplican con una función común, no con `if` dispersos por el notebook.
**Por qué:** las reglas se leen de un vistazo, se reutilizan y el reporte de calidad sale automáticamente.

```python
REGLAS_VUELOS = [
    {"nombre": "vuelo_id_no_nulo", "expresion": "vuelo_id IS NOT NULL"},
    {"nombre": "origen_distinto_destino", "expresion": "origen <> destino"},
    {"nombre": "salida_programada_valida", "expresion": "salida_programada IS NOT NULL"},
]
df_validos, df_invalidos = aplicar_expectativas(df, REGLAS_VUELOS)
```

### 3.2 Quarantine, no descarte silencioso
**Regla:** los registros que fallan una regla van a una tabla `<tabla>_quarantine` con la lista de reglas incumplidas y el timestamp.
**Por qué:** un registro descartado en silencio es un número de negocio que no cuadra y nadie sabe por qué.

### 3.3 Deduplicación en Silver
**Regla:** Silver tiene una fila por llave de negocio. El criterio de desempate (el más reciente, el de mayor prioridad de fuente) se documenta en el notebook.

### 3.4 Silver con MERGE
**Regla:** Silver se actualiza con MERGE por llave de negocio, para que los reprocesos sean idempotentes (ver [03-delta-unity-catalog.md](03-delta-unity-catalog.md#22-cargas-idempotentes)).

---

## 4. Gold: modelado

### 4.1 Modelo dimensional
**Regla:** Gold expone hechos (`fct_`) con llaves a dimensiones (`dim_`) y agregados (`agg_`) para los KPIs de uso frecuente.

### 4.2 Granularidad explícita
**Regla:** el `COMMENT` de cada tabla de hechos declara su granularidad: "un registro por vuelo operado".
**Por qué:** sin granularidad clara, alguien suma algo que no se debe sumar.

### 4.3 KPIs definidos una sola vez
**Regla:** un KPI (por ejemplo, % de puntualidad) se calcula en un único lugar de Gold y el resto lo consume de ahí.

---

## 5. Auditoría y observabilidad

### 5.1 Tabla de log de ejecución
**Regla:** cada paso del pipeline registra una fila con esta estructura:

| Columna | Ejemplo |
|---|---|
| `pipeline` | `vuelos` |
| `capa` | `silver` |
| `tabla_destino` | `workspace.bpt_academy_silver.vuelos` |
| `filas_leidas` | 1 050 |
| `filas_escritas` | 1 000 |
| `filas_rechazadas` | 50 |
| `inicio`, `fin` | timestamps |
| `estado` | `OK` / `ERROR` |
| `mensaje` | detalle del error, si hubo |

### 5.2 Patrón de notebook reutilizable
Todo notebook de pipeline sigue el mismo orden:

1. **Parámetros:** widgets o config.
2. **Lectura:** desde la capa anterior.
3. **Transformación:** con funciones de `src/`.
4. **Validación:** expectativas y quarantine.
5. **Escritura:** idempotente.
6. **Log:** registrar la ejecución, también si falla (`try/except` que registra y relanza).

```python
inicio = datetime.now()
try:
    ...  # lectura, transformación, validación, escritura
    registrar_ejecucion(spark, TABLA_LOG, pipeline="vuelos", capa="silver", estado="OK", ...)
except Exception as e:
    registrar_ejecucion(spark, TABLA_LOG, pipeline="vuelos", capa="silver", estado="ERROR", mensaje=str(e), ...)
    raise
```
