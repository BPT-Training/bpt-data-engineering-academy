# 04 · Arquitectura Medallion, calidad de datos y auditoría

Cómo estructura BPT un pipeline de datos de punta a punta.

---

## 1. Capas y equivalencias

La arquitectura Medallion organiza los datos en capas de calidad creciente. Algunos proyectos usan nombres propios para las mismas capas:

| Medallion | Nombre en proyectos BPT | Prefijo de notebook | Contenido |
|---|---|---|---|
| — | Landing | `10_` | Archivos tal como llegan de la fuente |
| Bronze | Conformance | `20_` | Dato crudo en tablas Delta + metadatos de ingesta |
| Silver | Curated | `30_` | Dato limpio, tipado, deduplicado y validado |
| Gold | Gold | `40_` | Modelos para consumo: dimensiones, hechos, KPIs |
| — | Orquestación | `99_` | Notebooks que coordinan la ejecución |

## 2. Contrato por capa

| Capa | Qué **sí** se hace | Qué **no** se hace |
|---|---|---|
| Bronze | Conservar el dato original y agregar metadatos de ingesta | Limpiar, filtrar, deduplicar o aplicar reglas de negocio |
| Silver | Tipar, estandarizar, deduplicar, validar calidad y conformar llaves | Agregar por KPIs de negocio |
| Gold | Dimensiones, hechos, agregados y KPIs | Lógica de limpieza (eso es Silver) |

**Regla:** si una transformación no respeta el contrato de su capa, está en la capa equivocada.
**Por qué:** si se limpia en Bronze, no se puede reprocesar desde el dato original; si se limpia en Gold, cada tabla Gold limpia distinto.

---

## 3. Bronze: ingesta

- **Carga incremental** con Auto Loader (`cloudFiles`) o `COPY INTO`: los dos registran qué archivos ya se cargaron, así que la carga es idempotente.
- **Columnas técnicas obligatorias:** `_ingest_ts` (momento de carga) y `_source_file` (archivo de origen, desde `_metadata.file_path`).
- **No perder datos mal formados:** Auto Loader guarda en `_rescued_data` lo que no encaja con el schema. Si aparece con datos, la fuente cambió.

---

## 4. Silver: calidad de datos

### 4.1 Reglas declaradas como datos
Las reglas se definen en una lista (nombre y expresión SQL) y se aplican con una función común, no con `if` dispersos por el notebook.

```python
FLIGHT_RULES = [
    {"name": "flight_id_not_null", "expression": "flight_id IS NOT NULL"},
    {"name": "origin_not_destination", "expression": "origin <> destination"},
]
df_valid, df_invalid = _udf_apply_expectations(df_dedup, FLIGHT_RULES)
```

### 4.2 Quarantine, no descarte silencioso
Los registros que fallan una regla van a `<tabla>_quarantine` con la lista de reglas incumplidas y la fecha. Un registro descartado en silencio es un número de negocio que no cuadra sin explicación.

### 4.3 Deduplicación y MERGE
Silver tiene una fila por llave de negocio, con un criterio de desempate documentado, y se actualiza con MERGE para que los reprocesos sean idempotentes ([03 §2.2](03-delta-unity-catalog.md#22-cargas-idempotentes)).

---

## 5. Gold: modelado

- **Modelo dimensional:** hechos (`fct_`) con llaves a dimensiones (`dim_`), y agregados (`agg_`) para los KPIs de uso frecuente.
- **Granularidad explícita:** el `COMMENT` de cada tabla de hechos la declara, por ejemplo "un registro por vuelo operado".
- **KPIs definidos una sola vez:** se calculan en un único lugar de Gold y el resto los consume desde ahí.

---

## 6. Validación de tablas resultado

Toda tabla que produce un pipeline pasa, como mínimo, estas validaciones. Se implementan con `assert` en el mismo notebook: **si una validación falla, el job falla**.

| Prioridad | Validación | Función en `bpt_utils` |
|---|---|---|
| 1 | Esquema esperado (columnas mínimas) | `validate_schema` |
| 2 | Tabla no vacía | `validate_not_empty` |
| 3 | Unicidad de la llave de negocio | `validate_unique_key` |
| 4 | Sin nulos en columnas críticas | `validate_no_nulls` |

```python
validate_schema(df_final, {"flight_id", "origin"}, target_table)
validate_not_empty(df_final, target_table)
validate_unique_key(df_final, ["flight_id"], target_table)
validate_no_nulls(df_final, ["flight_id", "origin"], target_table)
```

**Niveles de adopción:**
1. Validaciones en el mismo notebook del pipeline (mínimo obligatorio).
2. Notebook de validación separado para pipelines complejos, ejecutado después del pipeline.
3. Pruebas unitarias con pytest solo para funciones críticas de módulos compartidos.

**Smoke run obligatorio** antes de promover a QA: ejecutar en DEV con un volumen reducido (pocos días, una tabla) y dejar registro de la ejecución.

```python
# ❌ La validación no detiene el job y se escribe una tabla vacía
if df_final.count() == 0:
    print("Advertencia: DataFrame vacío")

# ❌ Validación atada a un valor fijo: falla cuando cambia la fuente
assert df_final.count() == 15234
```

---

## 7. Auditoría

### 7.1 Tabla de log de ejecución
Cada paso del pipeline registra una fila, también cuando falla:

| Columna | Ejemplo |
|---|---|
| `pipeline`, `layer`, `target_table` | `flights`, `silver`, `workspace.bpt_academy_silver.flights` |
| `rows_read`, `rows_written`, `rows_rejected` | 1050, 1000, 50 |
| `started_at`, `finished_at` | timestamps |
| `status`, `message` | `OK` / `ERROR` y el detalle del error |

### 7.2 Estructura estándar de un notebook de pipeline
Las secciones se delimitan con `# DBTITLE 1,<Título>` y el notebook empieza con una celda `%md` que documenta propósito, parámetros (`nb_*`) y dependencias ([05 §4](05-estructura-repos.md#4-notebooks)).

1. **Imports**
2. **Configuración:** parámetros y nombres de tablas
3. **Lectura:** desde la capa anterior
4. **Reglas de negocio:** transformaciones con funciones de `src/`
5. **Validación:** reglas de calidad y validaciones de la tabla resultado
6. **Persistencia:** escritura idempotente
7. **Log:** registro de la ejecución, también si falla (`try/except` que registra y re-lanza)

```python
started_at = datetime.now()
try:
    ...  # lectura, reglas de negocio, validación y persistencia
    log_execution(
        spark, LOG_TABLE, pipeline="flights", layer="silver",
        target_table=target_table, started_at=started_at, status="OK",
    )
except Exception as error:
    log_execution(
        spark, LOG_TABLE, pipeline="flights", layer="silver",
        target_table=target_table, started_at=started_at,
        status="ERROR", message=str(error)[:1000],
    )
    raise
```
