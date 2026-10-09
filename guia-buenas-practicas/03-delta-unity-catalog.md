# 03 · Delta Lake y Unity Catalog

Reglas para crear, escribir y mantener tablas en Databricks.

---

## 1. Unity Catalog: naming y organización

### 1.1 Nombres de tres niveles
**Regla:** referencia siempre las tablas como `catalogo.esquema.tabla`, nunca solo `tabla`.
**Por qué:** el catálogo y el esquema "actuales" dependen de la sesión; un nombre corto puede apuntar a otra tabla.

### 1.2 Convención de catálogos y esquemas
**Regla BPT:**

| Nivel | Convención | Ejemplo |
|---|---|---|
| Catálogo | Ambiente (+ dominio si aplica) | `dev`, `prod`, `prod_operaciones` |
| Esquema | Capa Medallion | `bronze`, `silver`, `gold` |
| Tabla | `snake_case`; en Gold con prefijos `dim_`, `fct_`, `agg_` | `silver.vuelos`, `gold.fct_vuelo` |

En Free Edition trabajamos en el catálogo `workspace`, así que los esquemas llevan prefijo (`bpt_academy_bronze`).

### 1.3 Documentar las tablas
**Regla:** toda tabla Silver y Gold lleva `COMMENT` en la tabla y en sus columnas clave.

```sql
COMMENT ON TABLE gold.fct_vuelo IS 'Un registro por vuelo operado. Fuente: silver.vuelos.';
ALTER TABLE gold.fct_vuelo ALTER COLUMN minutos_retraso COMMENT 'Salida real - programada, en minutos';
```

### 1.4 Volumes para archivos
**Regla:** los archivos (CSV, JSON, Parquet de aterrizaje) van en Volumes (`/Volumes/catalogo/esquema/volume/...`), no en DBFS ni en rutas de montaje.
**Por qué:** los Volumes están gobernados por Unity Catalog: permisos, lineage y auditoría.

### 1.5 Tablas administradas por defecto
**Regla:** crea tablas *managed* salvo que haya una razón concreta para una tabla externa.
**Por qué:** Databricks gestiona el almacenamiento, el mantenimiento predictivo y el borrado.

---

## 2. Escritura

### 2.1 Elegir el modo de escritura correcto
| Situación | Modo |
|---|---|
| Carga completa de una tabla pequeña | `overwrite` |
| Eventos nuevos, sin actualizaciones (logs, Bronze) | `append` |
| Registros que se actualizan (clientes, reservas) | `MERGE` |
| Reproceso de un periodo acotado | `overwrite` + `replaceWhere` |

### 2.2 Cargas idempotentes
**Regla:** reejecutar un proceso con la misma entrada debe dejar la tabla igual, sin duplicados.
**Por qué:** los jobs fallan y se reintentan. Un `append` reejecutado duplica los datos.

```python
from delta.tables import DeltaTable

(
    DeltaTable.forName(spark, "dev.silver.reservas").alias("t")
    .merge(df_nuevas.alias("s"), "t.reserva_id = s.reserva_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### 2.3 La fuente del MERGE sin duplicados
**Regla:** deduplica la fuente por la llave del MERGE antes de ejecutarlo.
**Por qué:** si dos filas de la fuente coinciden con la misma fila destino, el MERGE falla.

### 2.4 Schema enforcement por defecto
**Regla:** deja que Delta rechace escrituras con un schema distinto. Usa `mergeSchema` solo de forma deliberada y revisada.
**Por qué:** un cambio de schema accidental en la fuente no debe alterar una tabla de consumo.

```python
# Solo cuando la evolución del schema es intencional
df.write.option("mergeSchema", "true").mode("append").saveAsTable(tabla)
```

---

## 3. Historial y recuperación

### 3.1 `DESCRIBE HISTORY` como fuente de verdad
**Regla:** ante la pregunta "¿qué le pasó a esta tabla?", la respuesta empieza por `DESCRIBE HISTORY`: quién, cuándo, qué operación y cuántas filas.

### 3.2 Time travel para auditar y comparar
```sql
SELECT * FROM dev.silver.reservas VERSION AS OF 3;
SELECT * FROM dev.silver.reservas TIMESTAMP AS OF '2026-10-28T10:00:00';
```

### 3.3 `RESTORE` ante una carga errónea
**Regla:** si una carga dañó la tabla, restaura la versión anterior en vez de "arreglar a mano".

```sql
RESTORE TABLE dev.silver.reservas TO VERSION AS OF 5;
```

---

## 4. Mantenimiento

### 4.1 Liquid clustering en tablas nuevas
**Regla:** para tablas grandes nuevas usa `CLUSTER BY (col1, col2)` con las columnas que más se filtran, en lugar de `PARTITIONED BY`.
**Por qué:** se adapta a los cambios y evita el problema de las particiones pequeñas.

### 4.2 `OPTIMIZE` y `VACUUM`
- `OPTIMIZE` compacta archivos pequeños; en tablas managed con mantenimiento predictivo se ejecuta solo.
- `VACUUM` borra archivos que ya no referencia ninguna versión reciente. Después de un `VACUUM` no se puede hacer time travel más allá del periodo de retención (7 días por defecto).

---

## 5. Permisos

### 5.1 Permisos a grupos
**Regla:** otorga permisos a grupos (`data_engineers`, `analistas_operaciones`), nunca a usuarios individuales.

### 5.2 Mínimo privilegio por capa
| Grupo | Bronze | Silver | Gold |
|---|---|---|---|
| Ingeniería de datos | Lectura y escritura | Lectura y escritura | Lectura y escritura |
| Analistas | — | Lectura | Lectura |
| Consumidores de negocio | — | — | Lectura |

```sql
GRANT SELECT ON SCHEMA prod.gold TO `analistas_operaciones`;
```
