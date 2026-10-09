# Sesión 03 · Databricks Fundamentals + Pipeline End-to-End (M5 + M6)

**Duración:** 2.5 h (1 h + 1.5 h, con break)

**Objetivo:** consolidar Delta Lake y Unity Catalog, e integrarlo todo en un pipeline Bronze → Silver → Gold.

## M5 · Databricks Fundamentals (1 h)
| Archivo | Contenido |
|---|---|
| [`05_delta_unity_catalog.py`](05_delta_unity_catalog.py) | Jerarquía de Unity Catalog, tabla Delta documentada, `DESCRIBE HISTORY`, time travel, MERGE idempotente, schema enforcement, `RESTORE`, introducción a Medallion |
| [`05_delta_unity_catalog_lab.py`](05_delta_unity_catalog_lab.py) | 3 ejercicios: crear y documentar `lab_regiones`, upsert idempotente, time travel + RESTORE |

## M6 · Pipeline End-to-End (1.5 h)
Dominio: **operación de vuelos**. Son CSV sintéticos de [`data/`](../../data/) con errores de calidad a propósito.

| Archivo | Contenido |
|---|---|
| [`06_0_setup_pipeline.py`](06_0_setup_pipeline.py) | Diagrama del pipeline, tabla de log y widget para reiniciar todo |
| [`06_1_bronze.py`](06_1_bronze.py) | Auto Loader (`availableNow`) + `_ingest_ts` / `_source_file`; idempotente por checkpoint |
| [`06_2_silver.py`](06_2_silver.py) | Limpieza, tipado (dos formatos de fecha), dedup, reglas declarativas, integridad referencial, quarantine y MERGE |
| [`06_3_gold.py`](06_3_gold.py) | `dim_aeropuerto`, `fct_vuelo` (con validación de cardinalidad) y KPI `agg_puntualidad_ruta` |
| [`06_4_auditoria.py`](06_4_auditoria.py) | Log de ejecuciones, conciliación Bronze = Silver + quarantine + duplicados, motivos de rechazo, historial, prueba de idempotencia |

Orden de ejecución: `06_0` → `06_1` → `06_2` → `06_3` → `06_4`. Conteos esperados en [`data/README.md`](../../data/README.md#conteos-esperados-después-de-silver).

Solución del lab M5 en [`soluciones/`](soluciones/).

**Entregable:** tabla Delta versionada (M5) y pipeline B → S → G funcional con log de auditoría (M6).

**Guías relacionadas:** [03-delta-unity-catalog.md](../../guia-buenas-practicas/03-delta-unity-catalog.md) · [04-medallion.md](../../guia-buenas-practicas/04-medallion.md)
