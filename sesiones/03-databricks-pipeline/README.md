# Sesión 03 · Delta Lake y pipeline Medallion (M5 + M6)

**Duración:** 2.5 h (1 h + 1.5 h, con pausa)

**Objetivo:** consolidar Delta Lake y Unity Catalog, y recorrer un pipeline Bronze → Silver → Gold de punta a punta: qué va en cada capa, cómo se orquesta y cómo se consume Gold.

## M5 · Delta Lake y Unity Catalog (1 h)

| Bloque | Minutos | Contenido |
|---|---|---|
| 1 | 0–15 | Unity Catalog; tabla Delta documentada con `COMMENT` |
| 2 | 15–30 | `DESCRIBE HISTORY` y time travel |
| 3 | 30–45 | `append` reejecutado (antipatrón) frente a `MERGE` idempotente; schema enforcement |
| 4 | 45–60 | `RESTORE` y arquitectura Medallion |

| Archivo | Contenido |
|---|---|
| [`05_delta_unity_catalog.py`](05_delta_unity_catalog.py) | Notebook guiado |
| [`05_delta_unity_catalog_lab.py`](05_delta_unity_catalog_lab.py) | 3 ejercicios: crear y documentar `lab_regions`, upsert idempotente, time travel y `RESTORE` |

## M6 · Pipeline Medallion (1.5 h, overview)

Dominio: **operación de vuelos**, con CSV sintéticos de [`data/`](../../data/) y errores de calidad sembrados a propósito. Los notebooks siguen la convención de nombres del estándar: prefijo numérico de capa y sufijos `_nt` (notebook de capa) y `_orch` (orquestación).

| Bloque | Minutos | Notebook | Contenido |
|---|---|---|---|
| 1 | 0–15 | [`pipeline/00_pipeline_setup`](pipeline/00_pipeline_setup.py) · [`pipeline/20_landing_conformance_flights_nt`](pipeline/20_landing_conformance_flights_nt.py) | Diagrama del pipeline; Bronze con Auto Loader y columnas técnicas |
| 2 | 15–40 | [`pipeline/30_conformance_curated_flights_nt`](pipeline/30_conformance_curated_flights_nt.py) | Silver: conformar nombres a inglés, tipar, deduplicar, reglas de calidad, quarantine, MERGE y validación de la tabla |
| 3 | 40–55 | [`pipeline/40_curated_gold_flights_nt`](pipeline/40_curated_gold_flights_nt.py) | Gold: `dim_airport`, `fct_flight` y KPI `agg_route_punctuality` |
| 4 | 55–70 | [`pipeline/99_flights_orch`](pipeline/99_flights_orch.py) | Orquestación en orden y cómo crear el Job con tareas dependientes |
| 5 | 70–90 | [`pipeline/60_gold_consumption`](pipeline/60_gold_consumption.py) · [`pipeline/90_pipeline_audit`](pipeline/90_pipeline_audit.py) | Consultar Gold con SQL y Python; log, conciliación e idempotencia |

**Cómo ejecutarlo:** abrir `pipeline/99_flights_orch` y ejecutarlo completo; luego `60_gold_consumption` y `90_pipeline_audit`. Los conteos esperados están en [`data/README.md`](../../data/README.md#conteos-esperados-después-de-silver).

Solución del lab M5 en [`soluciones/`](soluciones/).

**Entregable:** tabla Delta versionada (M5) y pipeline B → S → G funcional con log de auditoría (M6).

**Guías relacionadas:** [03-delta-unity-catalog.md](../../guia-buenas-practicas/03-delta-unity-catalog.md) · [04-medallion.md](../../guia-buenas-practicas/04-medallion.md)
