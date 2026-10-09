# Sesión 00 · Setup Databricks Free Edition

**Duración:** 1 h

**Objetivo:** que todos queden con el entorno operativo y el repo clonado.

## Agenda
| Min | Tema |
|---|---|
| 0–10 | Registro y verificación de cuenta en Free Edition |
| 10–25 | Tour: workspace, notebooks, compute serverless, Catalog Explorer, `samples` |
| 25–40 | Workspace → Create → Git folder con este repo |
| 40–55 | Ejecutar `00_smoke_test` (crea esquemas y Volume, y copia los CSV) |
| 55–60 | Dudas |

## Archivos
| Archivo | Contenido |
|---|---|
| [`00_smoke_test.py`](00_smoke_test.py) | Valida Spark y `samples`, crea los esquemas `bpt_academy*` y el Volume `raw`, copia los CSV de `data/` e importa `bpt_utils`. Termina con ✅ |
| [`00_config.py`](00_config.py) | Configuración compartida que todos los notebooks cargan con `%run`: catálogo, esquemas, Volume, `sys.path` a `src/` |

**Entregable:** workspace listo, repo clonado y smoke test en verde.

**Workspace compartido:** si varias personas usan el mismo workspace, cada una cambia `ESQUEMA` en `00_config.py` por uno propio (ej. `bpt_academy_sp`).

**Guía relacionada:** [05-estructura-repos.md](../../guia-buenas-practicas/05-estructura-repos.md) (Git folders).
