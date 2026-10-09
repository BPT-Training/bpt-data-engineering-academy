# Sesión 00 · Configuración de Databricks Free Edition

**Duración:** 1 h

**Objetivo:** que cada participante termine con su workspace operativo, el repositorio clonado y el entorno validado.

**Antes de la sesión:** se envía la guía *Getting Started · Databricks Free Edition*, que incluye un checklist para llegar con el entorno listo.

## Agenda

| Bloque | Minutos | Contenido | Guía Free Edition |
|---|---|---|---|
| 1 | 0–15 | Qué es Databricks Free Edition; qué se puede y qué no se puede hacer | Secciones 2 y 9 |
| 2 | 15–30 | Registro, primer acceso y recorrido por el workspace | Secciones 3 y 4 |
| 3 | 30–45 | Compute serverless y SQL Warehouse; clonar el repositorio como Git folder | Secciones 5 y 6 |
| 4 | 45–60 | Ejecutar `00_smoke_test` y resolver dudas | Secciones 7 y 8 |

## Archivos

| Archivo | Contenido |
|---|---|
| [`00_smoke_test.py`](00_smoke_test.py) | Valida Spark y `samples`, crea los esquemas y el Volume del curso, copia los archivos de práctica e importa `bpt_utils`. Cada paso termina con un mensaje de confirmación |
| [`00_config.py`](00_config.py) | Configuración compartida: lee [`shared/config.yml`](../../shared/config.yml), define los nombres de esquemas y tablas, y habilita `import bpt_utils` |

**Entregable:** workspace listo, repositorio clonado y `00_smoke_test` ejecutado sin errores.

**Workspace compartido:** si varias personas usan el mismo workspace, cada una cambia `schema` en `shared/config.yml` por uno propio (por ejemplo `bpt_academy_sp`).

**Guía relacionada:** [05-estructura-repos.md](../../guia-buenas-practicas/05-estructura-repos.md) (configuración y Git).
