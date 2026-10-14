# Sesión 00 · Configuración de Databricks Free Edition

**Duración:** 1 h

**Objetivo:** que cada participante termine con su workspace operativo, el repositorio clonado y el entorno validado.

**Antes de la sesión:** se envía la guía *Getting Started · Databricks Free Edition*, que incluye un checklist para llegar con el entorno listo.

## Agenda

| Bloque | Minutos | Contenido | Guía Free Edition |
|---|---|---|---|
| 1 | 0–15 | Qué es Databricks Free Edition; qué se puede y qué no se puede hacer | Secciones 2.1 a 2.3 y 5 |
| 2 | 15–30 | Registro, primer acceso y recorrido por el workspace | Secciones 2.4 a 2.6 |
| 3 | 30–45 | Compute serverless, primer notebook, SQL Warehouse; clonar el repositorio y recorrer su organización | Secciones 2.7 a 2.9 y 3.1 a 3.4 |
| 4 | 45–60 | Ejecutar `00_smoke_test`; copia personal de los ejercicios y actualización con Pull; dudas | Secciones 3.5 a 3.7 y 4 |

## Archivos

| Archivo | Contenido |
|---|---|
| [`00_smoke_test.py`](00_smoke_test.py) | Valida Spark y `samples`, crea los esquemas y el Volume del curso, copia los archivos de práctica e importa `bpt_utils`. Cada paso termina con un mensaje de confirmación |
| [`00_config.py`](00_config.py) | Configuración compartida: lee [`shared/config.yml`](../../shared/config.yml), define los nombres de esquemas y tablas, y habilita `import bpt_utils` |

**Entregable:** workspace listo, repositorio clonado y `00_smoke_test` ejecutado sin errores.

**Workspace compartido:** si varias personas usan el mismo workspace, cada una cambia `schema` en `shared/config.yml` por uno propio (por ejemplo `bpt_academy_sp`).

**Guía relacionada:** [05-estructura-repos.md](../../guia-buenas-practicas/05-estructura-repos.md) (configuración y Git).
