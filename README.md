# BPT Data Engineering Academy

Programa de capacitación en **Python · PySpark · Databricks** (nivel intermedio) y **guía interna de buenas prácticas de BPT**.

- **Formato workshop:** 5 sesiones virtuales (Sesión 0 de configuración + 4 sesiones), 8 h en total.
- **Uso permanente:** referencia interna de BPT para ingeniería de datos (onboarding, code review, estándares de proyecto).

## Estructura

| Carpeta | Qué contiene |
|---|---|
| [`guia-buenas-practicas/`](guia-buenas-practicas/) | Estándares BPT: Python, PySpark, Delta/Unity Catalog, Medallion, repos, checklist de code review |
| [`sesiones/`](sesiones/) | Material del workshop: notebook guiado + lab con asserts + solución por módulo |
| [`src/bpt_utils/`](src/bpt_utils/) | Funciones reutilizables (limpieza, calidad, auditoría, escritura) que usan los notebooks |
| [`tests/`](tests/) | Tests pytest de `bpt_utils` |
| [`data/`](data/) | CSV sintéticos de operación de vuelos, con errores de calidad a propósito |
| [`scripts/`](scripts/) | Generador de los CSV (`generar_datos.py`) |
| [`referencias.md`](referencias.md) | Repos y recursos externos evaluados |

## Ruta del workshop

| Sesión | Módulos | Duración | Entregable |
|---|---|---|---|
| [00 · Setup](sesiones/00-setup/) | M0 · Free Edition, Git folders, serverless | 1 h | Smoke test en verde |
| [01 · Python](sesiones/01-python/) | M1 Python Fundamentals · M2 Python for Data Processing | 2 h | Funciones de limpieza y validación |
| [02 · PySpark](sesiones/02-pyspark/) | M3 PySpark Fundamentals · M4 PySpark Advanced Patterns | 2 h | Último registro válido por entidad |
| [03 · Databricks + Pipeline](sesiones/03-databricks-pipeline/) | M5 Delta / Unity Catalog · M6 Pipeline Bronze → Silver → Gold | 2.5 h | Tabla versionada + pipeline con auditoría |
| [04 · Challenge](sesiones/04-challenge/) | M7 Challenge & Knowledge Check | 0.5 h | Challenge + quiz |

## Cómo usarlo en Databricks Free Edition

1. Crear cuenta en <https://www.databricks.com/learn/free-edition>.
2. **Workspace → Create → Git folder** y pegar la URL de este repo.
3. Abrir [`sesiones/00-setup/00_smoke_test`](sesiones/00-setup/00_smoke_test.py) y ejecutar **Run all** con compute Serverless.

Todo el material corre en **compute serverless**, solo con Python y SQL, sobre el catálogo `samples` (`samples.tpch`) y los CSV de `data/`. El smoke test copia esos CSV a un Volume. No depende de acceso a internet.

## Convenciones del material

- **Idioma:** español, con los términos técnicos en inglés (DataFrame, join, window function…).
- **Notebooks:** archivos `.py` en formato fuente de Databricks (`# Databricks notebook source`), para tener diffs legibles.
- **Cada módulo tiene tres piezas:**
  - `NN_tema.py`: notebook guiado, se recorre en vivo.
  - `NN_tema_lab.py`: ejercicios con `# TODO` y `assert` que terminan en ✅.
  - `soluciones/NN_tema_lab_solucion.py`: solución de referencia.
- **Configuración compartida:** [`sesiones/00-setup/00_config.py`](sesiones/00-setup/00_config.py), cargada con `%run`. Define el catálogo, los esquemas y el Volume, y habilita `import bpt_utils`.

## Desarrollo local

```bash
pip install -r requirements-dev.txt   # requiere Java 17 para PySpark
pytest                                # tests de bpt_utils
ruff check . && ruff format --check .
python scripts/generar_datos.py       # regenera data/ (determinístico)
```


## Contribuir

Ramas `feature/<tema>` con PR a `main`. Los cambios a `guia-buenas-practicas/` requieren revisión de un líder técnico. Usa el [checklist de code review](guia-buenas-practicas/checklist-code-review.md).
