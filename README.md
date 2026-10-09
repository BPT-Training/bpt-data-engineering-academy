# BPT Data Engineering Academy

Programa de capacitación en **Python · PySpark · Databricks** y **guía de buenas prácticas de ingeniería de datos de BPT**.

- **Formato workshop:** 5 sesiones virtuales (Sesión 0 de configuración + 4 sesiones), 8 h en total.
- **Uso permanente:** referencia interna de BPT para onboarding, code review y estándares de proyecto.

## Estructura

| Carpeta | Qué contiene |
|---|---|
| [`guia-buenas-practicas/`](guia-buenas-practicas/) | Estándares de desarrollo: Python (PEP 8), PySpark, Delta/Unity Catalog, Medallion, repos, calidad y seguridad, checklist de code review |
| [`sesiones/`](sesiones/) | Material del workshop: notebook guiado, lab con verificaciones automáticas y solución por módulo |
| [`shared/config.yml`](shared/config.yml) | Configuración del curso (catálogo, esquemas, Volume), separada del código |
| [`src/bpt_utils/`](src/bpt_utils/) | Funciones reutilizables (limpieza, calidad, auditoría, escritura, validaciones) |
| [`tests/`](tests/) | Tests pytest de `bpt_utils` |
| [`data/`](data/) | CSV sintéticos de operación de vuelos, con errores de calidad a propósito |
| [`scripts/`](scripts/) | Generador de los CSV (`generar_datos.py`) |
| [`docs/notas-instructor.md`](docs/notas-instructor.md) | Pautas para dictar el workshop |
| [`referencias.md`](referencias.md) | Repositorios y recursos externos evaluados |

## Ruta del workshop

| Sesión | Módulos | Duración | Entregable |
|---|---|---|---|
| [00 · Configuración](sesiones/00-setup/) | M0 · Free Edition, compute, Git folders | 1 h | Entorno validado |
| [01 · Python](sesiones/01-python/) | M1 Python Fundamentals · M2 Python para procesamiento de datos | 2 h | Funciones de limpieza y validación |
| [02 · PySpark](sesiones/02-pyspark/) | M3 PySpark Fundamentals · M4 PySpark Advanced Patterns | 2 h | Último registro válido por entidad |
| [03 · Databricks + Pipeline](sesiones/03-databricks-pipeline/) | M5 Delta / Unity Catalog · M6 Pipeline Bronze → Silver → Gold | 2.5 h | Tabla versionada + pipeline con auditoría |
| [04 · Challenge](sesiones/04-challenge/) | M7 Challenge & Knowledge Check | 0.5 h | Challenge + quiz |

> **Estado:** las sesiones 00 y 01 ya siguen los lineamientos de desarrollo (código en inglés, 79 caracteres, docstrings Google). Las sesiones 02 a 04 se alinean en la siguiente fase.

## Cómo usarlo en Databricks Free Edition

1. Crear la cuenta en <https://www.databricks.com/learn/free-edition>.
2. **Workspace → Create → Git folder** con la URL `https://github.com/Spalacio-bpt/bpt-data-engineering-academy.git`.
3. Abrir [`sesiones/00-setup/00_smoke_test`](sesiones/00-setup/00_smoke_test.py) y ejecutar **Run all** con compute Serverless.

Todo el material corre en **compute serverless**, con Python y SQL, sobre el catálogo `samples` (`samples.tpch`) y los CSV de `data/`. No depende de acceso a internet.

## Convenciones del material

- **Idioma:** el código (variables, funciones, columnas) va en inglés; explicaciones, docstrings y comentarios, en español.
- **Estándar de código:** PEP 8 con líneas de máximo 79 caracteres, docstrings Google-style y las reglas de la [guía](guia-buenas-practicas/).
- **Notebooks:** archivos `.py` en formato fuente de Databricks, con una celda `%md` inicial (propósito, agenda, dependencias) y secciones `DBTITLE`.
- **Cada módulo tiene tres piezas:**
  - `NN_tema.py`: notebook guiado; cada concepto muestra el antipatrón y el patrón.
  - `NN_tema_lab.py`: ejercicios con `# TODO` y celdas de verificación.
  - `soluciones/NN_tema_lab_solucion.py`: solución de referencia.
- **Configuración compartida:** [`sesiones/00-setup/00_config.py`](sesiones/00-setup/00_config.py) lee `shared/config.yml` y habilita `import bpt_utils`; los notebooks lo cargan con `%run`.

## Desarrollo local

```bash
pip install -r requirements-dev.txt   # requiere Java 17 para PySpark
pytest                                # tests de bpt_utils
ruff check . && ruff format --check .
python scripts/generar_datos.py       # regenera data/ (determinístico)
```

## Contribuir

Ramas `feature/<tema>` con Pull Request a `main`. Los cambios a `guia-buenas-practicas/` requieren revisión de un líder técnico. Cada PR se revisa con el [checklist de code review](guia-buenas-practicas/checklist-code-review.md).
