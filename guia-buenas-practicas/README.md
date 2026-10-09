# Guía de buenas prácticas · Ingeniería de datos BPT

Estándares que BPT aplica en proyectos de datos sobre Python, PySpark y Databricks, alineados con los lineamientos de desarrollo de código de BPT. Sirve para:

- **Onboarding:** lectura obligatoria para personas nuevas en el equipo de datos.
- **Code review:** referencia de lo que se revisa en cada Pull Request.
- **Capacitaciones:** cada sesión del curso enlaza las secciones que aplica.

| Documento | Tema | Sesión del curso |
|---|---|---|
| [01-python.md](01-python.md) | PEP 8, naming, docstrings, funciones y manejo de errores | 01 |
| [02-pyspark.md](02-pyspark.md) | Alias, librerías, naming de DataFrames, UDF, patrones y rendimiento | 02 |
| [03-delta-unity-catalog.md](03-delta-unity-catalog.md) | Unity Catalog, escritura idempotente, historial y permisos | 03 (M5) |
| [04-medallion.md](04-medallion.md) | Capas, calidad de datos, validación de tablas y auditoría | 03 (M6) |
| [05-estructura-repos.md](05-estructura-repos.md) | Módulos, `config.yml`, parámetros `wf_*`/`nb_*`, notebooks, Git y pruebas | Transversal |
| [06-calidad-y-seguridad.md](06-calidad-y-seguridad.md) | SonarCloud, Fluid Attacks, secretos, `eval`/`exec`, inyección SQL y logs | Transversal |
| [checklist-code-review.md](checklist-code-review.md) | Checklist para Pull Requests | Transversal |

**Formato de cada regla:** la regla, el porqué y un ejemplo ❌ antipatrón / ✅ patrón.

**Idioma:** el código (variables, funciones, columnas) va en inglés; docstrings, comentarios y documentación, en español.

**Cambios a la guía:** por Pull Request, con revisión de un líder técnico. Si una regla no aplica en un proyecto, la excepción se documenta en el README de ese proyecto.
