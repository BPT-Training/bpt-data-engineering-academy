# Guía de buenas prácticas · Ingeniería de datos BPT

Estándares que BPT aplica en proyectos de datos sobre Python, PySpark y Databricks. Sirve para:

- **Onboarding:** lectura obligatoria para personas nuevas en el equipo de datos.
- **Code review:** checklist de lo que se revisa en un PR.
- **Workshops:** cada sesión del programa enlaza las secciones que aplica.

| Documento | Tema | Sesión del workshop |
|---|---|---|
| [01-python.md](01-python.md) | Python para ingeniería de datos | 01 |
| [02-pyspark.md](02-pyspark.md) | PySpark: rendimiento y legibilidad | 02 |
| [03-delta-unity-catalog.md](03-delta-unity-catalog.md) | Delta Lake y Unity Catalog | 03 (M5) |
| [04-medallion.md](04-medallion.md) | Arquitectura Medallion, calidad y auditoría | 03 (M6) |
| [05-estructura-repos.md](05-estructura-repos.md) | Organización de repos, notebooks y tests | Transversal |
| [checklist-code-review.md](checklist-code-review.md) | Checklist resumida para PRs | Transversal |

**Formato de cada regla:**
- **Regla:** qué hacer, en una línea.
- **Por qué:** la razón técnica.
- **Ejemplo:** ❌ el caso incorrecto y ✅ el correcto, cuando aplica.

**Cambios a la guía:** por PR, con revisión de un líder técnico. Si una regla no aplica en un proyecto, se documenta la excepción en el README de ese proyecto.
