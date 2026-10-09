# Ruta de profundización

Siguientes pasos después del workshop, en orden sugerido.

## 1. Consolidar (semanas 1–2)
- Rehacer los labs sin mirar las soluciones.
- Leer completa la [guía de buenas prácticas](../../guia-buenas-practicas/) y aplicar el [checklist](../../guia-buenas-practicas/checklist-code-review.md) a un notebook propio.
- Practicar con los ejercicios auto-calificados de [jrlasak/databricks-code-practice](https://github.com/jrlasak/databricks-code-practice) (ELT y Delta Lake).

## 2. Ampliar (semanas 3–6)
| Tema | Recurso |
|---|---|
| Lakeflow Declarative Pipelines (antes DLT) | Databricks Docs · Lakeflow Declarative Pipelines |
| Jobs y orquestación | Databricks Docs · Lakeflow Jobs |
| Structured Streaming | Databricks Docs · Structured Streaming |
| Proyecto Medallion completo | [DataWithBaraa/databricks_bootcamp_2026](https://github.com/DataWithBaraa/databricks_bootcamp_2026) |

## 3. Llevar a producción (semanas 6+)
- **Databricks Asset Bundles:** jobs y pipelines versionados y desplegados por ambiente ([05 §5](../../guia-buenas-practicas/05-estructura-repos.md#5-despliegue)).
- **CI:** lint (ruff) y tests (pytest) en cada PR, según [05 §4](../../guia-buenas-practicas/05-estructura-repos.md#4-testing-y-calidad).
- **Unity Catalog avanzado:** permisos por grupo, lineage, row filters y column masks.

## 4. Certificarse
- **Databricks Certified Data Engineer Associate:** cubre casi todo lo visto en el workshop.
- Material oficial gratuito en Databricks Academy (con la misma cuenta de Free Edition).

Más recursos evaluados: [referencias.md](../../referencias.md).
