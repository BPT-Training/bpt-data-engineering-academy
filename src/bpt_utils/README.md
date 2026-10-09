# bpt_utils

Funciones reutilizables que usan los notebooks del curso. Aplican la regla "los notebooks orquestan, los módulos contienen la lógica" ([05-estructura-repos.md](../../guia-buenas-practicas/05-estructura-repos.md)).

| Módulo | Responsabilidad | Funciones |
|---|---|---|
| `udf_cleaning.py` | Limpieza y estandarización | `to_snake_case`, `_udf_normalize_column_names`, `_udf_clean_columns`, `_udf_drop_duplicates_by_key`, `parse_timestamp` |
| `udf_quality.py` | Reglas de calidad y quarantine | `_udf_apply_expectations`, `_udf_summarize_quality` |
| `audit_helper.py` | Columnas de ingesta y log de ejecución | `_udf_add_ingestion_columns`, `create_log_table`, `log_execution` |
| `delta_writer.py` | Escrituras idempotentes | `build_merge_condition`, `merge_by_key`, `overwrite_table` |
| `validation_functions.py` | Validaciones de tablas resultado | `validate_schema`, `validate_not_empty`, `validate_unique_key`, `validate_no_nulls` |

## Convenciones

- **Prefijo `_udf_`:** funciones de transformación DataFrame → DataFrame, siguiendo el estándar de desarrollo.
- **Docstrings:** Google-style, con `Args`, `Returns` y `Raises`.
- **Columnas temporales:** prefijo `_tmp_`.
- **Sin efectos al importar:** ningún módulo crea sesiones, lee archivos ni ejecuta lógica en el nivel de módulo.

## Uso desde un notebook

`sesiones/00-setup/00_config` agrega `src/` al `sys.path`:

```python
# MAGIC %run ../00-setup/00_config
from bpt_utils.udf_quality import _udf_apply_expectations
```

Si se modifica un módulo, hay que reiniciar el entorno de Python del notebook para que tome los cambios.

## Tests

```bash
pip install -r requirements-dev.txt   # requiere Java 17 para PySpark local
pytest
```
