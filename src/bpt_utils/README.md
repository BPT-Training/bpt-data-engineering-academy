# bpt_utils

Funciones reutilizables que usan los notebooks del workshop. Demuestran la regla "la lógica va en módulos, los notebooks orquestan" ([05-estructura-repos.md](../../guia-buenas-practicas/05-estructura-repos.md)).

| Módulo | Funciones |
|---|---|
| `limpieza.py` | `a_snake_case`, `normalizar_columnas`, `recortar_textos`, `deduplicar`, `parsear_timestamp` |
| `calidad.py` | `aplicar_expectativas(df, reglas) -> (validos, invalidos)`, `resumen_calidad` |
| `auditoria.py` | `agregar_columnas_ingesta`, `crear_tabla_log`, `registrar_ejecucion` |

## Uso desde un notebook

`sesiones/00-setup/00_config.py` agrega `src/` al `sys.path`, así que basta con:

```python
# MAGIC %run ../00-setup/00_config
from bpt_utils.calidad import aplicar_expectativas
```

Si modificas un módulo, reinicia el entorno de Python del notebook para que tome los cambios (menú del compute → *Restart*), o usa `%load_ext autoreload` + `%autoreload 2`.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

Requieren Java 17 para correr PySpark local. Los tests están en [`tests/`](../../tests/).
