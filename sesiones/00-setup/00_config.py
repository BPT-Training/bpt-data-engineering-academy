# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración compartida del curso
# MAGIC
# MAGIC **Propósito:** cargar la configuración de `shared/config.yml` y habilitar `import bpt_utils`. Todos los notebooks del curso lo ejecutan con `%run`.
# MAGIC
# MAGIC | Variable | Contenido |
# MAGIC |---|---|
# MAGIC | `REPO_ROOT` | Raíz del repositorio en el workspace |
# MAGIC | `CATALOG`, `SCHEMA` | Catálogo y esquema del curso |
# MAGIC | `SCHEMA_BRONZE`, `SCHEMA_SILVER`, `SCHEMA_GOLD` | Esquemas de cada capa |
# MAGIC | `VOLUME_PATH`, `CHECKPOINT_PATH` | Rutas de archivos y checkpoints |
# MAGIC | `LOG_TABLE` | Tabla de log de ejecuciones |
# MAGIC | `get_table_name(schema, name)` | Nombre completo de tres niveles |
# MAGIC
# MAGIC **Por qué un `.yml`:** el estándar separa la configuración del código; cambiar de catálogo o esquema no requiere editar notebooks.
# MAGIC
# MAGIC > Guía: `guia-buenas-practicas/05-estructura-repos.md` (en la raíz del repositorio)

# COMMAND ----------

# DBTITLE 1,Imports
import os
import sys
from pathlib import Path

import yaml

# COMMAND ----------

# DBTITLE 1,Ubicar la raíz del repositorio


def find_repo_root() -> Path:
    """
    Sube desde el directorio actual hasta encontrar src/bpt_utils.

    Returns:
        Ruta de la raíz del repositorio.

    Raises:
        FileNotFoundError: Si el notebook no está dentro del Git folder.
    """
    current = Path(os.getcwd()).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "src" / "bpt_utils").is_dir():
            return candidate
    raise FileNotFoundError(
        "No se encontró la raíz del repositorio (src/bpt_utils). "
        "Abrir el notebook desde el Git folder del curso."
    )


REPO_ROOT = find_repo_root()
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

# COMMAND ----------

# DBTITLE 1,Configuración
with open(REPO_ROOT / "shared" / "config.yml", encoding="utf-8") as file:
    config = yaml.safe_load(file)

global_params = config["global_parameters"]
CATALOG = global_params["catalog"]
SCHEMA = global_params["schema"]
SCHEMA_BRONZE = f"{SCHEMA}_{config['bronze_parameters']['schema_suffix']}"
SCHEMA_SILVER = f"{SCHEMA}_{config['silver_parameters']['schema_suffix']}"
SCHEMA_GOLD = f"{SCHEMA}_{config['gold_parameters']['schema_suffix']}"
VOLUME = global_params["volume"]
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"
LOG_TABLE = f"{CATALOG}.{SCHEMA}.{global_params['log_table']}"


def get_table_name(schema: str, name: str) -> str:
    """
    Construye el nombre completo de tres niveles de una tabla.

    Args:
        schema: Esquema de la tabla.
        name: Nombre de la tabla.

    Returns:
        Nombre catalogo.esquema.tabla.
    """
    return f"{CATALOG}.{schema}.{name}"


print(f"Repositorio: {REPO_ROOT}")
print(f"Esquema:     {CATALOG}.{SCHEMA}")
print(f"Volume:      {VOLUME_PATH}")
