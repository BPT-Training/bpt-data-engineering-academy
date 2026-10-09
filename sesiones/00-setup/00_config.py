# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración compartida del workshop
# MAGIC
# MAGIC Todos los notebooks del curso cargan este archivo con `%run`. Define:
# MAGIC - Catálogo, esquemas y Volume donde trabajamos.
# MAGIC - La ruta a `src/` para poder importar `bpt_utils`.
# MAGIC
# MAGIC > Guía: [05-estructura-repos.md](../../guia-buenas-practicas/05-estructura-repos.md). La configuración vive en un solo lugar, no repetida en cada notebook.
# MAGIC
# MAGIC **No es necesario ejecutarlo directamente.**

# COMMAND ----------

import os
import sys
from pathlib import Path


def _raiz_repo() -> Path:
    """Sube desde el directorio del notebook hasta encontrar la carpeta src/bpt_utils."""
    actual = Path(os.getcwd()).resolve()
    for candidata in [actual, *actual.parents]:
        if (candidata / "src" / "bpt_utils").is_dir():
            return candidata
    raise FileNotFoundError(
        "No se encontró la raíz del repo (carpeta src/bpt_utils). "
        "¿Clonaste el repo como Git folder (Workspace → Create → Git folder)?"
    )


RAIZ_REPO = _raiz_repo()
if str(RAIZ_REPO / "src") not in sys.path:
    sys.path.insert(0, str(RAIZ_REPO / "src"))

# COMMAND ----------

# En Free Edition el catálogo por defecto es `workspace`.
# Si trabajas en un workspace compartido, cambia ESQUEMA por uno con tu nombre (ej. "bpt_academy_sp").
CATALOGO = "workspace"
ESQUEMA = "bpt_academy"
ESQUEMA_BRONZE = f"{ESQUEMA}_bronze"
ESQUEMA_SILVER = f"{ESQUEMA}_silver"
ESQUEMA_GOLD = f"{ESQUEMA}_gold"

VOLUME = "raw"
RUTA_VOLUME = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUME}"
RUTA_CHECKPOINTS = f"{RUTA_VOLUME}/_checkpoints"

TABLA_LOG = f"{CATALOGO}.{ESQUEMA}.log_ejecuciones"


def tabla(esquema: str, nombre: str) -> str:
    """Nombre completo de tres niveles: catalogo.esquema.tabla."""
    return f"{CATALOGO}.{esquema}.{nombre}"


print(f"Repo:    {RAIZ_REPO}")
print(f"Esquema: {CATALOGO}.{ESQUEMA}  (bronze/silver/gold: {ESQUEMA_BRONZE}, {ESQUEMA_SILVER}, {ESQUEMA_GOLD})")
print(f"Volume:  {RUTA_VOLUME}")
