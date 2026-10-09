# 05 · Organización de repositorios, módulos, configuración y notebooks

---

## 1. Layout de un repositorio de datos

```
<dominio>/
├── README.md                propósito, cómo ejecutar, responsables
├── shared/
│   ├── config.yml           configuración global y por capa
│   └── <modulo>/            módulos reutilizables del dominio
│       └── udf_cleaning.py
├── <fuente>/
│   ├── jobs/                notebooks de orquestación (_orch)
│   │   └── 99_flights_orch.py
│   └── notebooks/           notebooks por capa (_nt)
│       ├── 20_landing_conformance_flights_nt.py
│       └── 30_conformance_curated_flights_nt.py
└── tests/                   pytest de las funciones críticas
```

**Principio:** la lógica vive en `.py`, la configuración en `.yml` y la metadata en tablas de control. Ninguno invade el espacio de los otros.

---

## 2. Módulos y clases

| Regla | Detalle |
|---|---|
| Un módulo, una responsabilidad | Nombre según su función: `udf_cleaning.py`, `delta_writer.py`, `audit_helper.py` |
| Sin nombres genéricos | Evitar `utils.py` o `helpers.py` que mezclan temas |
| Docstring de módulo obligatorio | Propósito, contenido y contexto de uso |
| Sin efectos al importar | No crear sesiones, leer archivos ni ejecutar lógica en el nivel de módulo |
| Enfoque funcional por defecto | Clases solo si hay estado, varias estrategias de ejecución o un framework aprobado por arquitectura |
| Funciones de transformación en módulos | Nunca definidas ad hoc en notebooks productivos |

```python
# ❌ Módulo genérico que mezcla responsabilidades: utils.py
def get_location(...): ...     # almacenamiento
def clean_columns(...): ...    # transformación
def send_alert(...): ...       # notificaciones

# ❌ Efectos secundarios al importar
spark = SparkSession.builder.getOrCreate()
config = yaml.safe_load(open("config.yml"))

# ❌ Clase sin estado
class DataProcessor:
    def process(self, df):
        return df.filter(F.col("status") == 1)

# ✅ Función simple
def filter_active(df: DataFrame) -> DataFrame:
    """Filtra los registros activos."""
    return df.filter(F.col("status") == 1)
```

### Naming de archivos

| Tipo | Convención | Ejemplo |
|---|---|---|
| Archivos `.py` | `snake_case`, según la responsabilidad | `delta_writer.py` |
| Notebooks de pipeline | Prefijo numérico de capa ([04 §1](04-medallion.md#1-capas-y-equivalencias)) | `30_conformance_curated_flights_nt.py` |
| Orquestación | Sufijo `_orch` | `99_flights_orch.py` |
| Módulos reutilizables | Sufijo `_helper` o `_functions`; prefijo `udf_` para transformaciones | `audit_helper.py`, `udf_quality.py` |

```
# ❌ Nombres sin contexto
script1.py
notebook_final_v2.py
test_copy_BORRAR.py
```

---

## 3. Configuración y parámetros

### 3.1 `config.yml`
- Ubicado en `shared/` del dominio.
- Claves en `snake_case`.
- Secciones estables: `global_parameters` y una por capa (`bronze_parameters`, `silver_parameters`, …).

```yaml
global_parameters:
  catalog: workspace
  schema: bpt_academy

silver_parameters:
  schema_suffix: silver
```

```python
# ❌ Configuración mezclada con la lógica
CATALOG = "prd"
BASE_PATH = "/Volumes/prd/raw/flights"
ENV = "dev"
```

### 3.2 Parámetros con prefijo
| Prefijo | Uso | Ejemplos |
|---|---|---|
| `wf_*` | Parámetros del workflow o job (contexto de ejecución) | `wf_env`, `wf_system` |
| `nb_*` | Parámetros que consume el notebook | `nb_load_type`, `nb_table` |

- El notebook de orquestación lee los `wf_*`, construye los `nb_*` y ejecuta los notebooks de cada capa.
- Ningún valor de ambiente, ruta o sistema se escribe en el código: todo proceso corre en DEV, QA y PRD sin cambios.

```python
# ❌ Sin prefijo y con ambiente fijo
system = dbutils.widgets.get("system")
env = "dev"

# ✅
wf_env = dbutils.widgets.get("wf_env")
nb_table = dbutils.widgets.get("nb_table")
```

---

## 4. Notebooks

### 4.1 Los notebooks orquestan, los módulos contienen la lógica
El notebook lee parámetros, llama funciones de `src/` o `shared/` y escribe. Así la lógica se prueba con pytest y se reutiliza.

### 4.2 Documentación obligatoria
- **Primera celda `%md`:** propósito, parámetros (`nb_*`), dependencias (módulos, tablas, configuración) y consideraciones operativas.
- **Secciones con `# DBTITLE 1,<Título>`:** Imports, Configuración, Reglas de negocio, Persistencia.
- **La documentación se actualiza en el mismo PR** que cambia el comportamiento.

```python
# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuración
nb_table = dbutils.widgets.get("nb_table")
```

### 4.3 Formato fuente, sin outputs
Los notebooks se versionan como `.py` en formato fuente de Databricks: texto plano con diffs legibles y sin resultados de celdas, que pueden contener datos.

---

## 5. Git

- Trabajo desde un **Git folder** en Databricks, no copiando notebooks a mano.
- `main` protegida; cambios por **Pull Request** desde ramas `feature/<descripcion>`.
- Cada PR pasa el **Quality Gate** ([06](06-calidad-y-seguridad.md)) y la revisión con el [checklist](checklist-code-review.md).
- PRs pequeños; mensajes de commit que digan qué cambia y por qué.

---

## 6. Pruebas

| Nivel | Qué | Cuándo |
|---|---|---|
| 1 | `assert` de validación en el notebook del pipeline ([04 §6](04-medallion.md#6-validación-de-tablas-resultado)) | Siempre (mínimo obligatorio) |
| 2 | Notebook de validación separado | Pipelines complejos |
| 3 | pytest de funciones de módulos compartidos | Funciones críticas |
| — | Smoke run en DEV | Antes de promover a QA |

Las pruebas corren en Databricks con `assert` y PySpark, sin frameworks externos complejos que luego no se mantienen.

---

## 7. Despliegue

- Jobs y pipelines definidos como código (Databricks Asset Bundles), no creados a mano en la interfaz.
- Configuración por ambiente fuera del código.
- Dependencias resueltas en el compute o en el job, nunca con `%pip install` en notebooks productivos.
