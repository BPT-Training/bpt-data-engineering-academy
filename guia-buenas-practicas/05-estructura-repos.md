# 05 · Organización de repos, notebooks y tests

---

## 1. Layout estándar de un repo de datos BPT

```
mi-proyecto/
├── README.md              propósito, cómo correr, dueños
├── databricks.yml         Asset Bundle (jobs, pipelines, ambientes)
├── src/
│   └── mi_proyecto/       lógica reutilizable (.py)
├── notebooks/             orquestación: bronze/, silver/, gold/
├── tests/                 pytest sobre src/
├── resources/             definiciones de jobs del bundle
└── .azure-pipelines.yml   CI: lint + tests
```

**Regla:** todo repo tiene un README con qué hace, cómo se ejecuta y quién es el dueño.

---

## 2. Notebooks vs. módulos

### 2.1 Los notebooks orquestan, los módulos contienen la lógica
**Regla:** las funciones de transformación, calidad y auditoría viven en `src/` y se importan. El notebook lee parámetros, llama funciones y escribe.
**Por qué:** los módulos se prueban con pytest, se reutilizan entre notebooks y tienen diffs limpios.

```python
# notebook
from mi_proyecto.limpieza import normalizar_columnas
df_silver = normalizar_columnas(df_bronze)
```

### 2.2 Notebooks en formato fuente
**Regla:** los notebooks se versionan como `.py` en formato fuente de Databricks (`# Databricks notebook source`), no como `.ipynb` ni `.dbc`.
**Por qué:** son texto plano, así que Git muestra diffs legibles y no guarda outputs, que pueden contener datos.

### 2.3 Estructura estándar de un notebook
1. **Celda markdown:** título, propósito, entradas y salidas.
2. **Parámetros:** widgets.
3. **Imports y configuración.**
4. **Lógica:** llamadas a funciones.
5. **Validaciones:** asserts o expectativas.
6. **Escritura y log.**

### 2.4 Sin outputs ni datos en el repo
**Regla:** no se versionan resultados de celdas ni datos reales. Los datos de prueba son sintéticos y pequeños.

---

## 3. Git en Databricks

### 3.1 Git folders conectados a Azure DevOps
**Regla:** se trabaja desde un Git folder (Workspace → Create → Git folder), no copiando notebooks a mano en el workspace.

### 3.2 Flujo de ramas
- `main`: protegida, solo se actualiza por PR.
- `feature/<descripcion-corta>`: una rama por cambio.
- Cada PR lleva al menos un revisor y usa el [checklist de code review](checklist-code-review.md).

### 3.3 Commits y PRs pequeños
**Regla:** un PR resuelve una cosa. Los mensajes de commit dicen qué cambia y por qué (`silver: dedup de vuelos por último _ingest_ts`).

---

## 4. Testing y calidad

### 4.1 pytest para `src/`
**Regla:** cada función de `src/` tiene tests con DataFrames pequeños. Se ejecutan local con `pyspark` o en Databricks con Databricks Connect.

### 4.2 Lint en CI
**Regla:** el pipeline de Azure DevOps corre `ruff check` y `pytest` en cada PR.

---

## 5. Despliegue

### 5.1 Databricks Asset Bundles
**Regla:** los jobs y pipelines se despliegan con `databricks bundle deploy -t <ambiente>`, no se crean a mano en la UI.
**Por qué:** la definición queda versionada y es igual en todos los ambientes.

### 5.2 Configuración por ambiente fuera del código
**Regla:** catálogo, rutas y tamaños de cluster se definen por *target* en `databricks.yml` y se pasan como parámetros.
