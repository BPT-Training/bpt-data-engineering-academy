# Databricks notebook source
# MAGIC %md
# MAGIC # M6 · Orquestación del pipeline
# MAGIC
# MAGIC **Propósito:** ejecutar las capas en orden (preparación → Bronze → Silver → Gold) con un solo comando, y mostrar cómo se programa como un **Job**.
# MAGIC
# MAGIC **Parámetros (workflow):**
# MAGIC
# MAGIC | Parámetro | Valores | Uso |
# MAGIC |---|---|---|
# MAGIC | `wf_reset` | `false` / `true` | Se pasa como `nb_reset` al notebook de preparación |
# MAGIC
# MAGIC **Convención de parámetros:** `wf_*` es el contexto de la ejecución; el orquestador construye los `nb_*` que consume cada notebook (`guia-buenas-practicas/05-estructura-repos.md` §3.2, en la raíz del repositorio).

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Parámetros del workflow
dbutils.widgets.dropdown("wf_reset", "false", ["false", "true"], "Reiniciar")
wf_reset = dbutils.widgets.get("wf_reset")

NOTEBOOK_TIMEOUT_S = 1800
PIPELINE_STEPS = [
    ("./00_pipeline_setup", {"nb_reset": wf_reset}),
    ("./20_landing_conformance_flights_nt", {}),
    ("./30_conformance_curated_flights_nt", {}),
    ("./40_curated_gold_flights_nt", {}),
]

# COMMAND ----------

# DBTITLE 1,Ejecución en orden: si un paso falla, el pipeline se detiene
for notebook_path, notebook_params in PIPELINE_STEPS:
    started_at = datetime.now()
    print(f"▶ {notebook_path} {notebook_params}")
    dbutils.notebook.run(notebook_path, NOTEBOOK_TIMEOUT_S, notebook_params)
    elapsed_s = (datetime.now() - started_at).seconds
    print(f"  ✅ terminado en {elapsed_s} s")

print("Pipeline completo. Revisar 90_pipeline_audit.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programar el pipeline como un Job
# MAGIC
# MAGIC En un proyecto real la orquestación no se ejecuta a mano: se define como un **Job** con una tarea por capa y dependencias entre ellas. Free Edition permite crear Jobs con compute serverless.
# MAGIC
# MAGIC 1. Menú lateral → **Jobs & Pipelines → Create → Job**.
# MAGIC 2. Primera tarea: nombre `setup`, tipo **Notebook**, ruta `pipeline/00_pipeline_setup`, compute **Serverless**, parámetro `nb_reset = false`.
# MAGIC 3. **Add task** → `bronze` → `pipeline/20_landing_conformance_flights_nt`, con **Depends on:** `setup`.
# MAGIC 4. **Add task** → `silver` → `pipeline/30_conformance_curated_flights_nt`, con **Depends on:** `bronze`.
# MAGIC 5. **Add task** → `gold` → `pipeline/40_curated_gold_flights_nt`, con **Depends on:** `silver`.
# MAGIC 6. **Run now** y revisar el resultado de cada tarea. Opcional: **Schedules & Triggers** para programarlo.
# MAGIC
# MAGIC ```
# MAGIC  setup ──► bronze ──► silver ──► gold
# MAGIC ```
# MAGIC
# MAGIC **Notebook frente a Job:** este notebook sirve para el workshop. El Job agrega reintentos, historial de ejecuciones, alertas y programación. En proyectos, el Job se define como código (Databricks Asset Bundles), no a mano en la interfaz (`guia-buenas-practicas/05-estructura-repos.md` §7, en la raíz del repositorio).
