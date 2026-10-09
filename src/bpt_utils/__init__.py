"""
Utilidades reutilizables de BPT para pipelines de datos en Databricks.

Módulos:
    udf_cleaning: limpieza y estandarización de DataFrames.
    udf_quality: reglas de calidad declarativas y quarantine.
    audit_helper: columnas técnicas de ingesta y log de ejecución.
    delta_writer: escrituras idempotentes (MERGE y overwrite).
    validation_functions: validaciones mínimas de tablas resultado.
"""

__version__ = "0.2.0"
