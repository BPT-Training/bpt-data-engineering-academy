# Checklist de code review

Copiar en la descripción del PR y marcar lo que aplique. Cada punto enlaza su regla.

## Quality Gate
- [ ] SonarCloud aprobado, sin Vulnerabilities ni hallazgos Blocker/Critical ([06 §1](06-calidad-y-seguridad.md#1-quality-gate-sonarcloud-y-fluid-attacks))
- [ ] Sin credenciales, tokens ni cadenas de conexión en el código ([06 §2](06-calidad-y-seguridad.md#2-credenciales-y-secretos))
- [ ] Sin `eval()` ni `exec()`; parámetros validados antes de usarlos en SQL ([06 §3](06-calidad-y-seguridad.md#3-ejecución-dinámica-de-código))
- [ ] Sin datos personales ni credenciales en logs ([06 §4](06-calidad-y-seguridad.md#4-datos-sensibles-en-logs))

## Estilo (PEP 8)
- [ ] Líneas de máximo 79 caracteres (72 en docstrings y comentarios) ([01 §1](01-python.md#1-pep-8-como-estándar-obligatorio))
- [ ] Imports en tres bloques ordenados; alias `F`, `T`, `W` ([02 §1.1](02-pyspark.md#11-alias-estándar-obligatorios))
- [ ] Naming: `snake_case`, constantes en mayúscula, booleanos `is_`/`has_`, DataFrames `df_<estado>`, columnas temporales `_tmp_` ([01 §2](01-python.md#2-naming), [02 §2](02-pyspark.md#2-naming-en-pyspark))
- [ ] Código en inglés de forma consistente dentro del módulo
- [ ] Comentarios que explican el "por qué"; sin código comentado

## Documentación
- [ ] Docstrings Google-style con `Args`, `Returns` y `Raises` en funciones importables ([01 §3.2](01-python.md#32-docstrings-en-formato-google))
- [ ] Docstring de módulo en módulos compartidos ([05 §2](05-estructura-repos.md#2-módulos-y-clases))
- [ ] Notebook con celda `%md` inicial y secciones `DBTITLE` ([05 §4.2](05-estructura-repos.md#42-documentación-obligatoria))

## Diseño
- [ ] Funciones con una sola responsabilidad, sin globales ni valores hardcodeados ([01 §4](01-python.md#4-funciones))
- [ ] Funciones de transformación con prefijo `_udf_`, en módulos versionados ([02 §4](02-pyspark.md#4-funciones-de-transformación-udf))
- [ ] Configuración en `config.yml` y parámetros con prefijo `wf_*`/`nb_*` ([05 §3](05-estructura-repos.md#3-configuración-y-parámetros))
- [ ] Sin `%pip install` en notebooks productivos ([02 §1.2](02-pyspark.md#12-librerías-aceptadas))

## Errores
- [ ] Ningún error silenciado; se re-lanza con contexto ([01 §6](01-python.md#6-manejo-de-errores))
- [ ] Mensajes que dicen qué falló, dónde y qué revisar

## PySpark
- [ ] Funciones nativas antes que UDF de Python ([02 §4.1](02-pyspark.md#41-funciones-nativas-antes-que-udf-de-python))
- [ ] Schema explícito al leer fuentes ([02 §5](02-pyspark.md#5-schemas-y-tipos))
- [ ] Deduplicación determinística y joins con cardinalidad validada ([02 §6](02-pyspark.md#6-patrones-frecuentes))
- [ ] Sin `collect()` ni `toPandas()` sobre tablas grandes ([02 §7](02-pyspark.md#7-rendimiento-básico))

## Datos (Delta y Medallion)
- [ ] Nombres de tres niveles y `COMMENT` en tablas nuevas ([03 §1](03-delta-unity-catalog.md#1-unity-catalog-naming-y-organización))
- [ ] Escritura idempotente (MERGE o `replaceWhere`) ([03 §2.2](03-delta-unity-catalog.md#22-cargas-idempotentes))
- [ ] La transformación respeta el contrato de su capa ([04 §2](04-medallion.md#2-contrato-por-capa))
- [ ] Validaciones de la tabla resultado: esquema, no vacía, llave única, nulos ([04 §6](04-medallion.md#6-validación-de-tablas-resultado))
- [ ] Registros inválidos en quarantine y ejecución registrada en el log ([04 §7](04-medallion.md#7-auditoría))
- [ ] Smoke run en DEV antes de promover a QA
