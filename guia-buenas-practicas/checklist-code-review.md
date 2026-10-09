# Checklist de code review · Datos BPT

Copiar en la descripción del PR y marcar lo que aplique. Cada punto enlaza su regla.

## General
- [ ] El PR resuelve una sola cosa y el título dice qué ([05 §3.3](05-estructura-repos.md#33-commits-y-prs-pequeños))
- [ ] La lógica está en `src/` y el notebook solo orquesta ([05 §2.1](05-estructura-repos.md#21-los-notebooks-orquestan-los-módulos-contienen-la-lógica))
- [ ] No hay outputs, datos reales ni credenciales versionados ([01 §6.1](01-python.md#61-sin-credenciales-en-el-código))

## Python
- [ ] Nombres descriptivos, type hints y docstring en funciones públicas ([01 §1](01-python.md#1-estilo-y-legibilidad))
- [ ] Sin rutas, tablas ni umbrales hardcodeados ([01 §2.2](01-python.md#22-nada-hardcodeado-en-el-cuerpo))
- [ ] Excepciones específicas y `logging` en lugar de `print` ([01 §4](01-python.md#4-errores-y-logging))

## PySpark
- [ ] `functions as F`, sin `import *` ([02 §1.1](02-pyspark.md#11-imports-con-alias))
- [ ] Schema explícito en lecturas de fuentes ([02 §2.1](02-pyspark.md#21-schema-explícito-al-leer-fuentes))
- [ ] Built-ins antes que UDFs ([02 §3.1](02-pyspark.md#31-built-ins-antes-que-udfs))
- [ ] Deduplicación determinística ([02 §3.3](02-pyspark.md#33-deduplicación-determinística))
- [ ] Joins con tipo explícito y cardinalidad validada ([02 §4](02-pyspark.md#4-joins))
- [ ] Sin `collect()` / `toPandas()` sobre datos grandes ([02 §6.1](02-pyspark.md#61-nada-de-collect-sobre-datos-grandes))

## Delta / Unity Catalog
- [ ] Nombres de tres niveles, siguiendo la convención ([03 §1](03-delta-unity-catalog.md#1-unity-catalog-naming-y-organización))
- [ ] Escritura idempotente (MERGE / replaceWhere) ([03 §2.2](03-delta-unity-catalog.md#22-cargas-idempotentes))
- [ ] `COMMENT` en tablas nuevas de Silver y Gold ([03 §1.3](03-delta-unity-catalog.md#13-documentar-las-tablas))

## Medallion
- [ ] La transformación respeta el contrato de su capa ([04 §1](04-medallion.md#1-contrato-por-capa))
- [ ] Columnas técnicas de ingesta en Bronze ([04 §2.2](04-medallion.md#22-columnas-técnicas-obligatorias))
- [ ] Registros inválidos a quarantine, no descartados ([04 §3.2](04-medallion.md#32-quarantine-no-descarte-silencioso))
- [ ] Ejecución registrada en la tabla de log, también si falla ([04 §5](04-medallion.md#5-auditoría-y-observabilidad))
