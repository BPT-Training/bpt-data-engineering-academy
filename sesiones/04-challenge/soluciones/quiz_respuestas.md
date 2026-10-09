# Respuestas · Quiz

| # | Respuesta | Por qué | Guía |
|---|---|---|---|
| 1 | **b** `[1, 2]` | El valor por defecto mutable se crea una sola vez y se comparte entre llamadas | [01 §2.3](../../../guia-buenas-practicas/01-python.md#23-sin-argumentos-mutables-por-defecto) |
| 2 | **d** | `count` es una acción; el resto son transformaciones *lazy* | [M3](../../02-pyspark/03_pyspark_fundamentals.py) |
| 3 | **b** | El join multiplica filas en silencio; por eso se valida la cardinalidad | [02 §4.4](../../../guia-buenas-practicas/02-pyspark.md#44-validar-la-cardinalidad) |
| 4 | **c** | `dropDuplicates` se queda con cualquier fila; `groupBy` pierde las demás columnas | [02 §3.3](../../../guia-buenas-practicas/02-pyspark.md#33-deduplicación-determinística) |
| 5 | **b** | Una UDF de Python serializa fila a fila entre la JVM y Python, y es una caja negra para el optimizador | [02 §3.1](../../../guia-buenas-practicas/02-pyspark.md#31-built-ins-antes-que-udfs) |
| 6 | **b** | `append` no es idempotente; para reprocesos se usa MERGE o `replaceWhere` | [03 §2.2](../../../guia-buenas-practicas/03-delta-unity-catalog.md#22-cargas-idempotentes) |
| 7 | **c** | `DESCRIBE HISTORY` lista versiones, operación, usuario y métricas | [03 §3.1](../../../guia-buenas-practicas/03-delta-unity-catalog.md#31-describe-history-como-fuente-de-verdad) |
| 8 | **b** | Bronze conserva el dato crudo; Gold consume datos ya limpios | [04 §1](../../../guia-buenas-practicas/04-medallion.md#1-contrato-por-capa) |
| 9 | **c** | Nada se descarta en silencio: la quarantine permite explicar por qué un número no cuadra | [04 §3.2](../../../guia-buenas-practicas/04-medallion.md#32-quarantine-no-descarte-silencioso) |
| 10 | **b** | Los módulos se prueban con pytest y se reutilizan; los notebooks orquestan | [05 §2.1](../../../guia-buenas-practicas/05-estructura-repos.md#21-los-notebooks-orquestan-los-módulos-contienen-la-lógica) |

**Escala:** 9–10 dominio · 7–8 sólido · 5–6 repasar la guía · <5 agendar refuerzo.
