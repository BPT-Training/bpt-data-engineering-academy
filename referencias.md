# Referencias externas

Recursos evaluados para el programa. **Criterio:** contenido oficial de Databricks o repos pensados para Free Edition, importables como Git folder y ejecutables en serverless.

Leyenda: ✅ nativo, sin fricción · ⚠️ validar antes del taller.

El material de este repo es **propio de BPT**. Estos recursos son la fuente de inspiración y lectura complementaria para los participantes.

## Por sesión

| Sesión | Recurso | Qué aporta | Free Edition | Ajustes |
|---|---|---|---|---|
| 00 · Setup | [Databricks Academy · Get Started with Databricks Free Edition](https://www.databricks.com/training/catalog/get-started-with-databricks-free-edition-4486) | 2 notebooks guiados: workspace, Python/SQL, Unity Catalog, Volumes | ✅ | Registrarse con el mismo correo |
| 00 · Setup | [Docs · Git folders](https://docs.databricks.com/aws/en/repos/) | Workspace → Create → Git folder | ✅ | — |
| 01 · Python | [calvinvette/introduction-to-python-for-data-science-and-data-engineering-english](https://github.com/calvinvette/introduction-to-python-for-data-science-and-data-engineering-english) | Lecciones 01–06 (M1) y 10, 11, 13 (M2) | ⚠️ | Omitir `%run ../Includes/Classroom-Setup`; usar `samples.tpch` en vez del dataset del curso |
| 01 · Python | [jakevdp/WhirlwindTourOfPython](https://github.com/jakevdp/WhirlwindTourOfPython) | Notebooks 03–10 (M1) y 11–13 (M2) | ✅ | No cubre pandas → Spark |
| 02 · PySpark | [jrlasak/databricks-code-practice](https://github.com/jrlasak/databricks-code-practice) · `exercises/ELT` | Transformaciones y joins (M3), window functions (M4); asserts pass/fail | ✅ | — |
| 02 · PySpark | [Docs · PySpark basics](https://docs.databricks.com/aws/en/pyspark/basics) | select, withColumn, cast, filter, dedup, nulls, joins sobre `samples.tpch` | ✅ | No cubre window functions |
| 03 · M5 | [jrlasak/databricks-code-practice](https://github.com/jrlasak/databricks-code-practice) · `exercises/Delta Lake` | Tablas UC, MERGE, time travel, schema enforcement | ✅ | — |
| 03 · M5 | [Docs · Load and transform data with DataFrames](https://docs.databricks.com/aws/en/getting-started/dataframes) | Volume → CSV → DataFrame → Delta en UC | ⚠️ | Si falla la descarga, subir el CSV a mano al Volume |
| 03 · M6 | [DataWithBaraa/databricks_bootcamp_2026](https://github.com/DataWithBaraa/databricks_bootcamp_2026) | Proyecto Medallion completo (datasets/ + script/) | ⚠️ | Subir CSV a un Volume y ajustar rutas en Bronze |
| 03 · M6 | [jrlasak/databricks-code-practice](https://github.com/jrlasak/databricks-code-practice) · pipeline-labs | Lab Medallion con Auto Loader y COPY INTO | ✅ | Recortar a Bronze + Silver |
| 04 · Challenge | Proyecto de la sesión 03 | Reto: 1 regla de calidad en Silver o 1 KPI en Gold | ✅ | BPT define enunciado y rúbrica |

## Descartados

| Repo | Motivo |
|---|---|
| danielbeach/data-engineering-practice | Corre en Docker local, no en Databricks |
| coder2j/pyspark-tutorial | Pensado para Spark local |
| snehal29/Pyspark-Practice | Sin tracción ni evidencia de Databricks |
| billkellett/databricks-data-modeling-workshop | Formato `.dbc` antiguo con DBFS y descargas por internet |
| databricks-demos/dbdemos | Requiere permisos de clusters/pipelines que Free Edition no da |

## Recordatorio Free Edition

Solo compute serverless, solo Python y SQL (sin Scala ni R), acceso a internet limitado y catálogo `samples` disponible.
