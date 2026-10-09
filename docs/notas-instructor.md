# Notas para el instructor

Pautas para dictar el curso en formato workshop.

## Formato

- **Workshop guiado, no clase personalizada.** El instructor sigue la agenda de cada sesión; los participantes replican en su propio workspace.
- **Cada sesión de 2 horas** se divide en dos módulos de 1 hora; cada módulo, en **4 bloques de 15 minutos**. La agenda de cada bloque está en el README de la sesión.
- **Cada participante trabaja en su propio Databricks Free Edition.** BPT proporciona el repositorio; no se usan ambientes corporativos.

## Preguntas

- Las preguntas se reciben **por el chat** durante la sesión y se responden en los últimos **5 minutos** o por escrito después.
- **La sesión no se detiene** para resolver problemas individuales de configuración. Quien se atrase termina los ejercicios por su cuenta con el repositorio y las soluciones.
- Durante el programa hay un canal de soporte para dudas fuera de sesión.

## Alcance de las respuestas

- **Solo temas técnicos:** la herramienta (Databricks), el lenguaje (Python, PySpark, SQL) y las buenas prácticas de la guía.
- **No se dan recomendaciones sobre procesos, seguridad o decisiones del cliente.** Ese tipo de preguntas se registra y se escala al responsable del dominio.

## Tono

- Lenguaje **técnico y formal**. El grupo puede incluir personas de distintos países y roles.
- En cada celda se explica **para qué sirve**, el **patrón** y el **antipatrón**, con referencia a la guía de buenas prácticas.

## Antes de cada sesión

| Sesión | Preparación |
|---|---|
| 00 | Enviar la guía de Databricks Free Edition con anticipación. Verificar que la URL del repositorio clona correctamente |
| 01 | Ejecutar los notebooks guiados y las soluciones en un workspace limpio |
| 02 | Ídem; confirmar acceso a `samples.tpch` |
| 03 | Ejecutar `pipeline/99_flights_orch`, luego `60_gold_consumption` y `90_pipeline_audit`; comparar con los conteos de [data/README.md](../data/README.md) |
| 04 | Definir la opción del challenge y compartir la rúbrica |

## Después de cada sesión

- Pedir a los participantes que actualicen el repositorio (**Pull**) antes de la siguiente.
- Registrar las preguntas frecuentes para mejorar el material.
