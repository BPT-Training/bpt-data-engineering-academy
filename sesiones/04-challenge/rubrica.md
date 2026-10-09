# Rúbrica · Challenge M7

Puntaje total: **10 puntos**. Aprobado: **7 o más**.

| Criterio | 0 | 1 | 2 |
|---|---|---|---|
| **Correctitud** | La validación falla | La validación pasa con ayuda | La validación pasa sin ayuda |
| **Respeta el contrato de capa** | Limpia en Gold o agrega en Silver | Hay lógica en la capa equivocada, pero es menor | Cada cosa en su capa ([04 §2](../../guia-buenas-practicas/04-medallion.md#2-contrato-por-capa)) |
| **Idempotencia** | Reejecutar cambia el resultado | — | Reejecutar deja todo igual |
| **Estándar de código** | Sin alias, `import *`, nombres opacos | Parcial | Sigue la [guía](../../guia-buenas-practicas/): PEP 8, alias `F`, naming `df_<estado>`, docstrings |
| **Explica su decisión** | No sabe por qué funciona | Explica el qué | Explica el qué y el porqué (nulos, granularidad, propagación entre capas) |

## Preguntas de retroalimentación (2–3 min por persona)

- **Opción A:** ¿qué pasaría con los vuelos cancelados si la regla no tuviera `actual_departure_ts IS NULL OR …`?
- **Opción B:** ¿por qué construir el KPI desde `fct_flight` y no desde Silver?
- **Ambas:** si mañana llega un archivo nuevo al Volume, ¿qué notebooks hay que correr y en qué orden?
