# data

CSV **sintéticos** del dominio "operación de vuelos" para la sesión 03 (M6) y el challenge. **No contienen datos reales** de ningún cliente.

Se generan con [`scripts/generar_datos.py`](../scripts/generar_datos.py), que es determinístico (semilla fija). Cada entidad está en su propia subcarpeta porque Auto Loader lee carpetas, no archivos sueltos.

| Archivo | Filas | Columnas |
|---|---|---|
| `aeropuertos/aeropuertos.csv` | 17 | Código IATA, Nombre, Ciudad, País |
| `vuelos/vuelos.csv` | 305 | Vuelo Id, Numero Vuelo, Origen, Destino, Salida Programada, Salida Real, Estado |
| `reservas/reservas.csv` | 1505 | Reserva Id, Vuelo Id, Pasajero Id, Fecha Reserva, Tarifa, Moneda |

## Problemas de calidad sembrados a propósito

| Entidad | Problema | Cuántos | Dónde se resuelve |
|---|---|---|---|
| Todas | Encabezados con espacios y tildes | — | Bronze (`normalizar_columnas`) |
| Aeropuertos | Códigos en minúscula o con espacios que duplican otros | 2 | Silver: limpieza + dedup |
| Vuelos | Fechas en dos formatos (`AAAA-MM-DD` y `DD/MM/AAAA`) | ~20 % | Silver: `parsear_timestamp` |
| Vuelos | Filas duplicadas | 5 | Silver: dedup |
| Vuelos | Origen = destino | 4 | Quarantine |
| Vuelos | Salida programada vacía o `"sin dato"` | 3 | Quarantine |
| Vuelos | OPERADO sin salida real | 3 | Quarantine |
| Vuelos | Aeropuerto inexistente (`XXX`) | 2 | Quarantine |
| Vuelos | Códigos en minúscula con espacios | 2 | Silver: limpieza (quedan válidos) |
| Vuelos | Salida real 2 días **antes** de la programada | 2 | ⚠️ No lo detecta el pipeline base: **challenge, opción A** |
| Reservas | Filas duplicadas | 5 | Silver: dedup |
| Reservas | Tarifa negativa | 6 | Quarantine |
| Reservas | Tarifa vacía | 4 | Quarantine |
| Reservas | Vuelo inexistente (`VL9999`) | 5 | Quarantine |

## Conteos esperados después de Silver

| Entidad | Bronze | Silver | Quarantine | Duplicados |
|---|---|---|---|---|
| Aeropuertos | 17 | 15 | 0 | 2 |
| Vuelos | 305 | 288 | 12 | 5 |
| Reservas | 1505 | 1428 | 72 | 5 |

Las 72 reservas en quarantine incluyen las que apuntan a vuelos que también fueron a quarantine. Después de la opción A del challenge: vuelos 286 / 14, reservas 1416 / 84.
