"""Genera los CSV sintéticos de la sesión 03 (operación de vuelos) en data/.

Determinístico (semilla fija): reejecutarlo produce exactamente los mismos archivos.
Siembra a propósito problemas de calidad para practicar limpieza y quarantine.

Uso:
    python scripts/generar_datos.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEMILLA = 2026
RAIZ = Path(__file__).resolve().parents[1] / "data"

AEROPUERTOS = [
    ("BOG", "El Dorado", "Bogotá", "Colombia"),
    ("MDE", "José María Córdova", "Rionegro", "Colombia"),
    ("CLO", "Alfonso Bonilla Aragón", "Cali", "Colombia"),
    ("CTG", "Rafael Núñez", "Cartagena", "Colombia"),
    ("BAQ", "Ernesto Cortissoz", "Barranquilla", "Colombia"),
    ("SMR", "Simón Bolívar", "Santa Marta", "Colombia"),
    ("ADZ", "Gustavo Rojas Pinilla", "San Andrés", "Colombia"),
    ("PEI", "Matecaña", "Pereira", "Colombia"),
    ("BGA", "Palonegro", "Bucaramanga", "Colombia"),
    ("LIM", "Jorge Chávez", "Lima", "Perú"),
    ("UIO", "Mariscal Sucre", "Quito", "Ecuador"),
    ("MIA", "Miami International", "Miami", "Estados Unidos"),
    ("MAD", "Adolfo Suárez Madrid-Barajas", "Madrid", "España"),
    ("MEX", "Benito Juárez", "Ciudad de México", "México"),
    ("SJO", "Juan Santamaría", "San José", "Costa Rica"),
]

N_VUELOS = 300
N_RESERVAS = 1500
INICIO_MES = datetime(2026, 10, 1)


def escribir_csv(ruta: Path, encabezado: list[str], filas: list[list]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(encabezado)
        escritor.writerows(filas)
    print(f"{ruta.relative_to(RAIZ.parent)}: {len(filas)} filas")


def formatear_fecha(fecha: datetime, rng: random.Random) -> str:
    """80 % en formato ISO y 20 % en formato día/mes/año: dos formatos en la misma columna."""
    if rng.random() < 0.8:
        return fecha.strftime("%Y-%m-%d %H:%M")
    return fecha.strftime("%d/%m/%Y %H:%M")


def generar_aeropuertos() -> list[list]:
    filas = [list(a) for a in AEROPUERTOS]
    # Problemas sembrados: códigos en minúscula / con espacios que duplican aeropuertos existentes
    filas.append([" bog", "El Dorado", "Bogotá", "Colombia"])
    filas.append(["mde ", "José María Córdova", "Rionegro", "Colombia"])
    return filas


def generar_vuelos(rng: random.Random) -> list[list]:
    codigos = [a[0] for a in AEROPUERTOS]
    filas, programadas = [], []
    for i in range(1, N_VUELOS + 1):
        origen, destino = rng.sample(codigos, 2)
        programada = INICIO_MES + timedelta(
            days=rng.randrange(31), hours=rng.randrange(5, 23), minutes=rng.choice([0, 15, 30, 45])
        )
        if rng.random() < 0.05:
            estado, real = "CANCELADO", ""
        else:
            azar = rng.random()
            if azar < 0.10:
                retraso = rng.randint(-10, -1)
            elif azar < 0.70:
                retraso = rng.randint(0, 15)
            elif azar < 0.90:
                retraso = rng.randint(16, 60)
            else:
                retraso = rng.randint(61, 180)
            estado, real = "OPERADO", formatear_fecha(programada + timedelta(minutes=retraso), rng)
        programadas.append(programada)
        filas.append(
            [f"VL{i:04d}", f"AV{rng.randint(100, 9999)}", origen, destino,
             formatear_fecha(programada, rng), real, estado]
        )

    # Problemas sembrados (sobre índices fijos para que sean reproducibles)
    for idx in (10, 50, 120, 200):  # origen = destino
        filas[idx][3] = filas[idx][2]
    for idx in (30, 150, 250):  # salida programada ausente o basura
        filas[idx][4] = "" if idx != 150 else "sin dato"
    for idx in (70, 180, 260):  # OPERADO sin salida real
        filas[idx][6], filas[idx][5] = "OPERADO", ""
    for idx in (90, 210):  # códigos en minúscula con espacios (se corrigen en limpieza)
        filas[idx][2] = f" {filas[idx][2].lower()}"
    for idx in (100, 220):  # aeropuerto inexistente
        filas[idx][3] = "XXX"
    # Salida real dos días ANTES de la programada (error de digitación).
    # No lo detecta el pipeline base: es el objetivo de la opción A del challenge.
    for idx in (40, 160):
        filas[idx][5] = (programadas[idx] - timedelta(days=2)).strftime("%Y-%m-%d %H:%M")
        filas[idx][6] = "OPERADO"
    # Registros duplicados (misma fila repetida, como una doble carga de la fuente)
    filas.extend([list(filas[idx]) for idx in (5, 60, 140, 230, 290)])
    return filas


def generar_reservas(rng: random.Random, vuelos: list[list]) -> list[list]:
    ids_vuelos = sorted({v[0] for v in vuelos})
    filas = []
    for i in range(1, N_RESERVAS + 1):
        fecha = INICIO_MES - timedelta(days=rng.randrange(1, 60))
        tarifa = rng.randrange(150_000, 2_500_000, 1_000)
        filas.append(
            [f"RS{i:05d}", rng.choice(ids_vuelos), f"PX{rng.randint(1, 900):04d}",
             fecha.strftime("%Y-%m-%d"), str(tarifa), "COP"]
        )

    for idx in (15, 300, 600, 900, 1200, 1400):  # tarifa negativa
        filas[idx][4] = str(-abs(int(filas[idx][4])))
    for idx in (45, 450, 1050, 1350):  # tarifa vacía
        filas[idx][4] = ""
    for idx in (75, 375, 675, 975, 1275):  # vuelo inexistente
        filas[idx][1] = "VL9999"
    filas.extend([list(filas[idx]) for idx in (10, 500, 800, 1100, 1450)])
    return filas


def main() -> None:
    rng = random.Random(SEMILLA)
    vuelos = generar_vuelos(rng)
    escribir_csv(
        RAIZ / "aeropuertos" / "aeropuertos.csv",
        ["Código IATA", "Nombre", "Ciudad", "País"],
        generar_aeropuertos(),
    )
    escribir_csv(
        RAIZ / "vuelos" / "vuelos.csv",
        ["Vuelo Id", "Numero Vuelo", "Origen", "Destino", "Salida Programada", "Salida Real", "Estado"],
        vuelos,
    )
    escribir_csv(
        RAIZ / "reservas" / "reservas.csv",
        ["Reserva Id", "Vuelo Id", "Pasajero Id", "Fecha Reserva", "Tarifa", "Moneda"],
        generar_reservas(rng, vuelos),
    )


if __name__ == "__main__":
    main()
