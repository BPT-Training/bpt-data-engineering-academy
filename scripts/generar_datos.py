"""
Genera los CSV sintéticos de la Sesión 03 (operación de vuelos).

Es determinístico (semilla fija): cada ejecución produce los mismos
archivos. Siembra problemas de calidad a propósito para practicar
limpieza y quarantine. Los encabezados quedan en español, como los
entregaría un sistema fuente; Silver los conforma a inglés.

Uso:
    python scripts/generar_datos.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 2026
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
FLIGHT_COUNT = 300
BOOKING_COUNT = 1500
MONTH_START = datetime(2026, 10, 1)
ISO_SHARE = 0.8  # proporción de fechas en formato ISO

AIRPORTS = [
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

AIRPORT_HEADER = ["Código IATA", "Nombre", "Ciudad", "País"]
FLIGHT_HEADER = [
    "Vuelo Id",
    "Numero Vuelo",
    "Origen",
    "Destino",
    "Salida Programada",
    "Salida Real",
    "Estado",
]
BOOKING_HEADER = [
    "Reserva Id",
    "Vuelo Id",
    "Pasajero Id",
    "Fecha Reserva",
    "Tarifa",
    "Moneda",
]

# Posición de las columnas en cada fila de vuelo
ORIGIN, DESTINATION, SCHEDULED, ACTUAL, STATUS = 2, 3, 4, 5, 6
# Posición de las columnas en cada fila de reserva
BOOKING_FLIGHT, BOOKING_FARE = 1, 4


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    """
    Escribe un CSV en UTF-8 con encabezado.

    Args:
        path: Ruta del archivo de salida.
        header: Nombres de columna.
        rows: Filas de datos.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"{path.relative_to(DATA_DIR.parent)}: {len(rows)} filas")


def format_timestamp(value: datetime, rng: random.Random) -> str:
    """
    Formatea una fecha con uno de dos formatos, al azar.

    Args:
        value: Fecha y hora a formatear.
        rng: Generador aleatorio con semilla.

    Returns:
        Texto en formato ISO (80 %) o día/mes/año (20 %).
    """
    if rng.random() < ISO_SHARE:
        return value.strftime("%Y-%m-%d %H:%M")
    return value.strftime("%d/%m/%Y %H:%M")


def build_airports() -> list[list]:
    """
    Construye los aeropuertos con dos duplicados sembrados.

    Returns:
        Filas de aeropuertos.
    """
    rows = [list(airport) for airport in AIRPORTS]
    # Códigos en minúscula o con espacios que duplican aeropuertos
    rows.append([" bog", "El Dorado", "Bogotá", "Colombia"])
    rows.append(["mde ", "José María Córdova", "Rionegro", "Colombia"])
    return rows


def pick_delay_minutes(rng: random.Random) -> int:
    """
    Elige un retraso con una distribución realista.

    Args:
        rng: Generador aleatorio con semilla.

    Returns:
        Minutos de retraso (negativo si el vuelo salió antes).
    """
    draw = rng.random()
    if draw < 0.10:
        return rng.randint(-10, -1)
    if draw < 0.70:
        return rng.randint(0, 15)
    if draw < 0.90:
        return rng.randint(16, 60)
    return rng.randint(61, 180)


def build_flights(rng: random.Random) -> list[list]:
    """
    Construye los vuelos de octubre con problemas de calidad sembrados.

    Args:
        rng: Generador aleatorio con semilla.

    Returns:
        Filas de vuelos, incluidos 5 duplicados.
    """
    codes = [airport[0] for airport in AIRPORTS]
    rows, scheduled_times = [], []
    for number in range(1, FLIGHT_COUNT + 1):
        origin, destination = rng.sample(codes, 2)
        scheduled = MONTH_START + timedelta(
            days=rng.randrange(31),
            hours=rng.randrange(5, 23),
            minutes=rng.choice([0, 15, 30, 45]),
        )
        if rng.random() < 0.05:
            status, actual = "CANCELADO", ""
        else:
            delay = timedelta(minutes=pick_delay_minutes(rng))
            status = "OPERADO"
            actual = format_timestamp(scheduled + delay, rng)
        scheduled_times.append(scheduled)
        rows.append(
            [
                f"VL{number:04d}",
                f"AV{rng.randint(100, 9999)}",
                origin,
                destination,
                format_timestamp(scheduled, rng),
                actual,
                status,
            ]
        )

    # Índices fijos para que los problemas sean reproducibles
    for idx in (10, 50, 120, 200):  # origen = destino
        rows[idx][DESTINATION] = rows[idx][ORIGIN]
    for idx in (30, 150, 250):  # salida programada ausente o inválida
        rows[idx][SCHEDULED] = "sin dato" if idx == 150 else ""
    for idx in (70, 180, 260):  # OPERADO sin salida real
        rows[idx][STATUS], rows[idx][ACTUAL] = "OPERADO", ""
    for idx in (90, 210):  # minúscula con espacios (válidos tras limpiar)
        rows[idx][ORIGIN] = f" {rows[idx][ORIGIN].lower()}"
    for idx in (100, 220):  # aeropuerto inexistente
        rows[idx][DESTINATION] = "XXX"
    # Salida real 2 días antes de la programada: la detecta la opción A
    # del challenge, no el pipeline base
    for idx in (40, 160):
        early = scheduled_times[idx] - timedelta(days=2)
        rows[idx][ACTUAL] = early.strftime("%Y-%m-%d %H:%M")
        rows[idx][STATUS] = "OPERADO"
    # Doble carga de la fuente
    rows.extend([list(rows[idx]) for idx in (5, 60, 140, 230, 290)])
    return rows


def build_bookings(rng: random.Random, flights: list[list]) -> list[list]:
    """
    Construye las reservas con problemas de calidad sembrados.

    Args:
        rng: Generador aleatorio con semilla.
        flights: Filas de vuelos, para tomar los identificadores.

    Returns:
        Filas de reservas, incluidos 5 duplicados.
    """
    flight_ids = sorted({flight[0] for flight in flights})
    rows = []
    for number in range(1, BOOKING_COUNT + 1):
        booking_date = MONTH_START - timedelta(days=rng.randrange(1, 60))
        fare = rng.randrange(150_000, 2_500_000, 1_000)
        rows.append(
            [
                f"RS{number:05d}",
                rng.choice(flight_ids),
                f"PX{rng.randint(1, 900):04d}",
                booking_date.strftime("%Y-%m-%d"),
                str(fare),
                "COP",
            ]
        )

    for idx in (15, 300, 600, 900, 1200, 1400):  # tarifa negativa
        rows[idx][BOOKING_FARE] = str(-abs(int(rows[idx][BOOKING_FARE])))
    for idx in (45, 450, 1050, 1350):  # tarifa vacía
        rows[idx][BOOKING_FARE] = ""
    for idx in (75, 375, 675, 975, 1275):  # vuelo inexistente
        rows[idx][BOOKING_FLIGHT] = "VL9999"
    rows.extend([list(rows[idx]) for idx in (10, 500, 800, 1100, 1450)])
    return rows


def main() -> None:
    """Genera los tres archivos de práctica en data/."""
    rng = random.Random(SEED)
    flights = build_flights(rng)
    write_csv(
        DATA_DIR / "airports" / "airports.csv",
        AIRPORT_HEADER,
        build_airports(),
    )
    write_csv(DATA_DIR / "flights" / "flights.csv", FLIGHT_HEADER, flights)
    write_csv(
        DATA_DIR / "bookings" / "bookings.csv",
        BOOKING_HEADER,
        build_bookings(rng, flights),
    )


if __name__ == "__main__":
    main()
