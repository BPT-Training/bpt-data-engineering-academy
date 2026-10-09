import pytest

from bpt_utils.calidad import aplicar_expectativas, resumen_calidad

REGLAS = [
    {"nombre": "tarifa_no_negativa", "expresion": "tarifa >= 0"},
    {"nombre": "origen_distinto_destino", "expresion": "origen <> destino"},
]


@pytest.fixture
def df_reservas(spark):
    return spark.createDataFrame(
        [
            ("R1", "BOG", "MDE", 100.0),  # válida
            ("R2", "BOG", "BOG", 100.0),  # origen = destino
            ("R3", "BOG", "MDE", -5.0),  # tarifa negativa
            ("R4", "CLO", "CLO", None),  # falla ambas (null cuenta como incumplida)
        ],
        "reserva_id string, origen string, destino string, tarifa double",
    )


def test_separa_validos_e_invalidos(df_reservas):
    validos, invalidos = aplicar_expectativas(df_reservas, REGLAS)
    assert [r.reserva_id for r in validos.collect()] == ["R1"]
    assert sorted(r.reserva_id for r in invalidos.collect()) == ["R2", "R3", "R4"]
    assert validos.columns == df_reservas.columns


def test_invalidos_listan_reglas_fallidas(df_reservas):
    _, invalidos = aplicar_expectativas(df_reservas, REGLAS)
    fallidas = {r.reserva_id: r._reglas_fallidas for r in invalidos.collect()}
    assert fallidas["R2"] == ["origen_distinto_destino"]
    assert fallidas["R4"] == ["tarifa_no_negativa", "origen_distinto_destino"]
    assert "_quarantine_ts" in invalidos.columns


def test_resumen_calidad(df_reservas):
    _, invalidos = aplicar_expectativas(df_reservas, REGLAS)
    resumen = {r.regla: r.registros for r in resumen_calidad(invalidos).collect()}
    assert resumen == {"tarifa_no_negativa": 2, "origen_distinto_destino": 2}


def test_sin_reglas_falla(df_reservas):
    with pytest.raises(ValueError):
        aplicar_expectativas(df_reservas, [])
